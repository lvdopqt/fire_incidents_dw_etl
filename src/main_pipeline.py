import os
import pandas as pd
import psycopg2
from psycopg2 import sql
import io
import csv
from contextlib import contextmanager
from datetime import datetime
import numpy as np
import logging
import asyncio 

from prefect import flow, task 

from prefect_dbt.cli import DbtCoreOperation
from prefect_dbt.cli import DbtCliProfile

# --- Basic Logging Configuration (for local testing outside Prefect Agent) ---
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

# --- Global Constants (can be made flow parameters if dynamic changes are needed) ---
DATE_FORMATS = {
    'incidentdate': '%Y/%m/%d',
    'alarmdttm': '%Y/%m/%d %I:%M:%S %p',
    'arrivaldttm': '%Y/%m/%d %I:%M:%S %p',
    'closedttm': '%Y/%m/%d %I:%M:%S %p',
    'data_as_of': '%Y/%m/%d %I:%M:%S %p',
    'data_loaded_at': '%Y/%m/%d %I:%M:%S %p'
}

COLUMN_TYPE_MAPPING_FOR_SCHEMA = {
    'incidentnumber': 'TEXT', 'exposurenumber': 'INTEGER', 'id': 'TEXT',
    'incidentdate': 'DATE', 'alarmdttm': 'TIMESTAMP', 'arrivaldttm': 'TIMESTAMP',
    'closedttm': 'TIMESTAMP', 'data_as_of': 'TIMESTAMP', 'city': 'TEXT',
    'zipcode': 'TEXT', 'battalion': 'TEXT', 'stationarea': 'TEXT',
    'supervisordistrict': 'TEXT', 'neighborhood_district': 'TEXT',
    'point': 'TEXT', 'suppressionunits': 'INTEGER', 'emsunits': 'INTEGER',
    'numberofalarms': 'INTEGER', 'primarysituation': 'TEXT',
    'propertyuse': 'TEXT', 'address': 'TEXT', 'callnumber': 'INTEGER',
    'box': 'TEXT', 'suppressionpersonnel': 'INTEGER', 'emspersonnel': 'INTEGER',
    'otherunits': 'INTEGER', 'otherpersonnel': 'INTEGER',
    'firstunitonscene': 'TEXT', 'estimatedpropertyloss': 'NUMERIC',
    'estimatedcontentsloss': 'NUMERIC', 'firefatalities': 'INTEGER',
    'fireinjuries': 'INTEGER', 'civilianfatalities': 'INTEGER',
    'civilianinjuries': 'INTEGER', 'mutualaid': 'TEXT',
    'actiontakenprimary': 'TEXT', 'actiontakensecondary': 'TEXT',
    'actiontakenother': 'TEXT', 'detectoralertedoccupants': 'TEXT',
    'areaoffireorigin': 'TEXT', 'ignitioncause': 'TEXT',
    'ignitionfactorprimary': 'TEXT', 'ignitionfactorsecondary': 'TEXT',
    'heatsource': 'TEXT', 'itemfirstignited': 'TEXT',
    'humanfactorsassociatedwithignition': 'TEXT', 'structuretype': 'TEXT',
    'structurestatus': 'TEXT', 'flooroffireorigin': 'TEXT',
    'firespread': 'TEXT', 'noflamespread': 'NUMERIC',
    'numberoffloorswithminimumdamage': 'INTEGER',
    'numberoffloorswithsignificantdamage': 'INTEGER',
    'numberoffloorswithheavydamage': 'INTEGER',
    'numberoffloorswithextremedamage': 'INTEGER',
    'detectorspresent': 'TEXT', 'detectortype': 'TEXT',
    'detectoroperation': 'TEXT', 'detectoreffectiveness': 'TEXT',
    'detectorfailurereason': 'TEXT',
    'automaticextinguishingsystempresent': 'TEXT',
    'automaticextinguishing_systemtype': 'TEXT',
    'automaticextinguishing_systemperfomance': 'TEXT',
    'automaticextinguishing_systemfailure_reason': 'TEXT',
    'numberofsprinklerheadsoperating': 'INTEGER'
}

# --- Database Connection Context Manager ---
@contextmanager
def get_db_connection(dbname, user, password, host, port):
    conn = None
    cursor = None
    try:
        conn = psycopg2.connect(
            dbname=dbname, user=user, password=password, host=host, port=port
        )
        conn.autocommit = False
        cursor = conn.cursor()
        logger.info("Database connection successful.")
        yield conn, cursor
    except psycopg2.OperationalError as e:
        logger.error(f"Database connection failed: {e}")
        if conn: conn.rollback()
        raise
    except Exception as e:
        logger.error(f"An unexpected error occurred during database connection: {e}")
        if conn: conn.rollback()
        raise
    finally:
        if cursor: cursor.close()
        if conn:
            conn.close()
            logger.info("Database connection closed.")

# --- Helper Functions ---
def _clean_column_names(columns):
    return columns.str.lower().str.replace('[^0-9a-zA-Z_]+', '', regex=True)

def _dataframe_to_copy_buffer(df, sep, quotechar, quoting):
    csv_buffer = io.StringIO()
    df.to_csv(
        csv_buffer, index=False, header=False, sep=sep,
        quoting=quoting, quotechar=quotechar, na_rep='\\N'
    )
    csv_buffer.seek(0)
    return csv_buffer

# --- Prefect Tasks ---
@task
async def validate_etl_configuration(csv_file_path: str):
    if not os.path.exists(csv_file_path):
         raise FileNotFoundError(f"CSV file not found at {csv_file_path}.")
    logger.info(f"Configuration validated. CSV file found: {csv_file_path}.")
    return True

@task
async def get_last_loaded_timestamp_task(db_name: str, db_user: str, db_password: str, db_host: str, db_port: int, table_name: str):
    with get_db_connection(db_name, db_user, db_password, db_host, db_port) as (conn, cursor):
        try:
            query = sql.SQL("SELECT MAX(data_loaded_at_dttm) FROM {table}").format(table=sql.Identifier(table_name))
            cursor.execute(query)
            last_timestamp = cursor.fetchone()[0]
            if last_timestamp:
                logger.info(f"Last loaded timestamp in {table_name}: {last_timestamp}")
            else:
                logger.info(f"No data in {table_name}, first load assumed.")
            return last_timestamp
        except psycopg2.Error as e:
            err_msg = str(e).lower()
            if "relation" in err_msg and f"\"{table_name}\" does not exist" in err_msg:
                logger.warning(f"Table '{table_name}' does not exist. Assuming first load.")
                return None
            elif "column \"data_loaded_at_dttm\" does not exist" in err_msg:
                logger.warning(f"Column 'data_loaded_at_dttm' not in '{table_name}'. Assuming first load.")
                return None
            logger.error(f"Error getting last loaded timestamp: {e}")
            raise

@task
async def create_staging_table_if_not_exists_task(db_name: str, db_user: str, db_password: str, db_host: str, db_port: int, csv_path: str, table_name: str):
    logger.info(f"Checking/Creating table {table_name}...")
    with get_db_connection(db_name, db_user, db_password, db_host, db_port) as (conn, cursor):
        try:
            header_chunk_df = next(pd.read_csv(csv_path, chunksize=1, low_memory=False, dtype=str))
            cleaned_csv_columns = _clean_column_names(header_chunk_df.columns).tolist()
            
            column_definitions = [
                sql.Composed([sql.Identifier(col), sql.SQL(COLUMN_TYPE_MAPPING_FOR_SCHEMA.get(col, 'TEXT'))])
                for col in cleaned_csv_columns
            ]
            column_definitions.append(sql.Composed([sql.Identifier('data_loaded_at_dttm'), sql.SQL('TIMESTAMP DEFAULT CURRENT_TIMESTAMP')]))
            column_definitions.append(sql.SQL('PRIMARY KEY ({})').format(sql.SQL(', ').join([sql.Identifier('incidentnumber'), sql.Identifier('exposurenumber')])))
            
            create_table_sql = sql.SQL("CREATE TABLE IF NOT EXISTS {table} ({columns})").format(
                table=sql.Identifier(table_name), columns=sql.SQL(', ').join(column_definitions)
            )
            cursor.execute(create_table_sql)
            conn.commit()
            logger.info(f"Table '{table_name}' checked/created.")
            return cleaned_csv_columns + ['data_loaded_at_dttm']
        except StopIteration:
            logger.warning("CSV empty or headers only. No schema/load.")
            return []
        except Exception as e:
            conn.rollback()
            logger.error(f"Error in table creation/schema inference: {e}")
            raise

@task
async def load_csv_chunk_to_db_task(
    chunk_df: pd.DataFrame, table_name: str, columns_for_table: list,
    copy_sep: str, copy_quotechar: str, copy_quoting: int,
    db_name: str, db_user: str, db_password: str, db_host: str, db_port: int
) -> int: # Explicitly state return type is int
    if chunk_df.empty:
        logger.warning("Skipping empty chunk.")
        return 0
    chunk_df['data_loaded_at_dttm'] = datetime.now()
    csv_buffer = _dataframe_to_copy_buffer(chunk_df, copy_sep, copy_quotechar, copy_quoting)

    with get_db_connection(db_name, db_user, db_password, db_host, db_port) as (conn, cursor):
        try:
            temp_table_name_str = f"{table_name}_temp_chunk_{datetime.now().strftime('%Y%m%d%H%M%S%f')}"
            temp_table_name = sql.Identifier(temp_table_name_str)
            main_table_identifier = sql.Identifier(table_name)

            create_temp_table_sql = sql.SQL("CREATE TEMPORARY TABLE {} (LIKE {} INCLUDING DEFAULTS) ON COMMIT DROP").format(
                temp_table_name, main_table_identifier
            )
            cursor.execute(create_temp_table_sql)
            
            cursor.copy_from(
                csv_buffer, temp_table_name.string, sep=copy_sep,
                columns=[sql.Identifier(col).string for col in columns_for_table], null='\\N'
            )
            
            pk_columns = ['incidentnumber', 'exposurenumber']
            update_columns_set_clause = [col for col in columns_for_table if col not in pk_columns]
            update_set_clauses = [
                sql.Composed([sql.Identifier(col), sql.SQL('= EXCLUDED.'), sql.Identifier(col)])
                for col in update_columns_set_clause
            ]
            
            upsert_sql = sql.SQL("""
                INSERT INTO {main_table} ({all_columns})
                SELECT {all_columns_select} FROM {temp_table}
                ON CONFLICT (incidentnumber, exposurenumber) DO UPDATE SET {update_clauses}
            """).format(
                main_table=main_table_identifier,
                all_columns=sql.SQL(', ').join(sql.Identifier(col) for col in columns_for_table),
                all_columns_select=sql.SQL(', ').join(sql.Identifier(col) for col in columns_for_table),
                temp_table=temp_table_name,
                update_clauses=sql.SQL(', ').join(update_set_clauses)
            )
            cursor.execute(upsert_sql)
            conn.commit()
            rows_loaded = len(chunk_df)
            logger.info(f"Processed {rows_loaded} rows into {table_name} from chunk.")
            return rows_loaded
        except Exception as e:
            conn.rollback()
            logger.error(f"Error loading/upserting chunk into {table_name}: {e}")
            raise

@task
def process_and_load_csv_chunks_task(
    csv_file_path: str, raw_staging_table_name: str, csv_chunk_size: int,
    columns_for_table: list, copy_sep: str, copy_quotechar: str, copy_quoting: int,
    last_loaded_timestamp: datetime, db_name: str, db_user: str, db_password: str,
    db_host: str, db_port: int
) -> bool:
    """Process and load CSV chunks synchronously, returning True if any data was loaded"""
    logger.info(f"Loading data into {raw_staging_table_name} in chunks of {csv_chunk_size}...")
    if last_loaded_timestamp:
        logger.info(f"Filtering records newer than data_as_of: {last_loaded_timestamp}")

    chunk_count = 0
    total_rows_loaded = 0
    csv_reader = pd.read_csv(csv_file_path, chunksize=csv_chunk_size, low_memory=False, dtype=str)

    for chunk_df in csv_reader:
        chunk_count += 1
        logger.info(f"Processing chunk {chunk_count} ({len(chunk_df)} rows before filter)...")
        chunk_df.columns = _clean_column_names(chunk_df.columns)

        # Apply column transformations
        for col_name, pg_type in COLUMN_TYPE_MAPPING_FOR_SCHEMA.items():
            if col_name in chunk_df.columns:
                chunk_df[col_name] = chunk_df[col_name].astype(str).str.strip()
                if pg_type in ['DATE', 'TIMESTAMP']:
                    chunk_df[col_name] = pd.to_datetime(chunk_df[col_name], format=DATE_FORMATS.get(col_name), errors='coerce')
                elif pg_type in ['INTEGER', 'NUMERIC']:
                    chunk_df[col_name] = chunk_df[col_name].replace({'': np.nan, ' ': np.nan, 'None': np.nan, 'nan': np.nan})
                    chunk_df[col_name] = pd.to_numeric(chunk_df[col_name], errors='coerce')
                    if pg_type == 'INTEGER': chunk_df[col_name] = chunk_df[col_name].astype('Int64')
                elif pg_type == 'TEXT':
                    chunk_df[col_name] = chunk_df[col_name].replace({'None': None, '': None, 'nan': None})
        
        # Apply timestamp filtering if needed
        if last_loaded_timestamp and 'data_as_of' in chunk_df.columns:
            chunk_df['data_as_of'] = pd.to_datetime(chunk_df['data_as_of'], errors='coerce')
            chunk_df_filtered = chunk_df[chunk_df['data_as_of'].notna() & (chunk_df['data_as_of'] > last_loaded_timestamp)].copy()
            logger.info(f"Filtered chunk to {len(chunk_df_filtered)} new/updated rows.")
        else:
            chunk_df_filtered = chunk_df.copy()

        # Reorder columns to match table schema
        csv_columns_for_reindex = [col for col in columns_for_table if col != 'data_loaded_at_dttm']
        chunk_df_reordered = chunk_df_filtered.reindex(columns=csv_columns_for_reindex, fill_value=None)
        
        # Load chunk if it has data
        if not chunk_df_reordered.empty:
            rows_loaded = load_csv_chunk_to_db_task(
                chunk_df=chunk_df_reordered,
                table_name=raw_staging_table_name,
                columns_for_table=columns_for_table,
                copy_sep=copy_sep,
                copy_quotechar=copy_quotechar,
                copy_quoting=copy_quoting,
                db_name=db_name, db_user=db_user, db_password=db_password,
                db_host=db_host, db_port=db_port
            )
            total_rows_loaded += rows_loaded if rows_loaded else len(chunk_df_reordered)
            logger.info(f"Chunk {chunk_count} loaded successfully ({len(chunk_df_reordered)} rows).")
        else:
            logger.info(f"Chunk {chunk_count} resulted in no rows after filtering; skipping load.")

    logger.info(f"Data loading completed. Total chunks processed: {chunk_count}, Total rows loaded: {total_rows_loaded}")
    
    # Return True if any data was loaded, False otherwise
    data_was_loaded = total_rows_loaded > 0
    logger.info(f"Returning data_was_loaded: {data_was_loaded}")
    return data_was_loaded

@task
def run_dbt_command_task(
    command: str,
    dbt_profile_block_name: str,
    dbt_project_dir: str
):
    logger.info(f"Running dbt command: 'dbt {command}' in project: {dbt_project_dir}")
    try:

        dbt_core_operation = DbtCoreOperation(
            commands=[f"dbt {command}"],
            project_dir=dbt_project_dir,
            profiles_dir=dbt_project_dir
        )
        result_list = dbt_core_operation.run()
        
        final_result = result_list[-1] 
        logger.info(f"dbt command '{command}' output:\n{final_result}")

    except Exception as e:
        logger.error(f"Error executing dbt command '{command}': {e}")
        raise

# --- Main Prefect Flow ---
@flow(name="Fire Incidents Full Pipeline")
def fire_incidents_full_pipeline(
    csv_file_path: str, db_user: str, db_password: str, db_name: str, db_host: str, db_port: int,
    raw_staging_table_name: str, csv_chunk_size: int, copy_sep: str, copy_quotechar: str,
    copy_quoting: int, dbt_profile_block_name: str, dbt_project_dir: str
):
    logger.info("Starting Fire Incidents Full Pipeline flow...")

    # Step 1: Validate configuration
    logger.info("Step 1: Validating ETL configuration...")
    config_is_valid = validate_etl_configuration(csv_file_path)
    if not config_is_valid:
        logger.error("ETL config validation failed. Aborting pipeline.")
        return
    logger.info("ETL configuration validation completed successfully.")

    # Step 2: Create staging table
    logger.info("Step 2: Creating staging table if not exists...")
    columns_for_table = create_staging_table_if_not_exists_task(
        db_name, db_user, db_password, db_host, db_port, csv_file_path, raw_staging_table_name
    )
    if not columns_for_table:
        logger.warning("No columns detected from CSV. Aborting pipeline.")
        return
    logger.info(f"Staging table ready with {len(columns_for_table)} columns.")

    # Step 3: Get last loaded timestamp
    logger.info("Step 3: Getting last loaded timestamp...")
    last_loaded_timestamp = get_last_loaded_timestamp_task(
        db_name, db_user, db_password, db_host, db_port, raw_staging_table_name
    )
    logger.info(f"Last loaded timestamp: {last_loaded_timestamp}")

    # Step 4: Load data and get confirmation
    logger.info("Step 4: Starting data loading process...")
    data_was_loaded = process_and_load_csv_chunks_task(
        csv_file_path, raw_staging_table_name, csv_chunk_size, columns_for_table,
        copy_sep, copy_quotechar, copy_quoting, last_loaded_timestamp,
        db_name, db_user, db_password, db_host, db_port
    )
    
    logger.info(f"Data loading step completed. Data was loaded: {data_was_loaded}")
    
    # Step 5: Conditional dbt execution - only proceed if data was actually loaded
    if not data_was_loaded:
        logger.info("No new/updated rows were loaded. Skipping dbt transformations.")
        logger.info("Pipeline completed successfully (no transformations needed).")
        return

    # Step 6: Run dbt transformations (only if we reach this point)
    logger.info("Step 5: Data was successfully loaded. Proceeding with dbt transformations...")
    
    # Run dbt models
    logger.info("Step 5a: Running dbt models...")
    dbt_run_result = run_dbt_command_task("run", dbt_profile_block_name, dbt_project_dir)
    logger.info("dbt run completed successfully.")

    # Run dbt tests
    logger.info("Step 5b: Running dbt tests...")
    dbt_test_result = run_dbt_command_task("test", dbt_profile_block_name, dbt_project_dir)
    logger.info("dbt test completed successfully.")
    
    logger.info("Fire Incidents Full Pipeline completed successfully with transformations!")


# --- Entry Point for Local Execution ---
if __name__ == "__main__":
    fire_incidents_full_pipeline(
        csv_file_path=os.getenv('CSV_PATH', 'data/Fire_Incidents_20250515.csv'),
        db_user=os.getenv('DB_USER', 'user'),
        db_password=os.getenv('DB_PASSWORD', 'password'),
        db_name=os.getenv('DB_NAME', 'mydatabase'),
        db_host=os.getenv('DB_HOST', 'localhost'),
        db_port=int(os.getenv('DB_PORT', 5432)),
        raw_staging_table_name='stg_fire_incidents_raw',
        csv_chunk_size=10000,
        copy_sep='\t',
        copy_quotechar='"',
        copy_quoting=csv.QUOTE_MINIMAL,
        dbt_profile_block_name="fire-incidents-dwh-dbt-profile",
        dbt_project_dir="./dbt_project"
    )