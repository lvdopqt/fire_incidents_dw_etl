import os
import pandas as pd
import psycopg2
from psycopg2 import sql
# from sqlalchemy import create_engine # Not strictly needed for this psycopg2-focused script
# from sqlalchemy.types import Text # Not strictly needed if we use TEXT for all columns initially
import re
import io
import csv
from contextlib import contextmanager # Import contextmanager

# --- Configuration ---
# Use descriptive variable names
DB_USER = os.getenv('DB_USER')
DB_PASSWORD = os.getenv('DB_PASSWORD')
DB_NAME = os.getenv('DB_NAME')
DB_HOST = os.getenv('DB_HOST')
DB_PORT = os.getenv('DB_PORT')
CSV_FILE_PATH = os.getenv('CSV_PATH') # Renamed for clarity

RAW_STAGING_TABLE_NAME = 'stg_fire_incidents_raw'

# Define the chunk size for reading the CSV and loading to DB
CSV_CHUNK_SIZE = 10000

# Define the separator and quoting for the in-memory CSV buffer used by copy_from
COPY_SEP = '\t' # Use tab as separator for the in-memory buffer
COPY_QUOTECHAR = '"' # Use double quote as quote character
COPY_QUOTING = csv.QUOTE_MINIMAL # Only quote fields with special characters

# --- Validation ---
def validate_configuration():
    """Validates that required environment variables are set."""
    required_vars = {
        'DB_USER': DB_USER,
        'DB_PASSWORD': DB_PASSWORD,
        'DB_NAME': DB_NAME,
        'DB_HOST': DB_HOST,
        'DB_PORT': DB_PORT,
        'CSV_PATH': CSV_FILE_PATH
    }
    missing_vars = [var for var, value in required_vars.items() if not value]
    if missing_vars:
        print(f"Error: Missing required environment variables: {', '.join(missing_vars)}")
        print("Please check your .env file or docker-compose.yml configuration.")
        return False
    if not os.path.exists(CSV_FILE_PATH):
         print(f"Error: CSV file not found at {CSV_FILE_PATH}")
         return False
    return True

# --- Database Connection Context Manager ---
@contextmanager
def get_db_connection(dbname, user, password, host, port):
    """Provides a database connection and cursor using a context manager."""
    conn = None
    cursor = None
    try:
        conn = psycopg2.connect(
            dbname=dbname,
            user=user,
            password=password,
            host=host,
            port=port
        )
        conn.autocommit = False # Manual transaction management
        cursor = conn.cursor()
        print("Database connection successful.")
        yield conn, cursor
    except psycopg2.OperationalError as e:
        print(f"Error: Database connection failed: {e}")
        if conn:
            conn.rollback() # Ensure rollback on connection error before yielding
        raise # Re-raise the exception
    except Exception as e:
        print(f"An unexpected error occurred during database connection: {e}")
        if conn:
            conn.rollback() # Ensure rollback on other errors before yielding
        raise # Re-raise the exception
    finally:
        if cursor:
            cursor.close()
        if conn:
            conn.close()
            print("Database connection closed.")

# --- Helper Functions ---
def clean_column_names(columns):
    """Cleans column names for SQL compatibility."""
    # Replace non-alphanumeric characters (excluding underscore) with nothing, then lowercase
    # Use str.lower() first for consistent handling of case before regex
    cleaned_columns = columns.str.lower().str.replace('[^0-9a-zA-Z_]+', '', regex=True)
    # Handle potential leading/trailing underscores or multiple underscores resulting from cleaning
    # This part can be refined based on specific needs, but the basic replace is often sufficient.
    return cleaned_columns


def dataframe_to_copy_buffer(df, sep, quotechar, quoting):
    """Converts a DataFrame chunk to an in-memory text buffer for psycopg2 copy_from."""
    csv_buffer = io.StringIO()
    df.to_csv(
        csv_buffer,
        index=False,
        header=False, # copy_from doesn't need header in the buffer
        sep=sep,
        quoting=quoting,
        quotechar=quotechar
    )
    csv_buffer.seek(0) # Rewind the buffer
    return csv_buffer

# --- Core Database Operations ---

def create_staging_table_if_not_exists(cursor, csv_path, table_name):
    """
    Reads the first chunk of the CSV to infer column names, cleans them,
    and creates the staging table if it doesn't exist.
    Returns the list of cleaned column names.
    """
    print(f"Checking/Creating table {table_name} if it does not exist...")
    try:
        # Use a small chunksize just for reading headers/first row
        header_chunk_iterator = pd.read_csv(csv_path, chunksize=1, low_memory=False)
        header_chunk_df = next(header_chunk_iterator)
        print("Successfully read first row to determine column names.")

        # Clean up column names
        cleaned_columns = clean_column_names(header_chunk_df.columns)
        cleaned_column_list = cleaned_columns.tolist()
        print(f"Cleaned column names: {cleaned_column_list}")

        # Generate column definitions using psycopg2.sql objects (all TEXT for simplicity)
        column_definitions = [sql.Composed([sql.Identifier(col), sql.SQL(' TEXT')]) for col in cleaned_column_list]
        columns_composed = sql.SQL(', ').join(column_definitions)

        # Compose the final CREATE TABLE query using sql.SQL and sql.Identifier
        create_table_sql = sql.SQL("CREATE TABLE IF NOT EXISTS {table} ({columns})").format(
            table=sql.Identifier(table_name),
            columns=columns_composed
        )

        # Execute the CREATE TABLE query
        cursor.execute(create_table_sql)
        print(f"Table {table_name} checked/created.")
        return cleaned_column_list

    except StopIteration:
        print("Warning: CSV file is empty or only contains headers. No data to infer schema or load.")
        return [] # Return empty list if file is effectively empty
    except Exception as e:
        print(f"Error during initial chunk read or table creation: {e}")
        raise # Re-raise the exception

def truncate_table(cursor, table_name):
    """Truncates the specified table."""
    print(f"Truncating staging table: {table_name}")
    truncate_table_sql = sql.SQL("TRUNCATE TABLE {table}").format(
        table=sql.Identifier(table_name)
    )
    cursor.execute(truncate_table_sql)
    print("Staging table truncated.")

def load_chunk_into_db(cursor, df, table_name, columns, sep, quotechar, quoting):
    """Loads a single DataFrame chunk into the database using copy_from."""
    if df.empty:
        print("Warning: Skipping empty chunk.")
        return 0 # Indicate 0 rows loaded for this chunk

    # Prepare data for copy_from using the helper function
    csv_buffer = dataframe_to_copy_buffer(df, sep, quotechar, quoting)

    # Use copy_from to load the data chunk
    try:
        # Ensure columns passed to copy_from match the order/names in the DataFrame
        cursor.copy_from(
            csv_buffer,
            table_name,
            sep=sep,
            columns=columns # Pass the list of cleaned column names
        )
        rows_loaded = len(df)
        print(f"Loaded {rows_loaded} rows into {table_name} using copy_from.")
        return rows_loaded
    except Exception as e:
         print(f"Error loading chunk using copy_from: {e}")
         # Decide whether to raise or log and continue. Raising stops the process.
         raise e # Re-raise the exception

def load_csv_in_chunks(cursor, csv_path, table_name, chunk_size, columns, sep, quotechar, quoting):
    """Reads CSV in chunks and loads each chunk into the database."""
    print(f"Loading data into {table_name} in chunks of {chunk_size} rows using copy_from (Tab Separator)...")

    total_rows_loaded = 0
    chunk_count = 0

    # Read the CSV in chunks
    csv_chunk_iterator = pd.read_csv(csv_path, chunksize=chunk_size, low_memory=False)

    for chunk_df in csv_chunk_iterator:
        chunk_count += 1
        print(f"Processing chunk {chunk_count} ({len(chunk_df)} rows)...")

        # Clean up column names for the current chunk (important if headers aren't perfectly consistent)
        # Although we got column names from the first chunk, re-cleaning each chunk's columns
        # ensures consistency, especially important if low_memory=False isn't fully preventing dtype issues.
        chunk_df.columns = clean_column_names(chunk_df.columns)

        # Ensure the chunk DataFrame columns match the expected cleaned column names
        # This check is important if the file structure is inconsistent
        if not list(chunk_df.columns) == columns:
             print(f"Warning: Columns in chunk {chunk_count} do not match the expected table columns.")
             print(f"Expected: {columns}")
             print(f"Found: {list(chunk_df.columns)}")
             # Decide how to handle mismatch: skip chunk, attempt remapping, or raise error.
             # For now, we'll assume consistency after cleaning and proceed, but this is a potential failure point.
             # If remapping is needed, pandas.DataFrame.rename could be used.

        try:
            rows_loaded_in_chunk = load_chunk_into_db(
                cursor,
                chunk_df,
                table_name,
                columns, # Pass the expected cleaned columns
                sep,
                quotechar,
                quoting
            )
            total_rows_loaded += rows_loaded_in_chunk
            print(f"Chunk {chunk_count} processed. Total rows loaded so far: {total_rows_loaded}")
        except Exception as e:
            print(f"Error processing chunk {chunk_count}: {e}")
            # Stop the process if a chunk fails to load
            raise e # Re-raise the exception

    print(f"Finished processing all chunks. Total rows loaded: {total_rows_loaded}")

# --- Main ETL Orchestration ---
def run_etl():
    """
    Orchestrates the ETL-like process: connects to DB, creates table,
    truncates table, loads data in chunks, and manages transactions.
    """
    if not validate_configuration():
        return # Exit if config is invalid

    print(f"Starting ETL process for {CSV_FILE_PATH}...")

    # Use the context manager for database connection
    try:
        with get_db_connection(DB_NAME, DB_USER, DB_PASSWORD, DB_HOST, DB_PORT) as (conn, cursor):
            # 1. Create table if it doesn't exist and get cleaned columns
            cleaned_columns = create_staging_table_if_not_exists(cursor, CSV_FILE_PATH, RAW_STAGING_TABLE_NAME)

            if not cleaned_columns:
                 print("No columns found in CSV, exiting.")
                 return # Exit if CSV is empty or header-only

            # 2. Truncate the staging table
            truncate_table(cursor, RAW_STAGING_TABLE_NAME)

            # 3. Load data in chunks
            load_csv_in_chunks(
                cursor,
                CSV_FILE_PATH,
                RAW_STAGING_TABLE_NAME,
                CSV_CHUNK_SIZE,
                cleaned_columns, # Pass the cleaned columns
                COPY_SEP,
                COPY_QUOTECHAR,
                COPY_QUOTING
            )

            # 4. Commit the transaction if everything succeeded
            conn.commit()
            print("Transaction committed successfully.")
            print("ETL process finished.")

    except Exception as e:
        print(f"ETL process failed: {e}")
        # The context manager handles rollback and closing on exceptions

# --- Script Entry Point ---
if __name__ == "__main__":
    run_etl()