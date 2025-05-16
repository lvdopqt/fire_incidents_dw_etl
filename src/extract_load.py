import os
import pandas as pd
import psycopg2
from psycopg2 import sql
from sqlalchemy import create_engine # Still needed for initial connection string format
from sqlalchemy.types import Text # Still needed for potential DDL generation logic
import re
import io # Import io module for in-memory text handling
import csv # Import csv module for quoting options

# --- Configuration ---
DB_USER = os.getenv('DB_USER')
DB_PASSWORD = os.getenv('DB_PASSWORD')
DB_NAME = os.getenv('DB_NAME')
DB_HOST = os.getenv('DB_HOST')
DB_PORT = os.getenv('DB_PORT')
CSV_PATH = os.getenv('CSV_PATH')

RAW_STAGING_TABLE_NAME = 'stg_fire_incidents_raw'

# Define the chunk size for reading the CSV and loading to DB
CHUNK_SIZE = 10000 # Read and load in chunks of 10,000 rows

# Define the separator and quoting for the in-memory CSV buffer used by copy_from
COPY_SEP = '\t' # Use tab as separator for the in-memory buffer
COPY_QUOTECHAR = '"' # Use double quote as quote character
COPY_QUOTING = csv.QUOTE_MINIMAL # Only quote fields with special characters

# --- Validation ---
if not all([DB_USER, DB_PASSWORD, DB_NAME, DB_HOST, DB_PORT, CSV_PATH]):
    print("Error: Database environment variables or CSV_PATH are not configured.")
    print("Please check your .env file or docker-compose.yml configuration.")
    exit(1)

# --- Main ETL-L Function ---
def extract_and_load():
    """
    Extracts data from a CSV file in chunks using Pandas and loads each chunk
    into a raw staging table in PostgreSQL using psycopg2.copy_from.
    Uses a tab separator for the in-memory buffer for better handling of data with commas.
    This creates the table if it doesn't exist and replaces the existing data daily.
    Includes progress indication during the load process.
    Handles large files by processing them in chunks more memory efficiently.
    """
    print(f"Starting Extract and Load process for {CSV_PATH} in chunks using copy_from (Tab Separator)...")

    if not os.path.exists(CSV_PATH):
        print(f"Error [Extract]: CSV file not found at {CSV_PATH}")
        exit(1)

    conn = None
    cursor = None
    engine = None
    try:
        print("Connecting to PostgreSQL database...")
        # Create the connection using parameters from environment variables
        conn = psycopg2.connect(
            dbname=DB_NAME,
            user=DB_USER,
            password=DB_PASSWORD,
            host=DB_HOST,
            port=DB_PORT
        )
        conn.autocommit = False # Manage transactions manually
        cursor = conn.cursor()
        print("Database connection successful.")

        # Create SQLAlchemy engine (primarily for connection string format if needed elsewhere)
        # engine = create_engine(f'postgresql://{DB_USER}:{DB_PASSWORD}@{DB_HOST}:{DB_PORT}/{DB_NAME}')


        # --- Create table if it doesn't exist (read first chunk to get columns) ---
        print(f"Checking/Creating table {RAW_STAGING_TABLE_NAME} if it does not exist...")
        # Read just the first chunk to infer column names for table creation DDL
        try:
            # Use a small chunksize just for reading headers/first row
            header_chunk_iterator = pd.read_csv(CSV_PATH, chunksize=1, low_memory=False)
            header_chunk_df = next(header_chunk_iterator)
            print("Successfully read first row to determine column names.")

            # Clean up column names for SQL compatibility using Pandas string methods
            print("Cleaning up column names for SQL compatibility...")
            header_chunk_df.columns = header_chunk_df.columns.str.replace('[^0-9a-zA-Z_]+', '', regex=True).str.lower()
            print("Column names cleaned.")

            # Generate column definitions using psycopg2.sql objects from cleaned first chunk columns
            column_definitions = [sql.Composed([sql.Identifier(col), sql.SQL(' TEXT')]) for col in header_chunk_df.columns]
            columns_composed = sql.SQL(', ').join(column_definitions)

            # Compose the final CREATE TABLE query using sql.SQL and sql.Identifier
            create_table_sql = sql.SQL("CREATE TABLE IF NOT EXISTS {table} ({columns})").format(
                table=sql.Identifier(RAW_STAGING_TABLE_NAME),
                columns=columns_composed
            )

            # Execute the CREATE TABLE query
            cursor.execute(create_table_sql)
            print(f"Table {RAW_STAGING_TABLE_NAME} checked/created.")

        except StopIteration:
            print("Warning [Extract]: CSV file is empty or only contains headers. No data to load.")
            return # Exit function if file is empty
        except Exception as e:
            print(f"Error during initial chunk read or table creation: {e}")
            exit(1)
        # --- END Create table ---


        # Truncate the staging table before loading
        print(f"Truncating staging table: {RAW_STAGING_TABLE_NAME}")
        truncate_table_sql = sql.SQL("TRUNCATE TABLE {table}").format(
            table=sql.Identifier(RAW_STAGING_TABLE_NAME)
        )
        cursor.execute(truncate_table_sql)
        print("Staging table truncated.")

        # --- Load data from CSV in chunks using copy_from with progress ---
        print(f"Loading data into {RAW_STAGING_TABLE_NAME} in chunks of {CHUNK_SIZE} rows using copy_from (Tab Separator)...")

        # Read the CSV in chunks using the chunksize parameter
        # Start from the beginning of the file again
        csv_chunk_iterator = pd.read_csv(CSV_PATH, chunksize=CHUNK_SIZE, low_memory=False)

        chunk_count = 0
        total_rows_loaded = 0

        for chunk_df in csv_chunk_iterator:
            chunk_count += 1
            print(f"Processing and loading chunk {chunk_count} ({len(chunk_df)} rows)...")

            if chunk_df.empty:
                print(f"Warning: Chunk {chunk_count} is empty. Skipping.")
                continue # Skip empty chunks

            # Clean up column names for the current chunk (important if headers aren't consistent or for safety)
            chunk_df.columns = chunk_df.columns.str.replace('[^0-9a-zA-Z_]+', '', regex=True).str.lower()

            # Prepare data for copy_from: convert DataFrame chunk to CSV string in memory
            # Use .to_csv with index=False and header=False
            # Use the defined COPY_SEP and COPY_QUOTING
            csv_buffer = io.StringIO()
            chunk_df.to_csv(
                csv_buffer,
                index=False,
                header=False,
                sep=COPY_SEP,
                quoting=COPY_QUOTING,
                quotechar=COPY_QUOTECHAR
            )
            csv_buffer.seek(0) # Rewind the buffer to the beginning

            # Use copy_from to load the data chunk
            # Specify the table name, file-like object, separator, and format
            try:
                cursor.copy_from(
                    csv_buffer,
                    RAW_STAGING_TABLE_NAME,
                    sep=COPY_SEP, # Use the defined separator
                    columns=chunk_df.columns.tolist() # Pass the list of cleaned column names
                )
                total_rows_loaded += len(chunk_df)
                print(f"Chunk {chunk_count} loaded using copy_from. Total rows loaded so far: {total_rows_loaded}")
            except Exception as e:
                 print(f"Error loading chunk {chunk_count} using copy_from: {e}")
                 # Decide whether to exit or continue on chunk error
                 # For this test, let's exit on error
                 raise e # Re-raise the exception to be caught by the main except block


        print(f"Finished processing all chunks. Total rows loaded: {total_rows_loaded}")
        print("Initial load to raw staging table completed.")
        # --- END Load with progress ---

        # Commit the entire transaction (CREATE IF NOT EXISTS, TRUNCATE, and all COPYs)
        conn.commit()
        print("Transaction committed successfully.")
        print(f"Extract and Load process for {RAW_STAGING_TABLE_NAME} finished.")

    except psycopg2.OperationalError as e:
        print(f"Error [Load]: Database connection or operation failed: {e}")
        if conn:
            conn.rollback() # Rollback transaction on error
        exit(1)
    except Exception as e:
        print(f"Error [Load]: An unexpected error occurred during the load process: {e}")
        if conn:
            conn.rollback() # Rollback transaction on error
        exit(1)
    finally:
        # Ensure resources are closed
        if cursor:
            cursor.close()
        if conn:
            conn.close()
            print("Database connection closed.")
        # Note: SQLAlchemy engine doesn't need explicit closing in this simple case

# Execute the main function if the script is run directly
if __name__ == "__main__":
    extract_and_load()
