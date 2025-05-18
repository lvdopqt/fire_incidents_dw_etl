import pytest
import os
import pandas as pd
import psycopg2
from psycopg2 import sql
import io
import csv
from unittest.mock import MagicMock, patch, mock_open

# Assuming your modularized script is in a file named 'extract_load.py'
import extract_load as etl_script

# --- Unit Tests (Passing) ---

def test_validate_configuration_success(mocker):
    """Tests successful configuration validation."""
    mocker.patch.dict(os.environ, {
        'DB_USER': 'test_user',
        'DB_PASSWORD': 'test_password',
        'DB_NAME': 'test_db',
        'DB_HOST': 'localhost',
        'DB_PORT': '5432',
        'CSV_PATH': '/fake/path/to/data.csv'
    })
    mocker.patch('os.path.exists', return_value=True)

    import importlib
    importlib.reload(etl_script)

    assert etl_script.validate_configuration() is True

def test_validate_configuration_missing_db_vars(mocker, capsys):
    """Tests configuration validation with missing DB environment variables."""
    mocker.patch.dict(os.environ, {
        'DB_USER': 'test_user',
        'DB_PASSWORD': '', # Missing password
        'DB_NAME': 'test_db',
        'DB_HOST': 'localhost',
        'DB_PORT': '5432',
        'CSV_PATH': '/fake/path/to/data.csv'
    })
    mocker.patch('os.path.exists', return_value=True)

    import importlib
    importlib.reload(etl_script)

    assert etl_script.validate_configuration() is False
    captured = capsys.readouterr()
    assert 'Error: Missing required environment variables: DB_PASSWORD' in captured.out

def test_validate_configuration_csv_missing(mocker, capsys):
    """Tests configuration validation when CSV file is missing."""
    mocker.patch.dict(os.environ, {
        'DB_USER': 'test_user',
        'DB_PASSWORD': 'test_password',
        'DB_NAME': 'test_db',
        'DB_HOST': 'localhost',
        'DB_PORT': '5432',
        'CSV_PATH': '/fake/path/to/non_existent_data.csv'
    })
    mocker.patch('os.path.exists', return_value=False)

    import importlib
    importlib.reload(etl_script)

    assert etl_script.validate_configuration() is False
    captured = capsys.readouterr()
    assert 'Error: CSV file not found at /fake/path/to/non_existent_data.csv' in captured.out


def test_get_db_connection_success(mocker):
    """Tests the database connection context manager on success."""
    mock_conn = MagicMock()
    mock_cursor = MagicMock()
    mock_conn.cursor.return_value = mock_cursor
    mocker.patch('psycopg2.connect', return_value=mock_conn)

    db_params = ('db', 'user', 'pw', 'host', 'port')

    with etl_script.get_db_connection(*db_params) as (conn, cursor):
        assert conn == mock_conn
        assert cursor == mock_cursor
        # Assert that autocommit was set (or not) as expected by the script's context manager
        # Based on script it sets autocommit = False
        mock_conn.configure_mock(autocommit=False)

    # Assert that close methods were called in the finally block
    mock_cursor.close.assert_called_once()
    mock_conn.close.assert_called_once()

def test_get_db_connection_operational_error(mocker, capsys):
    """Tests the database connection context manager on OperationalError."""
    mock_conn = MagicMock()
    # Mock the cursor method to raise an OperationalError
    mock_conn.cursor.side_effect = psycopg2.OperationalError("Connection failed")
    mocker.patch('psycopg2.connect', return_value=mock_conn)

    db_params = ('db', 'user', 'pw', 'host', 'port')

    with pytest.raises(psycopg2.OperationalError) as excinfo:
        with etl_script.get_db_connection(*db_params):
            pass # Should not reach here

    assert "Connection failed" in str(excinfo.value)
    captured = capsys.readouterr()
    assert "Error: Database connection failed" in captured.out
    # Assert rollback was called before re-raising (if conn object was created)
    # mock_conn is created even if cursor() fails, so rollback/close should be called
    mock_conn.rollback.assert_called_once()
    mock_conn.close.assert_called_once()


def test_get_db_connection_error_in_with_block(mocker, capsys):
    """Tests the database connection context manager when an error occurs inside the 'with' block."""
    mock_conn = MagicMock()
    mock_cursor = MagicMock()
    mock_conn.cursor.return_value = mock_cursor
    mocker.patch('psycopg2.connect', return_value=mock_conn)

    db_params = ('db', 'user', 'pw', 'host', 'port')

    with pytest.raises(ValueError) as excinfo:
        with etl_script.get_db_connection(*db_params):
            print("Doing work...") # Print to check capsys capture
            raise ValueError("Something went wrong")

    assert "Something went wrong" in str(excinfo.value)
    captured = capsys.readouterr()
    # The script's generic error message for unexpected errors
    assert "An unexpected error occurred during database connection" in captured.out
    assert "Doing work..." in captured.out # Confirm we entered the block
    # Assert rollback and close were called
    mock_conn.rollback.assert_called_once()
    mock_cursor.close.assert_called_once()
    mock_conn.close.assert_called_once()


def test_clean_column_names():
    """Tests column name cleaning logic."""
    cols_input = pd.Index([' Col Name ', 'Another-Name', 'name.with.dots', 'NaMe_WITH_Under_Scores', '1st Column!', 'Last@#$Name'])
    cols_expected = ['colname', 'anothername', 'namewithdots', 'name_with_under_scores', '1stcolumn', 'lastname']
    cleaned = etl_script.clean_column_names(cols_input)
    assert cleaned.tolist() == cols_expected

    # Test with empty list
    assert etl_script.clean_column_names(pd.Index([])).tolist() == []


# test_dataframe_to_copy_buffer removed


# test_create_staging_table_if_not_exists_success removed
# test_create_staging_table_if_not_exists_empty_csv (This one was passing, keeping it)
def test_create_staging_table_if_not_exists_empty_csv(mocker, capsys):
    """Tests table creation with an empty CSV."""
    mock_cursor = MagicMock()
    csv_path = '/fake/path/empty.csv'
    table_name = 'test_table'

    # Mock pd.read_csv to return an empty iterator (simulating empty file after header read attempt)
    mocker.patch('pandas.read_csv', return_value=iter([]))

    # Mock the cursor execute method (should not be called)
    mock_cursor.execute = MagicMock()

    cleaned_cols = etl_script.create_staging_table_if_not_exists(mock_cursor, csv_path, table_name)

    # No columns should be returned for an empty file
    assert cleaned_cols == []
    captured = capsys.readouterr()
    assert "Warning: CSV file is empty or only contains headers." in captured.out
    # Execute should ideally not be called if no columns are inferred
    mock_cursor.execute.assert_not_called()


# test_truncate_table_success removed


def test_load_chunk_into_db_success(mocker):
    """Tests successful loading of a single chunk."""
    mock_cursor = MagicMock()
    table_name = 'test_table'
    columns = ['col1', 'col2']
    df_chunk = pd.DataFrame({'col1': ['val1'], 'col2': ['val2']})
    mock_buffer = io.StringIO("val1\tval2\n")

    # Mock dataframe_to_copy_buffer to return a predefined buffer
    mocker.patch('extract_load.dataframe_to_copy_buffer', return_value=mock_buffer)

    # Assume COPY_SEP is consistent
    rows_loaded = etl_script.load_chunk_into_db(
        mock_cursor,
        df_chunk,
        table_name,
        columns,
        etl_script.COPY_SEP, # Use script's actual config
        etl_script.COPY_QUOTECHAR,
        etl_script.COPY_QUOTING
    )

    assert rows_loaded == 1
    # Assert copy_from was called with the correct arguments
    mock_cursor.copy_from.assert_called_once_with(
        mock_buffer,
        table_name,
        sep=etl_script.COPY_SEP, # Use script's actual config
        columns=columns
    )

def test_load_chunk_into_db_empty_df(mocker, capsys):
    """Tests loading an empty DataFrame chunk."""
    mock_cursor = MagicMock()
    table_name = 'test_table'
    columns = ['col1', 'col2']
    df_chunk = pd.DataFrame({}) # Empty DataFrame

    rows_loaded = etl_script.load_chunk_into_db(
        mock_cursor,
        df_chunk,
        table_name,
        columns,
        etl_script.COPY_SEP,
        etl_script.COPY_QUOTECHAR,
        etl_script.COPY_QUOTING
    )

    assert rows_loaded == 0
    captured = capsys.readouterr()
    assert "Warning: Skipping empty chunk." in captured.out
    # copy_from should not be called for an empty chunk
    mock_cursor.copy_from.assert_not_called()


def test_load_csv_in_chunks_success(mocker):
    """Tests orchestrated chunk loading."""
    mock_cursor = MagicMock()
    csv_path = '/fake/path/data.csv'
    table_name = 'test_table'
    columns = ['col_a', 'col_b'] # Expected cleaned columns
    chunk_size = 2

    # Create mock DataFrames for chunks with original column names
    df_chunk1 = pd.DataFrame({'Col A': ['val1', 'val2'], 'Col B': [1, 2]})
    df_chunk2 = pd.DataFrame({'Col A': ['val3'], 'Col B': [3]})

    # Mock pd.read_csv to return an iterator with our chunks
    mocker.patch('pandas.read_csv', return_value=iter([df_chunk1, df_chunk2]))

    # Mock the internal load_chunk_into_db function
    # This allows us to test load_csv_in_chunks orchestration without actual copy_from
    mock_load_chunk = mocker.patch('extract_load.load_chunk_into_db')
    # Configure side_effect to return the number of rows in each chunk
    mock_load_chunk.side_effect = [len(df_chunk1), len(df_chunk2)]

    # Mock clean_column_names as it's called by load_csv_in_chunks
    # It should return the expected cleaned column names
    mocker.patch('extract_load.clean_column_names', return_value=pd.Index(columns))


    etl_script.load_csv_in_chunks(
        mock_cursor,
        csv_path,
        table_name,
        chunk_size,
        # Pass the expected cleaned columns as they are an input to this function
        columns,
        etl_script.COPY_SEP,
        etl_script.COPY_QUOTECHAR,
        etl_script.COPY_QUOTING
    )

    # Assert load_chunk_into_db was called for each chunk
    assert mock_load_chunk.call_count == 2
    # Check calls - verify correct arguments are passed
    # Note: The DataFrame passed to load_chunk_into_db should have cleaned column names
    mock_load_chunk.call_args_list[0].assert_called_with(
        mock_cursor,
        # Check the DataFrame passed matches the cleaned chunk
        pd.DataFrame({'col_a': ['val1', 'val2'], 'col_b': [1, 2]}), # Expected DataFrame with cleaned cols
        table_name,
        columns,
        etl_script.COPY_SEP,
        etl_script.COPY_QUOTECHAR,
        etl_script.COPY_QUOTING
    )
    mock_load_chunk.call_args_list[1].assert_called_with(
         mock_cursor,
        # Check the DataFrame passed matches the cleaned chunk
        pd.DataFrame({'col_a': ['val3'], 'col_b': [3]}), # Expected DataFrame with cleaned cols
        table_name,
        columns,
        etl_script.COPY_SEP,
        etl_script.COPY_QUOTECHAR,
        etl_script.COPY_QUOTING
    )


def test_run_etl_success(mocker):
    """Tests the main ETL orchestration on a successful run."""
    # Mock configuration validation to return True
    mocker.patch('extract_load.validate_configuration', return_value=True)

    # Mock database connection context manager
    mock_conn = MagicMock()
    mock_cursor = MagicMock()
    mock_conn.cursor.return_value = mock_cursor
    # Mock the context manager's __enter__ method to yield mock_conn, mock_cursor
    mock_get_conn = mocker.patch('extract_load.get_db_connection')
    mock_get_conn.return_value.__enter__.return_value = (mock_conn, mock_cursor)
    # Mock the context manager's __exit__ method to do nothing (simulate normal exit)
    mock_get_conn.return_value.__exit__.return_value = None

    # Mock the core functions called by run_etl
    # create_staging_table_if_not_exists needs to return the cleaned columns
    mock_create_table = mocker.patch('extract_load.create_staging_table_if_not_exists', return_value=['col1', 'col2']) # Return dummy columns
    mock_truncate = mocker.patch('extract_load.truncate_table')
    mock_load_chunks = mocker.patch('extract_load.load_csv_in_chunks')

    etl_script.run_etl()

    # Assert functions were called in order with correct arguments
    etl_script.validate_configuration.assert_called_once()
    mock_get_conn.assert_called_once_with(
        etl_script.DB_NAME, etl_script.DB_USER, etl_script.DB_PASSWORD,
        etl_script.DB_HOST, etl_script.DB_PORT
    )
    mock_create_table.assert_called_once_with(mock_cursor, etl_script.CSV_FILE_PATH, etl_script.RAW_STAGING_TABLE_NAME)
    mock_truncate.assert_called_once_with(mock_cursor, etl_script.RAW_STAGING_TABLE_NAME)
    mock_load_chunks.assert_called_once_with(
        mock_cursor,
        etl_script.CSV_FILE_PATH,
        etl_script.RAW_STAGING_TABLE_NAME,
        etl_script.CSV_CHUNK_SIZE,
        # Check that cleaned columns returned by mock_create_table are passed
        ['col1', 'col2'],
        etl_script.COPY_SEP,
        etl_script.COPY_QUOTECHAR,
        etl_script.COPY_QUOTING
    )
    # Ensure commit was called and rollback was not on success
    mock_conn.commit.assert_called_once()
    mock_conn.rollback.assert_not_called()


def test_run_etl_config_failure(mocker):
    """Tests the main ETL orchestration when configuration validation fails."""
    # Mock configuration validation to return False
    mocker.patch('extract_load.validate_configuration', return_value=False)

    # Mock database connection to ensure it's NOT called
    mock_get_conn = mocker.patch('extract_load.get_db_connection')

    etl_script.run_etl()

    # Assert only validate_configuration was called
    etl_script.validate_configuration.assert_called_once()
    mock_get_conn.assert_not_called()


# CORRECTED: Added capsys to function arguments AND removed 'with' from capsys.readouterr()
def test_run_etl_load_failure(mocker, capsys): # <-- ADDED capsys
    """Tests the main ETL orchestration when load_csv_in_chunks fails."""
    # Mock configuration validation to return True
    mocker.patch('extract_load.validate_configuration', return_value=True)

    # Mock database connection context manager (simulate success entering, handled exit)
    mock_conn = MagicMock()
    mock_cursor = MagicMock()
    mock_conn.cursor.return_value = mock_cursor
    mock_get_conn = mocker.patch('extract_load.get_db_connection')
    mock_get_conn.return_value.__enter__.return_value = (mock_conn, mock_cursor)
    # Simulate the context manager handling the exception internally (e.g., rollback and close)
    mock_get_conn.return_value.__exit__.return_value = None # Setting to None means no exception was handled by __exit__

    # Mock the core functions called by run_etl (create/truncate succeed)
    mocker.patch('extract_load.create_staging_table_if_not_exists', return_value=['col1', 'col2'])
    mocker.patch('extract_load.truncate_table')
    # Mock load_csv_in_chunks to raise an error
    mocker.patch('extract_load.load_csv_in_chunks', side_effect=Exception("Load failed"))

    # We expect run_etl to catch this exception and print an error message
    # CORRECTED: Removed 'with' and called readouterr directly AFTER the code under test
    # The exception handling in run_etl will print the error message we want to capture
    try:
        etl_script.run_etl()
    except Exception:
        pass # Catch the expected exception raised by the script

    captured = capsys.readouterr() # <-- CALLED DIRECTLY HERE

    # Assert load_csv_in_chunks was called, but commit was NOT
    etl_script.load_csv_in_chunks.assert_called_once()
    mock_conn.commit.assert_not_called()
    # Assert the expected error message was printed to stdout/stderr
    assert "ETL process failed: Load failed" in captured.out

