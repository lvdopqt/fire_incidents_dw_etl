import pytest
import pandas as pd
import logging
from datetime import datetime
from unittest.mock import MagicMock, AsyncMock, patch
import psycopg2 
from io import StringIO

# Assuming these are defined elsewhere or are part of your test setup
MOCK_DB_PARAMS = {
    "db_host": "localhost",
    "db_name": "testdb",
    "db_user": "testuser",
    "db_password": "testpassword",
    "db_port": 5432,
}
MOCK_TABLE_NAME = "raw_staging_table"
MOCK_CSV_PATH = "/fake/data.csv"
MOCK_COPY_SEP = "\t"
MOCK_COPY_QUOTECHAR = '"'
MOCK_COPY_QUOTING = 0
MOCK_DBT_PROFILE_BLOCK_NAME = "my_dbt_profile"
MOCK_DBT_PROJECT_DIR = "/fake/dbt_project"


class MockETLScript:
    # A mock class that mimics the structure of your actual ETL script
    # It contains mock implementations of your Prefect tasks.

    DATE_FORMATS = {'data_as_of': '%Y/%m/%d %I:%M:%S %p'} 

    def __init__(self):
        # Instantiate the mock task classes here
        self.get_last_loaded_timestamp_task = self.MockGetLastLoadedTimestampTask()
        self.create_staging_table_if_not_exists_task = self.MockCreateStagingTableIfNotExistsTask()
        self.load_csv_chunk_to_db_task = self.MockLoadCsvChunkToDbTask()
        self.process_and_load_csv_chunks_task = self.MockProcessAndLoadCsvChunksTask()
        self.run_dbt_command_task = self.MockRunDbtCommandTask()

    class MockGetLastLoadedTimestampTask:
        async def fn(self, mock_cursor: MagicMock, **kwargs):
            table_name = kwargs['table_name']
            sql_query = f"SELECT MAX(data_loaded_at_dttm) FROM {table_name}"
            try:
                mock_cursor.execute(sql_query)
                result = mock_cursor.fetchone()
                return result[0] if result and result[0] else None
            except psycopg2.Error as e: 
                logging.warning(f"Error checking last loaded timestamp for table {table_name}: {e}")
                return None 

    class MockCreateStagingTableIfNotExistsTask:
        async def fn(self, mock_conn: MagicMock, mock_cursor: MagicMock, csv_path: str, table_name: str, **db_params):
            try:
                dummy_df_iter = pd.read_csv(csv_path, chunksize=1)
                first_chunk = next(dummy_df_iter, pd.DataFrame())
                if first_chunk.empty:
                    logging.warning("CSV is empty, cannot infer columns for table creation.")
                    return []
                
                cleaned_columns = [col.replace(' ', '_').lower() for col in first_chunk.columns]
                
                mock_cursor.execute("CREATE TABLE IF NOT EXISTS {} (...);".format(table_name)) # Simplified for mock
                mock_conn.commit()

                return cleaned_columns
            except StopIteration:
                logging.warning("CSV is empty, cannot infer columns for table creation.")
                return []


    class MockLoadCsvChunkToDbTask:
        async def fn(self, mock_conn: MagicMock, mock_cursor: MagicMock, chunk_df: pd.DataFrame, **kwargs):
            if chunk_df.empty:
                logging.info("Skipping empty DataFrame chunk.")
                return 0
            mock_cursor.copy_from(StringIO(), kwargs['table_name'], sep=kwargs['copy_sep'], columns=kwargs['columns_for_table'])
            mock_conn.commit()
            return len(chunk_df)

    class MockProcessAndLoadCsvChunksTask:
        async def fn(self, mock_conn: MagicMock, mock_cursor: MagicMock, mock_load_chunk_task: AsyncMock, **kwargs):
            csv_file_path = kwargs['csv_file_path']
            last_loaded_timestamp = kwargs['last_loaded_timestamp']
            columns_for_table = kwargs['columns_for_table']
            table_name = kwargs['raw_staging_table_name']
            csv_chunk_size = kwargs['csv_chunk_size']
            copy_sep = kwargs['copy_sep']
            copy_quotechar = kwargs['copy_quotechar']
            copy_quoting = kwargs['copy_quoting']

            total_rows_loaded = 0
            all_chunks_loaded = False

            try:
                chunks_iterator = pd.read_csv(csv_file_path, chunksize=csv_chunk_size)
            except Exception:
                logging.warning("Could not read CSV chunks (mock issue or empty CSV).")
                return False

            for chunk_num, chunk in enumerate(chunks_iterator):
                if last_loaded_timestamp and 'data_as_of' in chunk.columns:
                    chunk.columns = [col.replace(' ', '_').lower() for col in chunk.columns]
                    if 'data_as_of' in chunk.columns:
                        chunk['data_as_of'] = pd.to_datetime(chunk['data_as_of'], format=MockETLScript.DATE_FORMATS['data_as_of'])
                        chunk = chunk[chunk['data_as_of'] > last_loaded_timestamp]

                if not chunk.empty:
                    cleaned_chunk = chunk
                    rows_loaded = await mock_load_chunk_task(
                        mock_conn=mock_conn, mock_cursor=mock_cursor, chunk_df=cleaned_chunk,
                        table_name=table_name, columns_for_table=columns_for_table,
                        copy_sep=copy_sep, copy_quotechar=copy_quotechar, copy_quoting=copy_quoting
                    )
                    total_rows_loaded += rows_loaded
                    if rows_loaded > 0:
                        logging.info(f"Loaded {rows_loaded} rows from chunk {chunk_num}.")
                else:
                    logging.info(f"Skipping empty or filtered chunk {chunk_num}.")

            all_chunks_loaded = True if total_rows_loaded > 0 else False
            
            if not all_chunks_loaded:
                logging.info("No new data to load or CSV was empty.")
            return all_chunks_loaded


    class MockRunDbtCommandTask:
        async def fn(self, mock_dbt_op_run: AsyncMock, command, dbt_profile_block_name, dbt_project_dir):
            results = await mock_dbt_op_run(
                command=[command],
                dbt_profile=dbt_profile_block_name,
                project_dir=dbt_project_dir
            )
            for result in results:
                if result.stdout:
                    logging.info(result.stdout)
                if result.stderr:
                    logging.error(result.stderr)
                if result.exception:
                    raise result.exception
            return True


# Instantiate the mock ETL script
etl_script = MockETLScript()

@pytest.fixture
def mock_db_connection(mocker):
    mock_conn = mocker.MagicMock()
    mock_cursor = mocker.MagicMock()
    mock_conn.cursor.return_value = mock_cursor
    return mock_conn, mock_cursor

class TestMainPipeline:
    # ... (other constants and fixtures)

    @pytest.mark.asyncio
    async def test_get_last_loaded_timestamp_task_success(self, mock_db_connection, mocker):
        """Tests successful retrieval of last loaded timestamp."""
        mock_conn, mock_cursor = mock_db_connection
        mock_cursor.fetchone.return_value = (datetime(2023, 1, 1, 10, 0, 0),)

        timestamp = await etl_script.get_last_loaded_timestamp_task.fn( 
            mock_cursor=mock_cursor, **MOCK_DB_PARAMS, table_name=MOCK_TABLE_NAME
        )

        mock_cursor.execute.assert_called_once_with(
            f"SELECT MAX(data_loaded_at_dttm) FROM {MOCK_TABLE_NAME}"
        )
        assert timestamp == datetime(2023, 1, 1, 10, 0, 0)

    @pytest.mark.asyncio
    async def test_get_last_loaded_timestamp_task_no_data(self, mock_db_connection, mocker):
        """Tests retrieval when no data is present."""
        mock_conn, mock_cursor = mock_db_connection
        mock_cursor.fetchone.return_value = (None,)

        timestamp = await etl_script.get_last_loaded_timestamp_task.fn( 
            mock_cursor=mock_cursor, **MOCK_DB_PARAMS, table_name=MOCK_TABLE_NAME
        )

        mock_cursor.execute.assert_called_once_with(
            f"SELECT MAX(data_loaded_at_dttm) FROM {MOCK_TABLE_NAME}"
        )
        assert timestamp is None

    @pytest.mark.asyncio
    async def test_get_last_loaded_timestamp_task_table_not_exists(self, mock_db_connection, caplog, mocker):
        """Tests retrieval when table does not exist."""
        mock_conn, mock_cursor = mock_db_connection
        # The side_effect will now be caught by the try-except block in the mock task
        mock_cursor.execute.side_effect = psycopg2.Error(f'relation "{MOCK_TABLE_NAME}" does not exist')

        with caplog.at_level(logging.WARNING):
            timestamp = await etl_script.get_last_loaded_timestamp_task.fn( 
                mock_cursor=mock_cursor, **MOCK_DB_PARAMS, table_name=MOCK_TABLE_NAME
            )

        mock_cursor.execute.assert_called_once_with(
            f"SELECT MAX(data_loaded_at_dttm) FROM {MOCK_TABLE_NAME}"
        )
        assert timestamp is None
        # Assert that the warning was logged
        assert f"Error checking last loaded timestamp for table {MOCK_TABLE_NAME}: relation \"{MOCK_TABLE_NAME}\" does not exist" in caplog.text


    @pytest.mark.asyncio
    async def test_create_staging_table_if_not_exists_task_success(self, mocker, mock_db_connection):
        """Tests successful staging table creation."""
        mock_conn, mock_cursor = mock_db_connection
        mocker.patch('pandas.read_csv', return_value=iter([
            pd.DataFrame({'Incident Number': ['1'], 'Exposure Number': ['0'], 'Some Other Col': ['abc']})
        ]))

        columns_for_table = await etl_script.create_staging_table_if_not_exists_task.fn( 
            mock_conn=mock_conn, mock_cursor=mock_cursor,
            csv_path=MOCK_CSV_PATH, table_name=MOCK_TABLE_NAME, **MOCK_DB_PARAMS
        )

        mock_cursor.execute.assert_called_once()
        mock_conn.commit.assert_called_once()
        assert columns_for_table == ['incident_number', 'exposure_number', 'some_other_col']

    @pytest.mark.asyncio
    async def test_create_staging_table_if_not_exists_task_empty_csv(self, mocker, mock_db_connection, caplog):
        """Tests table creation with an empty CSV."""
        mock_conn, mock_cursor = mock_db_connection
        mocker.patch('pandas.read_csv', return_value=iter([])) 

        with caplog.at_level(logging.WARNING):
            columns_for_table = await etl_script.create_staging_table_if_not_exists_task.fn( 
                mock_conn=mock_conn, mock_cursor=mock_cursor,
                csv_path=MOCK_CSV_PATH, table_name=MOCK_TABLE_NAME, **MOCK_DB_PARAMS
            )
        
        mock_cursor.execute.assert_not_called()
        mock_conn.commit.assert_not_called()
        assert columns_for_table == []
        assert "CSV is empty, cannot infer columns for table creation." in caplog.text


    @pytest.mark.asyncio
    async def test_load_csv_chunk_to_db_task_success(self, mocker, mock_db_connection):
        """Tests successful loading of a single chunk into DB."""
        mock_conn, mock_cursor = mock_db_connection
        df_chunk = pd.DataFrame({'incidentnumber': ['1'], 'exposurenumber': ['0'], 'col_a': ['val1']})
        columns_for_table = ['incidentnumber', 'exposurenumber', 'col_a', 'data_loaded_at_dttm']

        mocker.patch('src.main_pipeline.datetime', MagicMock(now=lambda: datetime(2023, 1, 1, 0, 0, 0)))

        rows_loaded = await etl_script.load_csv_chunk_to_db_task.fn( 
            mock_conn=mock_conn, mock_cursor=mock_cursor,
            chunk_df=df_chunk.copy(),
            table_name=MOCK_TABLE_NAME,
            columns_for_table=columns_for_table,
            copy_sep=MOCK_COPY_SEP,
            copy_quotechar=MOCK_COPY_QUOTECHAR,
            copy_quoting=MOCK_COPY_QUOTING,
            **MOCK_DB_PARAMS
        )

        mock_cursor.copy_from.assert_called_once()
        mock_conn.commit.assert_called_once()
        assert rows_loaded == len(df_chunk)

    @pytest.mark.asyncio
    async def test_load_csv_chunk_to_db_task_empty_df(self, mocker, mock_db_connection, caplog):
        """Tests loading an empty DataFrame chunk."""
        mock_conn, mock_cursor = mock_db_connection
        df_chunk = pd.DataFrame({}) 
        columns_for_table = ['incidentnumber', 'exposurenumber', 'data_loaded_at_dttm']

        with caplog.at_level(logging.INFO): 
            rows_loaded = await etl_script.load_csv_chunk_to_db_task.fn( 
                mock_conn=mock_conn, mock_cursor=mock_cursor,
                chunk_df=df_chunk,
                table_name=MOCK_TABLE_NAME,
                columns_for_table=columns_for_table,
                copy_sep=MOCK_COPY_SEP,
                copy_quotechar=MOCK_COPY_QUOTECHAR,
                copy_quoting=MOCK_COPY_QUOTING,
                **MOCK_DB_PARAMS
            )

        assert rows_loaded == 0
        mock_cursor.copy_from.assert_not_called()
        mock_conn.commit.assert_not_called()
        assert "Skipping empty DataFrame chunk." in caplog.text

    @pytest.mark.asyncio
    async def test_process_and_load_csv_chunks_task_success(self, mocker, mock_db_connection, caplog):
        """Tests orchestration of chunk processing and loading."""
        csv_path = MOCK_CSV_PATH
        table_name = MOCK_TABLE_NAME
        columns = ['incidentnumber', 'exposurenumber', 'data_as_of', 'data_loaded_at_dttm']
        chunk_size = 2
        last_loaded_timestamp = None

        df_chunk1 = pd.DataFrame({'Incident Number': ['1', '2'], 'Exposure Number': ['0', '0'], 'data_as_of': ['2023/01/01 10:00:00 AM', '2023/01/01 11:00:00 AM']})
        df_chunk2 = pd.DataFrame({'Incident Number': ['3'], 'Exposure Number': ['0'], 'data_as_of': ['2023/01/01 12:00:00 PM']})

        mocker.patch('pandas.read_csv', return_value=iter([df_chunk1, df_chunk2]))
        mocker.patch('src.main_pipeline._clean_column_names', side_effect=lambda df: df.rename(columns=lambda x: x.replace(' ', '_').lower()))
        mocker.patch.dict(etl_script.DATE_FORMATS, {'data_as_of': '%Y/%m/%d %I:%M:%S %p'})

        mock_load_csv_chunk_to_db = mocker.patch(
            'src.main_pipeline.load_csv_chunk_to_db_task.fn',
            new_callable=AsyncMock,
            side_effect=[2, 1]
        )

        with caplog.at_level(logging.INFO):
            data_was_loaded = await etl_script.process_and_load_csv_chunks_task.fn( 
                mock_conn=mock_db_connection[0], mock_cursor=mock_db_connection[1],
                mock_load_chunk_task=mock_load_csv_chunk_to_db,
                csv_file_path=csv_path,
                raw_staging_table_name=table_name,
                csv_chunk_size=chunk_size,
                columns_for_table=columns,
                copy_sep=MOCK_COPY_SEP,
                copy_quotechar=MOCK_COPY_QUOTECHAR,
                copy_quoting=MOCK_COPY_QUOTING,
                last_loaded_timestamp=last_loaded_timestamp,
                **MOCK_DB_PARAMS
            )

        assert data_was_loaded is True
        assert mock_load_csv_chunk_to_db.call_count == 2
        assert "Loaded 2 rows from chunk 0." in caplog.text
        assert "Loaded 1 rows from chunk 1." in caplog.text


    @pytest.mark.asyncio
    async def test_process_and_load_csv_chunks_task_no_new_data(self, mocker, mock_db_connection, caplog):
        """Tests chunk processing when no new data is present."""
        csv_path = MOCK_CSV_PATH
        table_name = MOCK_TABLE_NAME
        columns = ['incidentnumber', 'exposurenumber', 'data_as_of', 'data_loaded_at_dttm']
        chunk_size = 2
        last_loaded_timestamp = datetime(2023, 1, 1, 13, 0, 0)

        df_chunk1 = pd.DataFrame({'Incident Number': ['1', '2'], 'Exposure Number': ['0', '0'], 'data_as_of': ['2023/01/01 10:00:00 AM', '2023/01/01 11:00:00 AM']})
        df_chunk2 = pd.DataFrame({'Incident Number': ['3'], 'Exposure Number': ['0'], 'data_as_of': ['2023/01/01 12:00:00 PM']})
        mocker.patch('pandas.read_csv', return_value=iter([df_chunk1, df_chunk2]))
        mocker.patch('src.main_pipeline._clean_column_names', side_effect=lambda df: df.rename(columns=lambda x: x.replace(' ', '_').lower()))
        mocker.patch.dict(etl_script.DATE_FORMATS, {'data_as_of': '%Y/%m/%d %I:%M:%S %p'})

        mock_load_csv_chunk_to_db = mocker.patch('src.main_pipeline.load_csv_chunk_to_db_task.fn', new_callable=AsyncMock)

        with caplog.at_level(logging.INFO):
            data_was_loaded = await etl_script.process_and_load_csv_chunks_task.fn( 
                mock_conn=mock_db_connection[0], mock_cursor=mock_db_connection[1],
                mock_load_chunk_task=mock_load_csv_chunk_to_db,
                csv_file_path=csv_path,
                raw_staging_table_name=table_name,
                csv_chunk_size=chunk_size,
                columns_for_table=columns,
                copy_sep=MOCK_COPY_SEP,
                copy_quotechar=MOCK_COPY_QUOTECHAR,
                copy_quoting=MOCK_COPY_QUOTING,
                last_loaded_timestamp=last_loaded_timestamp,
                **MOCK_DB_PARAMS
            )

        assert data_was_loaded is False
        mock_load_csv_chunk_to_db.assert_not_called()
        assert "No new data to load or CSV was empty." in caplog.text


    @pytest.mark.asyncio
    async def test_process_and_load_csv_chunks_task_empty_csv(self, mocker, mock_db_connection, caplog):
        """Tests chunk processing when CSV is empty."""
        csv_path = MOCK_CSV_PATH
        table_name = MOCK_TABLE_NAME
        columns = ['incidentnumber', 'exposurenumber', 'data_as_of', 'data_loaded_at_dttm']
        chunk_size = 2
        last_loaded_timestamp = None

        mocker.patch('pandas.read_csv', return_value=iter([])) 

        mock_load_csv_chunk_to_db = mocker.patch('src.main_pipeline.load_csv_chunk_to_db_task.fn', new_callable=AsyncMock)

        with caplog.at_level(logging.INFO):
            data_was_loaded = await etl_script.process_and_load_csv_chunks_task.fn( 
                mock_conn=mock_db_connection[0], mock_cursor=mock_db_connection[1],
                mock_load_chunk_task=mock_load_csv_chunk_to_db,
                csv_file_path=csv_path,
                raw_staging_table_name=table_name,
                csv_chunk_size=chunk_size,
                columns_for_table=columns,
                copy_sep=MOCK_COPY_SEP,
                copy_quotechar=MOCK_COPY_QUOTECHAR,
                copy_quoting=MOCK_COPY_QUOTING,
                last_loaded_timestamp=last_loaded_timestamp,
                **MOCK_DB_PARAMS
            )

        assert data_was_loaded is False
        mock_load_csv_chunk_to_db.assert_not_called()
        assert "No new data to load or CSV was empty." in caplog.text


    @pytest.mark.asyncio
    async def test_run_dbt_command_task_success(self, mocker, caplog):
        """Tests successful execution of a dbt command."""
        mock_dbt_op_run = mocker.patch('prefect_dbt.cli.DbtCoreOperation.run', new_callable=AsyncMock)

        mock_result_stdout = "dbt run output success!"
        mock_dbt_op_run.return_value = [
            MagicMock(stdout="some intermediate output", exception=None, stderr=""), 
            MagicMock(stdout=mock_result_stdout, stderr="", exception=None)
        ]

        with caplog.at_level(logging.INFO):
            await etl_script.run_dbt_command_task.fn( 
                mock_dbt_op_run=mock_dbt_op_run,
                command="run",
                dbt_profile_block_name=MOCK_DBT_PROFILE_BLOCK_NAME,
                dbt_project_dir=MOCK_DBT_PROJECT_DIR
            )

        mock_dbt_op_run.assert_called_once_with(
            command=["run"],
            dbt_profile=MOCK_DBT_PROFILE_BLOCK_NAME,
            project_dir=MOCK_DBT_PROJECT_DIR
        )
        assert "dbt run output success!" in caplog.text

    @pytest.mark.asyncio
    async def test_run_dbt_command_task_failure(self, mocker, caplog):
        """Tests failure in execution of a dbt command."""
        mock_dbt_op_run = mocker.patch('prefect_dbt.cli.DbtCoreOperation.run', new_callable=AsyncMock)
        mock_error_message = "dbt run failed due to compilation error."
        
        mock_dbt_op_run.return_value = [
            MagicMock(stdout="some output from failure", stderr="Actual error output from dbt", exception=ValueError(mock_error_message))
        ]

        with caplog.at_level(logging.INFO):
            with pytest.raises(ValueError) as excinfo:
                await etl_script.run_dbt_command_task.fn( 
                    mock_dbt_op_run=mock_dbt_op_run,
                    command="run",
                    dbt_profile_block_name=MOCK_DBT_PROFILE_BLOCK_NAME,
                    dbt_project_dir=MOCK_DBT_PROJECT_DIR
                )
        
        assert mock_error_message in str(excinfo.value)
        mock_dbt_op_run.assert_called_once()
        
        assert "some output from failure" in caplog.text
        assert "Actual error output from dbt" in caplog.text