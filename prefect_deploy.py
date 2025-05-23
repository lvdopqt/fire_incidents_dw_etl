# prefect_deploy.py
import csv
import os
from datetime import timedelta

from prefect.deployments import Deployment
from prefect.server.schemas.schedules import IntervalSchedule
from prefect.filesystems import LocalFileSystem

from src.main_pipeline import fire_incidents_full_pipeline

# Storage block for flow code. This block must be created via Prefect CLI:
# prefect block create local-file-system --name "local-repo-etl" --base-path "$(pwd)"
storage_block_name = os.getenv("PREFECT_STORAGE_BLOCK", "local-repo-etl")
storage = LocalFileSystem.load(storage_block_name) # This line now assumes the block already exists

default_flow_parameters = {
    "csv_file_path": "/app/data/Fire_Incidents_20250515.csv",
    "db_user": os.getenv('DB_USER'),
    "db_password": os.getenv('DB_PASSWORD'),
    "db_name": os.getenv('DB_NAME'),
    "db_host": os.getenv('DB_HOST'),
    "db_port": os.getenv('DB_PORT'),
    "raw_staging_table_name": "stg_fire_incidents_raw",
    "csv_chunk_size": 10000,
    "copy_sep": '\t',
    "copy_quotechar": '"',
    "copy_quoting": csv.QUOTE_MINIMAL,
    "dbt_profile_block_name": "fire-incidents-dwh-dbt-profile",
    "dbt_project_dir": "/app/dbt_project"
}

# Build and apply the Prefect Deployment.
deployment = Deployment.build_from_flow(
    flow=fire_incidents_full_pipeline,
    name="fire-incidents-full-pipeline-deployment",
    version="1.0", # Increment for significant deployment changes.
    description="Full ETL pipeline for Fire Incidents data including dbt transformations and tests.",
    tags=["etl", "dbt", "fire_incidents"],
    schedule=IntervalSchedule(interval=timedelta(days=1)), # Daily schedule.
    storage=storage,
    work_queue_name="default-agent-pool", # Must match the agent's work pool name.
    parameters=default_flow_parameters,
    entrypoint="src/main_pipeline.py:fire_incidents_full_pipeline"
)

if __name__ == "__main__":
    print("Applying Prefect Deployment: fire-incidents-full-pipeline-deployment...")
    deployment.apply()
    print("Deployment applied successfully.")