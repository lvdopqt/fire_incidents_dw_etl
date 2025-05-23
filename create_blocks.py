import asyncio
import os
from prefect.filesystems import LocalFileSystem
from prefect_dbt.cli import DbtCliProfile

async def create_prefect_blocks():
    print("Creating/Updating Prefect Blocks...")

    os.environ["PREFECT_API_URL"] = os.getenv("PREFECT_API_URL", "http://prefect-server:4200/api")

    local_fs_block = LocalFileSystem(basepath="/app")
    await local_fs_block.save(name="local-repo-etl", overwrite=True)
    print("LocalFileSystem block 'local-repo-etl' created/updated successfully.")

    db_user = os.getenv('DB_USER')
    db_password = os.getenv('DB_PASSWORD')
    db_name = os.getenv('DB_NAME')
    db_host = os.getenv('DB_HOST')
    db_port = os.getenv('DB_PORT')

    if not all([db_user, db_password, db_name, db_host, db_port]):
        print("ERROR: One or more database environment variables (DB_USER, DB_PASSWORD, DB_NAME, DB_HOST, DB_PORT) are not set. Please ensure they are configured in your Docker Compose or environment.")
        return

    dbt_profile_block = DbtCliProfile(
        name="fire_incidents_dwh", 
        target="dev",
        profile_directory="/app/dbt_project",
        target_configs={
            "type": "postgres",
            "host": db_host,
            "port": int(db_port),
            "user": db_user,
            "password": db_password,
            "database": db_name,
            "schema": "public",
            "threads": 1
        }
    )
    await dbt_profile_block.save(name="fire-incidents-dwh-dbt-profile", overwrite=True)
    print("DbtCliProfile block 'fire-incidents-dwh-dbt-profile' created/updated successfully.")

if __name__ == "__main__":
    asyncio.run(create_prefect_blocks())