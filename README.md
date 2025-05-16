# Fire Incidents Data Warehouse ETL and Modeling

This project implements an Extract, Load, and Transform (ELT) pipeline to load fire incident data into a PostgreSQL data warehouse and model it using dbt for analytical querying.

## Project Structure

```
.
├── data/                         # Contains the source CSV file
│   └── Fire_Incidents_20250515.csv
├── dbt_project/                  # dbt project files
│   ├── dbt_packages/             # Installed dbt packages (ignored by Git)
│   │   └── dbt_utils
│   ├── macros/                   # Custom dbt macros
│   │   └── generic_tests.sql     # Custom generic test macros (if any remain)
│   ├── models/                   # dbt models
│   │   ├── base/                 # Base layer: initial cleaning and casting
│   │   │   └── base_fire_incidents.sql
│   │   ├── facts/                # Fact tables
│   │   │   └── fct_fire_incidents.sql
│   │   ├── mart/                 # Mart layer: dimension tables
│   │   │   ├── dim_battalion.sql
│   │   │   ├── dim_district.sql
│   │   │   └── dim_time.sql
│   │   ├── staging/              # Staging layer: typically selects from base
│   │   │   ├── schema.yml        # Source definition
│   │   │   └── stg_fire_incidents.sql
│   │   └── tests/                # Data tests
│   │       ├── fct_fire_incidents_unique_incident_exposure.sql # Singular unique test
│   │       └── schema.yml        # Generic test definitions
│   ├── profiles.yml              # dbt profile configuration
│   ├── dbt_project.yml           # dbt project configuration
│   ├── packages.yml              # dbt package dependencies
│   └── seeds/                    # dbt seed files (if any)
├── docker-compose.yml            # Defines Docker services (PostgreSQL, dbt)
├── Dockerfile                    # Builds the dbt container image
├── Makefile                      # Shortcuts for common commands
├── README.md                     # This file
├── reports/                      # Example reports or queries
│   ├── example_queries.sql
│   ├── query_1_incidents_per_year.txt # Example query output
│   ├── query_2_incidents_by_situation_year.txt # Example query output
│   └── query_3_avg_units_per_battalion.txt # Example query output
├── requirements.txt              # Python dependencies
├── scripts/                      # Shell scripts
│   └── run_pipeline.sh           # Script to orchestrate the ETL/ELT process
└── src/                          # Source code for data extraction/loading
└── extract_load.py           # Python script for ETL
```

## Development Summary

This project leverages Docker to containerize the data warehouse (PostgreSQL) and the data transformation tool (dbt). This provides a consistent and isolated development environment. The Docker image for the dbt container was specifically built to be cross-platform compatible, ensuring it runs correctly on different architectures (like ARM64).

The pipeline follows an ELT (Extract, Load, Transform) pattern:

1.  **Extract & Load:** A Python script (`src/extract_load.py`) reads the source CSV file (`data/Fire_Incidents_20250515.csv`) in chunks and loads the raw data directly into a staging table (`stg_fire_incidents_raw`) in the PostgreSQL database using `psycopg2.copy_from`. The script includes column name cleaning to make them SQL-friendly and handles potential data quality issues like empty strings in timestamp/numeric fields by casting them to `NULL`.
2.  **Transform:** dbt models (`dbt_project/models/`) are used to transform the raw data.
    * The `base` model (`base_fire_incidents.sql`) performs initial cleaning, renaming, and casting of columns from the raw staging table.
    * The `mart` layer contains dimension models (`dim_battalion.sql`, `dim_district.sql`, `dim_time.sql`) that create unique lists of dimension attributes with surrogate keys.
    * The `facts` model (`fct_fire_incidents.sql`) builds the central fact table by joining the base data with the dimension tables to include foreign keys. This model is materialized incrementally for efficient daily updates.

## Data Quality Testing

Data quality tests are implemented using dbt tests defined in `dbt_project/models/tests/schema.yml` and singular test files.

A key test implemented is the check for **uniqueness of the combination of `incident_number` and `exposure_number`** in the `fct_fire_incidents` table. This was implemented as a singular test (`fct_fire_incidents_unique_incident_exposure.sql`) to ensure that each record in the fact table represents a unique incident exposure.

## Assumptions

* The source CSV file is available at the path specified by the `CSV_PATH` environment variable.
* The CSV file has a header row.
* The combination of "Incident Number" and "Exposure Number" uniquely identifies each record in the source data.
* The database is PostgreSQL.
* The source data reflects the "current state" daily, and a truncate-and-load approach for the raw staging table is acceptable.
* The database credentials and name are provided via environment variables (`DB_USER`, `DB_PASSWORD`, `DB_NAME`, `DB_HOST`, `DB_PORT`). So create a env file like this:
```
DB_USER=admin
DB_PASSWORD=reallysecurepassword123
DB_NAME=fire_dwh_db

DB_HOST=db
DB_PORT=5432

CSV_PATH=/app/data/Fire_Incidents_20250515.csv
```

## Usage

1.  **Prerequisites:** Ensure you have Docker and `make` installed.
2.  **Environment Variables:** Create a `.env` file in the root directory of the project based on the provided `.env.example` (if available), filling in your database credentials and CSV file path.
3.  **Build and Run the Pipeline:** Use the Makefile to build the Docker images and run the ETL/ELT pipeline:
    ```bash
    make run-pipeline
    ```
    This command will:
    * Start the PostgreSQL and dbt containers.
    * Install dbt packages.
    * Run the Python script to load data into the raw staging table.
    * Run dbt to build the models and execute tests.

4.  **Run Example Queries:** Use the Makefile to execute predefined analytical queries and save their results to the `reports/` directory:
    ```bash
    make run-queries
    ```

5.  **Access the Data Warehouse:** You can connect to the PostgreSQL database using your preferred SQL client and the credentials from your `.env` file. The modeled data will be available in the `public` schema (or the schema configured in your dbt `profiles.yml`).

6.  **Example Queries:** Refer to the `reports/example_queries.sql` file for example SQL queries. The output of the `make run-queries` command will be saved in `.txt` files within the `reports/` directory.
