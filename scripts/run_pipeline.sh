#!/bin/bash

# Exit immediately if a command exits with a non-zero status.
set -e

echo "Starting the ETL pipeline..."

# --- Clean dbt target directory ---
echo "Cleaning dbt target directory..."
# Navigate to the dbt project directory inside the container
# dbt_project is mapped from your local dbt_project folder to /usr/app
cd /usr/app
dbt clean
echo "dbt target directory cleaned."

# --- Install dbt packages (if using packages.yml) ---
echo "Installing dbt packages..."
# packages.yml is located in the dbt project directory
dbt deps
echo "dbt packages installed."

# --- 1. Run the Extract & Load script (Python) ---
echo "Running Python extract_load.py script..."
# extract_load.py is mapped from your local src folder to /app/src/extract_load.py
# The Python environment with pandas, psycopg2, sqlalchemy, etc. is already set up
# during the Docker image build process.
python /app/src/extract_load.py
echo "Python extract_load.py script finished."

# --- 2. Run dbt commands (Transform & Model) ---
echo "Running dbt commands..."

# dbt commands should be run from the dbt project directory, which is /usr/app
# We are already in /usr/app from the 'cd' command above

# Optional: Check dbt connection and configuration
echo "Running dbt debug..."
# dbt debug uses the profiles.yml (which should be correctly configured via .env)
dbt debug --profile fire_incidents_dwh
echo "dbt debug finished."


# Run dbt models (staging, base, mart, facts)
# dbt will automatically resolve dependencies based on ref() calls in models
echo "Running dbt models..."
dbt run --profile fire_incidents_dwh
echo "dbt run finished."

# Run dbt tests
echo "Running dbt tests..."
dbt test --profile fire_incidents_dwh
echo "dbt test finished."

echo "ETL pipeline finished successfully!"
