# .PHONY defines targets that do not correspond to file names
.PHONY: up down build bash run-pipeline clean help

# Default target - shows help if no target is specified
default: help


# Displays available options
help:
	@echo "Usage: make <target>"
	@echo ""
	@echo "Targets:"
	@echo "  up           - Starts the Docker Compose services (db, dbt) in detached mode."
	@echo "  down         - Stops and removes the Docker Compose services."
	@echo "  bash         - Accesses the bash shell inside the 'dbt' container."
	@echo "  run-pipeline - Executes the full ETL pipeline script inside the 'dbt' container."
	@echo "  clean        - Executes cleanup of dbt artifacts (e.g., dbt clean)."
	@echo "  help         - Shows this help message."

# Starts services in detached mode (-d)
up:
	@echo "Starting Docker Compose services..."
	docker compose up -d
	@echo "Docker Compose services started."

# Stops and removes services
down:
	@echo "Stopping Docker Compose services..."
	docker compose down
	@echo "Docker Compose services stopped."

build:
	@echo "Building custom dbt Docker image..."
	docker compose build dbt
	@echo "Custom dbt Docker image built."

# Accesses the bash shell inside the dbt container
bash:
	@echo "Accessing 'dbt' container shell..."
	docker compose exec dbt bash

# Executes the full pipeline script inside the dbt container
# This target depends on the 'up' target to ensure services are running
run-pipeline: up
	@echo "Sourcing .env for Makefile environment..."
	# Source the variables from .env so they are available for make commands (e.g., pg_isready)
	# This syntax works on most Linux/macOS shells. May vary on Windows.
	- export $(cat .env | xargs)
	@echo ".env sourced."

	@echo "Checking database health..."
	# Wait until the 'db' service is healthy. '-t 5' is a short timeout.
	# Uses the variables from .env for the pg_isready connection.
	docker compose exec db pg_isready -U ${DB_USER} -d ${DB_NAME} -t 5 || (echo "Error: Database is not healthy or accessible. Aborting." && exit 1)
	@echo "Database is healthy."

	@echo "Giving the pipeline file proper permissions"
	chmod +x scripts/run_pipeline.sh

	@echo "Executing run_pipeline.sh inside 'dbt' container..."
	# The run_pipeline.sh script is mapped to /app/scripts/run_pipeline.sh inside the container
	docker compose exec dbt bash -c "/app/scripts/run_pipeline.sh"
	@echo "Pipeline execution finished."

# Cleans dbt artifacts
clean:
	@echo "Running dbt clean inside 'dbt' container..."
	docker compose exec dbt bash -c "cd /usr/app && dbt clean"
	@echo "dbt clean finished."

# Check database health
db-health:
	@echo "Checking database health..."
	docker compose exec db pg_isready -U ${DB_USER} -d ${DB_NAME} -t 5 || (echo "Error: Database is not healthy or accessible. Aborting." && exit 1)
	@echo "Database is healthy."

# Run example SQL queries with hardcoded credentials and output to files (redirecting inside container)
run-queries: up db-health
	@echo "Running example SQL queries and saving output to reports/..."
	# Execute a bash command inside the db container that runs psql and redirects output
	# Query 1: Total number of incidents per year
	docker compose exec db bash -c "psql -U admin -d fire_dwh_db -c \"SELECT t.year, COUNT(f.incident_number) AS total_incidents FROM public.fct_fire_incidents f JOIN public.dim_time t ON f.incident_time_key = t.time_key GROUP BY t.year ORDER BY t.year;\" > /app/reports/query_1_incidents_per_year.txt"
	# Query 2: Number of incidents by primary situation and year
	docker compose exec db bash -c "psql -U admin -d fire_dwh_db -c \"SELECT t.year, f.primary_situation, COUNT(f.incident_number) AS total_incidents FROM public.fct_fire_incidents f JOIN public.dim_time t ON f.incident_time_key = t.time_key WHERE f.primary_situation IS NOT NULL GROUP BY t.year, f.primary_situation ORDER BY t.year, total_incidents DESC;\" > /app/reports/query_2_incidents_by_situation_year.txt"
	# Query 3: Average number of suppression units per battalion
	docker compose exec db bash -c "psql -U admin -d fire_dwh_db -c \"SELECT b.battalion_name, AVG(f.suppression_units) AS average_suppression_units FROM public.fct_fire_incidents f JOIN public.dim_battalion b ON f.battalion_key = b.battalion_key WHERE f.suppression_units IS NOT NULL GROUP BY b.battalion_name ORDER BY average_suppression_units DESC;\" > /app/reports/query_3_avg_units_per_battalion.txt"
	@echo "Example SQL queries finished. Results saved in the reports/ directory."

# Runs the pytest test suite inside the dbt container
test: up db-health
	@echo "Sourcing .env for Makefile environment..."
	# Ensure .env variables are available for tests (especially DB connection details)
	- export $(cat .env | xargs)
	@echo ".env sourced."

	@echo "Running tests inside 'dbt' container..."
	# Assuming your code and test file (e.g., test_etl_script.py) are mounted to /usr/app
	# Adjust the 'cd /usr/app' path if your docker-compose.yml maps your project code elsewhere
	docker compose exec dbt bash -c "cd /app && pytest -v -s"
	@echo "Test execution finished."