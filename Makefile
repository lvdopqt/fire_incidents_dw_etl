# .PHONY defines targets that do not correspond to file names
.PHONY: up down build bash run-pipeline run-queries clean test help

# Default target - shows help if no target is specified
default: help

# Displays available options
help:
	@echo "Usage: make <target>"
	@echo ""
	@echo "Targets:"
	@echo "  up           - Starts the Docker Compose services (db, prefect-server, prefect-agent) in detached mode."
	@echo "  down         - Stops and removes the Docker Compose services."
	@echo "  run-pipeline - Triggers a run of the Prefect ETL pipeline deployment."
	@echo "  run-queries  - Executes example SQL queries against the data warehouse."
	@echo "  test         - Run unit tests for the pipeline."
	@echo "  clean        - Cleans dbt artifacts and Python cache."
	@echo "  help         - Shows this help message."

# Starts services in detached mode (-d)
up:
	@echo "Starting Docker Compose services..."
	docker compose up -d
	@echo "Docker Compose services started."
	@echo "Wait for Prefect server and agent to fully initialize and deployments/blocks to be created (approx. 20-30 seconds)..."
	@sleep 30 # Give Prefect server and agent time to be fully ready and for deployments/blocks to be registered

# Stops and removes services
down:
	@echo "Stopping Docker Compose services..."
	docker compose down
	@echo "Docker Compose services stopped."

# Triggers a run of the Prefect ETL pipeline deployment
# Assumes 'fire-incidents-full-pipeline-deployment' is created by prefect_deploy.py
# This command should be run from the host after 'make up' has completed and agent is running.
run-pipeline:
	@echo "Triggering Prefect ETL pipeline deployment 'fire-incidents-full-pipeline-deployment'..."
	# Execute this command from the prefect-agent container as it has access to the Prefect API URL
	docker compose exec prefect-agent python src/main_pipeline.py
	@echo "Prefect ETL pipeline run triggered. Check Prefect UI (http://localhost:4200) for status."

# Executes example SQL queries against the data warehouse
# Assumes 'fire-incidents-full-pipeline-deployment' is created by prefect_deploy.py
# This command should be run from the host after 'make up' has completed and agent is running.
run-queries:
	@echo "Running example SQL queries and saving output to reports/..."
	# Execute a bash command inside the db container that runs psql and redirects output
	# Query 1: Total number of incidents per year
	docker compose exec db bash -c "psql -U admin -d fire_dwh_db -c \"SELECT t.year, COUNT(f.incident_number) AS total_incidents FROM public.fct_fire_incidents f JOIN public.dim_time t ON f.incident_time_key = t.time_key GROUP BY t.year ORDER BY t.year;\" > /app/reports/query_1_incidents_per_year.txt"
	# Query 2: Number of incidents by primary situation and year
	docker compose exec db bash -c "psql -U admin -d fire_dwh_db -c \"SELECT t.year, f.primary_situation, COUNT(f.incident_number) AS total_incidents FROM public.fct_fire_incidents f JOIN public.dim_time t ON f.incident_time_key = t.time_key WHERE f.primary_situation IS NOT NULL GROUP BY t.year, f.primary_situation ORDER BY t.year, total_incidents DESC;\" > /app/reports/query_2_incidents_by_situation_year.txt"
	# Query 3: Average number of suppression units per battalion
	docker compose exec db bash -c "psql -U admin -d fire_dwh_db -c \"SELECT b.battalion_name, AVG(f.suppression_units) AS average_suppression_units FROM public.fct_fire_incidents f JOIN public.dim_battalion b ON f.battalion_key = b.battalion_key WHERE f.suppression_units IS NOT NULL GROUP BY b.battalion_name ORDER BY average_suppression_units DESC;\" > /app/reports/query_3_avg_units_per_battalion.txt"
	@echo "Example SQL queries finished. Results saved in the reports/ directory."


# Runs unit tests using pytest inside the prefect-agent container
test:
	@echo "Running unit tests..."
	docker compose exec prefect-agent pytest tests/
	@echo "Unit tests finished."

# Cleans dbt artifacts, Python cache, and other temporary files
clean:
	@echo "Cleaning dbt artifacts and Python cache..."
	# Clean dbt artifacts inside the prefect-agent container
	docker compose exec prefect-agent dbt clean --profiles-dir /app/dbt_project --project-dir /app/dbt_project || true
	# Remove __pycache__ and .pytest_cache locally (outside container, as they are host-mounted)
	find . -type d -name "__pycache__" -exec rm -rf {} + || true
	find . -type d -name ".pytest_cache" -exec rm -rf {} + || true
	@echo "Clean-up complete."