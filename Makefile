DAGSTER_HOME := $(PWD)/.dagster_home
DAGSTER_MODULE := flight_performance_analytics_pipeline.definitions

export DAGSTER_HOME

.PHONY: help dev ingest silver silver-run silver-checks db-up db-down db-reset db-check dbt-compile

help: ## Show available commands
	@grep -E '^[a-zA-Z_-]+:.*?## .*$$' $(MAKEFILE_LIST) | awk 'BEGIN {FS = ":.*?## "}; {printf "  \033[36m%-15s\033[0m %s\n", $$1, $$2}'

dev: ## Start the Dagster UI
	source .venv/bin/activate && .venv/bin/dg dev

ingest: ## Run the full bronze ingestion pipeline
	.venv/bin/dagster asset materialize -m $(DAGSTER_MODULE) --select "*" --config run_config.yaml

silver: ## Build staging models and run dbt checks
	set -a; source .env; set +a; .venv/bin/dbt build \
		--project-dir src/flight_performance_analytics_pipeline/dbt_transformations \
		--profiles-dir src/flight_performance_analytics_pipeline/dbt_transformations \
		--select staging

silver-run: ## Build staging models without running dbt checks
	set -a; source .env; set +a; .venv/bin/dbt run \
		--project-dir src/flight_performance_analytics_pipeline/dbt_transformations \
		--profiles-dir src/flight_performance_analytics_pipeline/dbt_transformations \
		--select staging

silver-checks: ## Run only dbt checks for staging models
	set -a; source .env; set +a; .venv/bin/dbt test \
		--project-dir src/flight_performance_analytics_pipeline/dbt_transformations \
		--profiles-dir src/flight_performance_analytics_pipeline/dbt_transformations \
		--select staging

db-up: ## Start the PostgreSQL container
	docker compose up postgres -d

db-down: ## Stop all containers
	docker compose down

db-reset: ## Wipe and restart the PostgreSQL container (drops all data)
	docker compose down -v && docker compose up postgres -d

db-check: ## Check if PostgreSQL is accepting connections
	docker exec flight-performance-analytics-pipeline-postgres-1 pg_isready -U postgres

dbt-compile: ## Compile dbt models and regenerate the manifest (required before make dev after model changes)
	.venv/bin/dbt compile \
		--project-dir src/flight_performance_analytics_pipeline/dbt_transformations \
		--profiles-dir src/flight_performance_analytics_pipeline/dbt_transformations
