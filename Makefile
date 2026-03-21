DAGSTER_HOME := $(PWD)/.dagster_home
DAGSTER_MODULE := flight_performance_analytics_pipeline.definitions

export DAGSTER_HOME

.PHONY: help dev ingest db-up db-down db-reset db-check

help: ## Show available commands
	@grep -E '^[a-zA-Z_-]+:.*?## .*$$' $(MAKEFILE_LIST) | awk 'BEGIN {FS = ":.*?## "}; {printf "  \033[36m%-15s\033[0m %s\n", $$1, $$2}'

dev: ## Start the Dagster UI
	dg dev

ingest: ## Run the full bronze ingestion pipeline
	dagster asset materialize -m $(DAGSTER_MODULE) --select "*" --config run_config.yaml

db-up: ## Start the PostgreSQL container
	docker compose up postgres -d

db-down: ## Stop all containers
	docker compose down

db-reset: ## Wipe and restart the PostgreSQL container (drops all data)
	docker compose down -v && docker compose up postgres -d

db-check: ## Check if PostgreSQL is accepting connections
	docker exec flight-performance-analytics-pipeline-postgres-1 pg_isready -U postgres

dbt-compile: ## Compile dbt models and regenerate the manifest (required before make dev after model changes)
	dbt compile \
		--project-dir src/flight_performance_analytics_pipeline/dbt_transformations \
		--profiles-dir src/flight_performance_analytics_pipeline/dbt_transformations
