DAGSTER_HOME := $(PWD)/.dagster_home
DAGSTER_MODULE := flight_performance_analytics_pipeline.definitions

export DAGSTER_HOME

.PHONY: help dev ingest silver silver-run silver-checks gold gold-run gold-checks gold-load gold-load-month daily-run-test backfill-run db-up db-down db-reset db-check ch-up ch-down ch-reset ch-check ch-init dbt-compile

help: ## Show available commands
	@grep -E '^[a-zA-Z_-]+:.*?## .*$$' $(MAKEFILE_LIST) | awk 'BEGIN {FS = ":.*?## "}; {printf "  \033[36m%-15s\033[0m %s\n", $$1, $$2}'

dev: ## Start the Dagster UI
	source .venv/bin/activate && .venv/bin/dg dev

ingest: ## Run the full bronze ingestion pipeline
	set -a; source .env; set +a; uv run dagster asset materialize -m $(DAGSTER_MODULE) --select "*" --config run_config.yaml

silver-run: ## Build staging models and run dbt checks
	set -a; source .env; set +a; .venv/bin/dbt build \
		--project-dir src/flight_performance_analytics_pipeline/dbt_transformations \
		--profiles-dir src/flight_performance_analytics_pipeline/dbt_transformations \
		--select staging

silver: ## Build staging models without running dbt checks
	set -a; source .env; set +a; .venv/bin/dbt run \
		--project-dir src/flight_performance_analytics_pipeline/dbt_transformations \
		--profiles-dir src/flight_performance_analytics_pipeline/dbt_transformations \
		--select staging

silver-checks: ## Run only dbt checks for staging models
	set -a; source .env; set +a; .venv/bin/dbt test \
		--project-dir src/flight_performance_analytics_pipeline/dbt_transformations \
		--profiles-dir src/flight_performance_analytics_pipeline/dbt_transformations \
		--select staging

gold: ## Build gold mart models and run dbt checks, then load into ClickHouse
	set -a; source .env; set +a; .venv/bin/dbt build \
		--project-dir src/flight_performance_analytics_pipeline/dbt_transformations \
		--profiles-dir src/flight_performance_analytics_pipeline/dbt_transformations \
		--select marts

gold-run: ## Build gold mart models without running dbt checks
	set -a; source .env; set +a; .venv/bin/dbt run \
		--project-dir src/flight_performance_analytics_pipeline/dbt_transformations \
		--profiles-dir src/flight_performance_analytics_pipeline/dbt_transformations \
		--select marts

gold-checks: ## Run only dbt checks for gold mart models
	set -a; source .env; set +a; .venv/bin/dbt test \
		--project-dir src/flight_performance_analytics_pipeline/dbt_transformations \
		--profiles-dir src/flight_performance_analytics_pipeline/dbt_transformations \
		--select marts

gold-load: ## Load all gold data into ClickHouse: dims (full) + fact (all monthly partitions)
	set -a; source .env; set +a; uv run dagster asset materialize \
		-m $(DAGSTER_MODULE) \
		--select 'gold_dim_carrier,gold_dim_airport,gold_dim_date'
	set -a; source .env; set +a; for m in 2024-01-01 2024-02-01 2024-03-01 2024-04-01 2024-05-01 2024-06-01 2024-07-01 2024-08-01 2024-09-01 2024-10-01 2024-11-01 2024-12-01 2025-01-01 2025-02-01 2025-03-01 2025-04-01 2025-05-01 2025-06-01 2025-07-01 2025-08-01 2025-09-01 2025-10-01 2025-11-01 2025-12-01; do uv run dagster asset materialize -m $(DAGSTER_MODULE) --select gold_fact_flight_delays --partition $$m || exit 1; done

gold-load-month: ## Load a single monthly fact partition into ClickHouse (MONTH=YYYY-MM-DD)
	@if [ -z "$(MONTH)" ]; then echo "Usage: make gold-load-month MONTH=2024-01-01"; exit 1; fi
	set -a; source .env; set +a; uv run dagster asset materialize \
		-m $(DAGSTER_MODULE) \
		--select gold_fact_flight_delays \
		--partition $(MONTH)

daily-run-test: ## Execute one monthly partition for the automated asset selection (MONTH=YYYY-MM-DD, default=current UTC month)
	@month="$(MONTH)"; \
	if [ -z "$$month" ]; then month=$$(date -u +%Y-%m-01); fi; \
	echo "Running daily asset automation for partition $$month"; \
	set -a; source .env; set +a; uv run dagster asset materialize \
		-m $(DAGSTER_MODULE) \
		--select 'group:bronze,group:silver,group:gold' \
		--partition $$month \
		--config run_config.yaml

backfill-run: ## Execute fact backfill across all configured monthly partitions
	set -a; source .env; set +a; for m in 2024-01-01 2024-02-01 2024-03-01 2024-04-01 2024-05-01 2024-06-01 2024-07-01 2024-08-01 2024-09-01 2024-10-01 2024-11-01 2024-12-01 2025-01-01 2025-02-01 2025-03-01 2025-04-01 2025-05-01 2025-06-01 2025-07-01 2025-08-01 2025-09-01 2025-10-01 2025-11-01 2025-12-01; do \
		echo "Backfill partition $$m"; \
		uv run dagster asset materialize -m $(DAGSTER_MODULE) --select gold_fact_flight_delays --partition $$m || exit 1; \
	done

db-up: ## Start the PostgreSQL container
	docker compose up postgres -d

db-down: ## Stop all containers
	docker compose down

db-reset: ## Wipe and restart the PostgreSQL container (drops all data)
	docker compose down -v && docker compose up postgres -d

db-check: ## Check if PostgreSQL is accepting connections
	docker exec flight-performance-analytics-pipeline-postgres-1 pg_isready -U postgres

ch-up: ## Start the ClickHouse container
	docker compose up clickhouse -d

ch-down: ## Stop the ClickHouse container
	docker compose stop clickhouse

ch-reset: ## Wipe and restart the ClickHouse container (drops all data)
	docker compose rm -sf clickhouse && docker volume rm -f flight-performance-analytics-pipeline_clickhouse_data && docker compose up clickhouse -d

ch-check: ## Show all tables in the gold database
	docker exec flight-performance-analytics-pipeline-clickhouse-1 clickhouse-client --query "SHOW TABLES FROM gold"

ch-init: ## Manually run the ClickHouse init SQL scripts (use if container was already running when scripts were added)
	docker exec -i flight-performance-analytics-pipeline-clickhouse-1 clickhouse-client < sql_scripts/clickhouse/01_create_databases.sql
	docker exec -i flight-performance-analytics-pipeline-clickhouse-1 clickhouse-client < sql_scripts/clickhouse/02_create_dim_tables.sql
	docker exec -i flight-performance-analytics-pipeline-clickhouse-1 clickhouse-client < sql_scripts/clickhouse/03_create_fact_tables.sql
	docker exec -i flight-performance-analytics-pipeline-clickhouse-1 clickhouse-client < sql_scripts/clickhouse/04_optimizations.sql

dbt-compile: ## Compile dbt models and regenerate the manifest (required before make dev after model changes)
	.venv/bin/dbt compile \
		--project-dir src/flight_performance_analytics_pipeline/dbt_transformations \
		--profiles-dir src/flight_performance_analytics_pipeline/dbt_transformations
