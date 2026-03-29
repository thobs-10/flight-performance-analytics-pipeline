# flight-performance-analytics-pipeline

Data engineering project that extracts, transforms, and analyzes flight performance data. The pipeline tracks on-time performance, delays, cancellations, diversions, and delay causes across airlines and airports.

The current pipeline supports incremental silver processing, partition-aware gold fact loading, and scheduled Dagster automation for daily runs and historical backfills.

## Architecture

**Medallion Architecture:**
- **Bronze**: Raw flight delay data ingested from external API sources
- **Silver**: Incremental staging layer with cleaned, validated data models in PostgreSQL
- **Gold**: Analytics-ready dimensional and fact tables in ClickHouse (analytical OLAP store)

**Orchestration & Tools:**
- **Dagster**: Asset-based orchestration and data quality checks
- **dbt**: Data transformation and modeling (PostgreSQL staging → Postgres marts)
- **PostgreSQL**: Transactional OLTP storage (bronze/silver/staging)
- **ClickHouse**: Columnar OLAP database for fast analytical queries (gold layer)
- **Python 3.13+**: Core pipeline logic with Polars for efficient data manipulation

**Partitioning & Automation:**
- **Silver Incremental Model**: `stg_airline_delay_data` uses dbt incremental `delete+insert`
- **Gold Fact Partitioning**: Dagster partition key format is `YYYY-MM-DD` by month
- **ClickHouse Physical Partitioning**: `gold.fact_flight_delays` is partitioned by year
- **Daily Automation**: Dagster schedule runs daily at `03:00 UTC`
- **Backfill Support**: Manual monthly replay across the configured partition range

---

## Prerequisites

- **Python 3.13+** (recommended via `pyenv`)
- **Docker & Docker Compose** (for PostgreSQL and ClickHouse containers)
- **uv** package manager (for fast dependency installation)
- **.env** file with database credentials (copy from `.env.example`)

---

## Quick Start

### 1. Initial Setup
```bash
# Clone the repository
git clone <repo-url>
cd flight-performance-analytics-pipeline

# Copy environment template and configure credentials
cp .env.example .env
# Edit .env to set custom PostgreSQL/ClickHouse credentials if needed

# Set up Python environment
python3.13 -m venv .venv
source .venv/bin/activate
uv sync  # Install all dependencies via uv
```

### 2. Start Infrastructure (Docker)
```bash
# Start PostgreSQL container
make db-up

# Verify PostgreSQL is healthy
make db-check

# Start ClickHouse container
make ch-up

# Verify ClickHouse is healthy (after ~10 seconds)
sleep 10
make ch-check  # Should show empty tables initially
```

### 3. Bronze Layer (Data Ingestion)
```bash
# Run full bronze pipeline to ingest flight delay data from external sources
make ingest
```

### 4. Silver Layer (Staging Transformations)
```bash
# Build incremental staging models in PostgreSQL
make silver

# Or run models plus dbt tests
make silver-run
```

### 5. Gold Layer (Analytics Marts + ClickHouse Load)
```bash
# Build Postgres analytics mart models (dbt) with tests
make gold

# Materialize Dagster gold assets and load dims plus all configured fact partitions
make gold-load

# Or load a single fact partition
make gold-load-month MONTH=2025-11-01
```

### 6. Automation
```bash
# Execute the automated daily asset selection for one partition
make daily-run-test MONTH=2025-11-01

# Execute the full historical fact backfill across configured partitions
make backfill-run
```

### 7. Verify Final State
```bash
# Check ClickHouse has all 6 gold objects (3 dims + 1 fact + 2 analytics views)
make ch-check

# Query fact table row count to confirm data load
docker exec flight-performance-analytics-pipeline-clickhouse-1 clickhouse-client --query "SELECT count() FROM gold.fact_flight_delays"

# Inspect row distribution by year/month
docker exec flight-performance-analytics-pipeline-clickhouse-1 clickhouse-client --query "SELECT year, month, count() FROM gold.fact_flight_delays GROUP BY year, month ORDER BY year, month"

# Launch Dagster UI to browse assets and runs
make dev
# Open http://localhost:3000 in browser
```

### 8. Dashboard (Streamlit)
```bash
# Install new dependency if not already synced
uv sync

# Launch one-page dashboard
streamlit run main.py
```

The dashboard includes two insight tiles sourced directly from ClickHouse gold
materialized views:
- **Top 10 carriers by delay rate** from `gold.mv_carrier_delay_performance`
- **Monthly delay trend** from `gold.mv_monthly_delay_trends`

Results are cached for 24 hours in Streamlit to reduce repeated query load.

---

## Processing Model

### Bronze
- Bronze ingestion reads the source CSV, appends `_ingested_at`, and appends rows into `bronze.airline_delay_data`.
- Runtime config comes from `run_config.yaml`.

### Silver
- `stg_airline_delay_data` is an incremental dbt model.
- Incremental filtering is based on `_ingested_at` so unchanged source data does not trigger a full rebuild.
- `make silver-run` builds the model and runs dbt tests.

### Gold
- Gold dimensions (`dim_carrier`, `dim_airport`, `dim_date`) remain full reloads because they are small.
- Gold fact is partitioned logically by month in Dagster, using partition keys like `2025-11-01`.
- ClickHouse stores `gold.fact_flight_delays` in yearly partitions.
- To avoid memory-heavy ClickHouse mutations, a single fact partition run refreshes the full target year by dropping the ClickHouse year partition and reloading that year in chunks.

### Dagster Automation
- Job: `daily_pipeline`
- Job: `backfill_pipeline`
- Schedule: `daily_pipeline_schedule`
- Schedule time: `03:00 UTC`
- The schedule derives the active monthly partition key as the first day of the current UTC month.

---

## Available Commands

```bash
# View all available Makefile targets
make help

# Data Pipeline
make ingest          # Bronze layer: run full ingestion pipeline
make silver          # Silver layer: build staging models without dbt tests
make silver-run      # Silver layer: build staging models + dbt tests
make silver-checks   # Silver layer: run only dbt tests
make gold            # Gold layer: build mart models + dbt tests
make gold-run        # Gold layer: build mart models without tests
make gold-checks     # Gold layer: run only dbt tests on marts

# Infrastructure
make db-up           # Start PostgreSQL container
make db-down         # Stop PostgreSQL container
make db-reset        # Wipe and restart PostgreSQL (loses all data)
make db-check        # Verify PostgreSQL is accepting connections

make ch-up           # Start ClickHouse container
make ch-down         # Stop ClickHouse container
make ch-reset        # Wipe and restart ClickHouse (loses all data)
make ch-check        # Show all tables in gold database
make ch-init         # Manually run ClickHouse init scripts

# Gold-Specific
make gold-load       # Load gold dims and all configured monthly fact partitions into ClickHouse
make gold-load-month MONTH=YYYY-MM-DD  # Load one monthly fact partition into ClickHouse

# Automation
make daily-run-test MONTH=YYYY-MM-DD   # Execute automated bronze→silver→gold selection for one partition
make backfill-run  # Replay configured fact partitions across the historical range

# Development
make dev             # Launch Dagster UI (http://localhost:3000)
make dbt-compile     # Recompile dbt manifest (required after model changes)
```

---

## Project Structure

```
flight-performance-analytics-pipeline/
├── src/
│   └── flight_performance_analytics_pipeline/
│       ├── assets/
│       │   ├── bronze/       # Ingestion assets
│       │   ├── silver/       # Incremental staging assets and checks
│       │   └── gold/         # ClickHouse load assets and checks
│       ├── dbt_transformations/
│       │   ├── models/
│       │   │   ├── staging/      # Incremental silver layer models (PostgreSQL)
│       │   │   └── marts/        # Gold layer dimensional, fact, and analytics models
│       │   └── profiles.yml      # dbt PostgreSQL connection config
│       ├── resources/        # Dagster resources (DB connections)
│       ├── jobs/             # Dagster job definitions
│       ├── schedules/        # Dagster schedules
│       └── definitions.py    # Main Dagster definitions registry
├── sql_scripts/
│   ├── postgres/             # SQL DDL for bronze/silver tables
│   └── clickhouse/           # SQL DDL for gold dimensional/fact tables
├── tests/                    # Unit & integration tests
├── Makefile                  # Command orchestration
├── pyproject.toml            # Python dependencies (uv)
├── docker-compose.yml        # PostgreSQL & ClickHouse services
└── .env.example              # Environment variable template
```

---

## Troubleshooting

### PostgreSQL Connection Issues
```bash
# Verify PostgreSQL container is running
docker ps | grep postgres

# Check PostgreSQL logs
docker logs flight-performance-analytics-pipeline-postgres-1

# Manually test connection
make db-check
```

### ClickHouse Connection Issues
```bash
# Verify ClickHouse container is running
docker ps | grep clickhouse

# Check ClickHouse logs
docker logs flight-performance-analytics-pipeline-clickhouse-1

# Manually initialize schemas if needed
make ch-init
```

### dbt Compilation Errors
```bash
# Recompile the dbt manifest after model changes
make dbt-compile

# This is required before running `make dev` if you modify any dbt models
```

### Full Reset (Start Over)
```bash
# Delete all data and restart containers
make db-reset && make ch-reset

# Re-run full pipeline
make ingest && make silver && make gold && make gold-load
```

### Memory Limit on ClickHouse Insert
- Gold dimension loads and fact reloads use chunked inserts (`10,000` rows per batch)
- The fact loader avoids month-level `DELETE` mutations because ClickHouse stores the fact table in yearly partitions
- If memory issues return, keep the year-partition refresh strategy and reduce chunk size or increase ClickHouse container memory in `docker-compose.yml`

### Daily Automation Notes
- `make daily-run-test` runs the same bronze, silver, and gold grouped asset selection used by automation, scoped to one monthly partition key
- The daily schedule is defined in Dagster as `daily_pipeline_schedule` and runs at `03:00 UTC`
- Dagster may print `SupersessionWarning` for `dagster asset materialize`; this is informational and does not indicate failure

---

## Data Quality Checks

The pipeline includes automated checks at each layer:

- **Silver Layer**: dbt generic tests on staging models (not_null, unique, relationships)
- **Gold Layer**:
  - dbt tests on dimensional & fact models
  - dbt tests on analytics models
  - Dagster asset checks on ClickHouse tables (row counts, null checks, referential integrity)
- **Automation Runs**:
  - Daily runs execute bronze checks, silver checks, gold checks, and fact integrity checks in one Dagster run

All checks are run automatically as part of `make silver-run`, `make gold`, `make gold-load`, and `make daily-run-test`.

---

## Scheduling

- Dagster job `daily_pipeline` orchestrates bronze, silver, and gold asset groups
- Dagster job `backfill_pipeline` exists for fact-partition replay workflows
- Dagster schedule `daily_pipeline_schedule` runs `daily_pipeline` every day at `03:00 UTC`
- The scheduled partition key is generated in `YYYY-MM-01` format


---

## Pictures
![alt text](<Pasted Graphic.png>)
![alt text](<Pasted Graphic 2.png>)
![alt text](<Pasted Graphic 3.png>)
