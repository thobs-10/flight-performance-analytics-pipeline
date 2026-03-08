import logging
import os
from pathlib import Path

logging.basicConfig(level=logging.INFO, format="[%(asctime)s]: %(message)s:")

PROJECT_ROOT = Path("src")
project_name = "src/flight-performance-analytics-pipeline"

DIRECTORIES_TO_CREATE = [
    "docs/dashboard/sample_screenshots",
]

FILES_TO_CREATE = [
    # CI/CD
    ".github/workflows/ci.yml",
    ".github/workflows/deploy.yml",
    ".github/PULL_REQUEST_TEMPLATE.md",
    # Infrastructure as Code(IaC)
    f"{project_name}/infrastructure/terraform/environments/dev/main.tf",
    f"{project_name}/infrastructure/terraform/environments/dev/variables.tf",
    f"{project_name}/infrastructure/terraform/environments/dev/terraform.tfvars",
    f"{project_name}/infrastructure/terraform/environments/prod/main.tf",
    f"{project_name}/infrastructure/terraform/environments/prod/variables.tf",
    f"{project_name}/infrastructure/terraform/environments/prod/terraform.tfvars",
    f"{project_name}/infrastructure/terraform/modules/networking/main.tf",
    f"{project_name}/infrastructure/terraform/modules/networking/variables.tf",
    f"{project_name}/infrastructure/terraform/modules/networking/outputs.tf",
    f"{project_name}/infrastructure/terraform/modules/compute/main.tf",
    f"{project_name}/infrastructure/terraform/modules/compute/variables.tf",
    f"{project_name}/infrastructure/terraform/modules/compute/outputs.tf",
    f"{project_name}/infrastructure/terraform/modules/storage/main.tf",
    f"{project_name}/infrastructure/terraform/modules/storage/variables.tf",
    f"{project_name}/infrastructure/terraform/modules/storage/outputs.tf",
    f"{project_name}/infrastructure/terraform/global/provider.tf",
    f"{project_name}/infrastructure/terraform/global/variables.tf",
    f"{project_name}/infrastructure/docker/Dockerfile.dagster",
    f"{project_name}/infrastructure/docker/Dockerfile.clickhouse",
    f"{project_name}/infrastructure/docker/docker-compose.yml",
    f"{project_name}/infrastructure/docker/docker-compose.override.yml",
    # Source Code
    f"{project_name}/__init__.py",
    f"{project_name}/assets/__init__.py",
    f"{project_name}/assets/bronze/__init__.py",
    f"{project_name}/assets/bronze/extract_from_api.py",
    f"{project_name}/assets/bronze/load_to_postgres.py",
    f"{project_name}/assets/silver/__init__.py",
    f"{project_name}/assets/silver/clean_food_prices.py",
    f"{project_name}/assets/silver/validate_data.py",
    f"{project_name}/assets/gold/__init__.py",
    f"{project_name}/assets/gold/dim_country.py",
    f"{project_name}/assets/gold/dim_food_category.py",
    f"{project_name}/assets/gold/dim_date.py",
    f"{project_name}/assets/gold/fact_food_prices.py",
    f"{project_name}/jobs/__init__.py",
    f"{project_name}/jobs/daily_pipeline.py",
    f"{project_name}/jobs/backfill_pipeline.py",
    f"{project_name}/jobs/quality_checks_job.py",
    f"{project_name}/resources/__init__.py",
    f"{project_name}/resources/postgres_io_manager.py",
    f"{project_name}/resources/clickhouse_io_manager.py",
    f"{project_name}/resources/fao_api_resource.py",
    f"{project_name}/resources/email_alert_resource.py",
    f"{project_name}/schedules/__init__.py",
    f"{project_name}/schedules/daily_schedule.py",
    # f"{project_name}/sensors/__init__.py",
    # f"{project_name}/sensors/new_data_sensor.py",
    f"{project_name}/utils/__init__.py",
    f"{project_name}/utils/logging_config.py",
    f"{project_name}/utils/data_validators.py",
    f"{project_name}/utils/date_utils.py",
    f"{project_name}/definitions.py",
    # DBT Transformations
    f"{project_name}/dbt_transformations/models/staging/schema.yml",
    f"{project_name}/dbt_transformations/models/staging/stg_food_prices.sql",
    f"{project_name}/dbt_transformations/models/marts/core/dim_country.sql",
    f"{project_name}/dbt_transformations/models/marts/core/dim_food_category.sql",
    f"{project_name}/dbt_transformations/models/marts/core/fact_prices.sql",
    f"{project_name}/dbt_transformations/models/marts/analytics/monthly_avg_prices.sql",
    f"{project_name}/dbt_transformations/models/marts/analytics/price_volatility.sql",
    f"{project_name}/dbt_transformations/models/sources.yml",
    f"{project_name}/dbt_transformations/macros/generate_schema_name.sql",
    f"{project_name}/dbt_transformations/dbt_project.yml",
    f"{project_name}/dbt_transformations/profiles.yml",
    # SQL Scripts
    "sql_scripts/postgres/01_create_raw_tables.sql",
    "sql_scripts/postgres/02_create_staging_tables.sql",
    "sql_scripts/postgres/03_create_indexes.sql",
    "sql_scripts/clickhouse/01_create_databases.sql",
    "sql_scripts/clickhouse/02_create_dim_tables.sql",
    "sql_scripts/clickhouse/03_create_fact_tables.sql",
    "sql_scripts/clickhouse/04_optimizations.sql",
    # Tests
    "tests/unit/__init__.py",
    "tests/unit/test_data_validators.py",
    "tests/unit/test_date_utils.py",
    "tests/unit/test_extract_api.py",
    "tests/integration/test_postgres_connection.py",
    "tests/integration/test_clickhouse_connection.py",
    "tests/integration/test_end_to_end.py",
    # "tests/fixtures/sample_api_response.json",
    # "tests/fixtures/test_config.yaml",
    # Documentation and Dashboards
    # "dashboard/metabase/docker-compose.yml",
    # "dashboard/metabase/config/settings.yml",
    # "dashboard/metabase/dashboards/food_prices_dashboard.json",
    # "dashboard/superset/docker-compose.yml",
    # "dashboard/superset/dashboard_export.json",
    # Setup and Utilities
    # "scripts/setup/init_postgres.sh",
    # "scripts/setup/init_clickhouse.sh",
    # "scripts/setup/seed_data.py",
    # "scripts/utils/backup_postgres.sh",
    # "scripts/utils/backup_clickhouse.sh",
    # "scripts/utils/cleanup_old_data.py",
    # "scripts/deploy.sh",
    # Logging
    f"{project_name}/logging/__init__.py",
    f"{project_name}/logging/logging.py",
    # Configuration Files
    f"{project_name}/config/logging/.gitkeep",
    # "config/logging/logging_config.yaml",
    # "config/dagster/workspace.yaml",
    # "config/alerts/alert_rules.yaml",
    # Notebooks
    "notebooks/01_data_exploration.ipynb",
    # "notebooks/02_quality_analysis.ipynb",
    # "notebooks/03_sample_queries.ipynb",
    # Documentation and Diagrams
    "docs/architecture/data_flow_diagram.png",
    "docs/architecture/medallion_architecture.md",
    "docs/architecture/fact_dimensional_model.md",
    # "docs/setup/local_development.md",
    # "docs/setup/cloud_deployment.md",
    # "docs/setup/environment_variables.md",
    # "docs/api/fao_api_documentation.md",
    "docs/dashboard/user_guide.md",
    ".env.example",
    # ".gitignore",
    ".pre-commit-config.yaml",
    # "Makefile",
    # "dev-requirements.txt",
    # "docker-compose.yml",
    # "README.md",
    # "LICENSE",
]


def create_project_structure() -> None:
    for filepath in FILES_TO_CREATE:
        file_path = Path(filepath)
        filedir, filename = os.path.split(file_path)

        if filedir != "":
            os.makedirs(filedir, exist_ok=True)
            logging.info(f"Created directory: {filedir} for filename: {filename}")
        if (not os.path.exists(filename)) or (os.path.getsize(filename) == 0):
            with open(filepath, "w", encoding="utf-8"):
                logging.info(f"Created file: {filename}")
        else:
            logging.info(f"File {filename} already exists.")


def main() -> None:
    logging.info("Starting project structure creation...")
    create_project_structure()
    logging.info("Project structure creation completed.")


if __name__ == "__main__":
    main()
