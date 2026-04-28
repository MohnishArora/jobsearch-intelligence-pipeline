import os
from pathlib import Path
from dagster import Definitions, ScheduleDefinition, AssetSelection, define_asset_job
from dagster_dlt import DagsterDltResource
from dagster_dbt import DbtCliResource, dbt_assets
from assets.ingest import serpapi_jobs_asset

# 1. Locate your dbt project
# This looks one folder up from 'orchestration' to find 'dbt_project'
DBT_PROJECT_DIR = Path(__file__).joinpath("..", "..", "dbt_project").resolve()

# 2. Load dbt models as Dagster Assets
# Dagster reads the 'manifest.json' that dbt created when you ran 'dbt run'
@dbt_assets(manifest=DBT_PROJECT_DIR.joinpath("target", "manifest.json"))
def job_pipeline_dbt_assets(context, dbt: DbtCliResource):
    yield from dbt.cli(["build"], context=context).stream()

# 3. Define the Automation Schedule
# AssetSelection.all() now includes BOTH your dlt ingestion and your dbt models
job_search_schedule = ScheduleDefinition(
    name="daily_job_search_schedule",
    target=AssetSelection.all(),
    cron_schedule="0 7,13,19 * * *",
    execution_timezone="America/Los_Angeles",
)

# 4. The Master Definition
defs = Definitions(
    assets=[serpapi_jobs_asset, job_pipeline_dbt_assets],
    resources={
        "dlt_resource": DagsterDltResource(),
        "dbt": DbtCliResource(project_dir=os.fspath(DBT_PROJECT_DIR)),
    },
    schedules=[job_search_schedule],
)