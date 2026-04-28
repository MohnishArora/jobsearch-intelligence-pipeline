import dlt
from dagster import AssetExecutionContext
from dagster_dlt import DagsterDltResource, dlt_assets
from serpapi import GoogleSearch
import os
from datetime import datetime, timedelta
from dotenv import load_dotenv

# 1. Load your credentials from the .env file
load_dotenv()

@dlt.source
def serpapi_jobs_source(api_key=None):
    """
    A dlt source that fetches jobs from SerpApi for specific categories,
    limited to the last 21 weeks.
    """
    
    # Calculate the date 21 weeks ago from today
    # This keeps your ingestion lean and saves SerpApi credits
    cut_off_date = (datetime.now() - timedelta(weeks=21)).strftime('%Y-%m-%d')
    
    # Define your search categories
    queries = [
        "Data Engineer",
        "Analytics Engineer",
        "Data Operations and Business Analyst"
    ]

    @dlt.resource(
        name="raw_jobs",
        primary_key="job_id",
        write_disposition="merge",
    )
    def fetch_jobs():
        for category_query in queries:
            # Append the 'after' operator to target only recent listings
            search_query = f"{category_query} after:{cut_off_date}"
            
            params = {
                "engine": "google_jobs",
                "q": search_query,
                "location": "United States",
                "hl": "en",
                "gl": "us",
                "api_key": api_key or os.getenv("SERPAPI_KEY")
            }
            
            search = GoogleSearch(params)
            results = search.get_dict()
            
            jobs = results.get("jobs_results", [])
            
            for job in jobs:
                # We save the original category name (without the date) 
                # so your downstream dbt logic stays clean
                job["search_query_category"] = category_query
                yield job

    return fetch_jobs()

# 2. Define the Dagster Asset
@dlt_assets(
    dlt_source=serpapi_jobs_source(),
    dlt_pipeline=dlt.pipeline(
        pipeline_name="usa_job_ingestion",
        destination="snowflake",
        dataset_name="BRONZE",
    ),
    name="serpapi_jobs_asset",
    group_name="ingestion"
)
def serpapi_jobs_asset(context: AssetExecutionContext, dlt_resource: DagsterDltResource):
    """
    Materializes the SerpApi job data into Snowflake with a 21-week recency filter.
    """
    yield from dlt_resource.run(context=context)