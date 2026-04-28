# Job Intelligence Pipeline
## Automated Job Discovery and Filtering for OPT and Visa-Friendly Data Engineering Roles

**Tools:** SerpAPI | dlt | Dagster | dbt | Snowflake | Medallion Architecture | Python

**Dataset:** Live job postings via Google Jobs (SerpAPI) | Refreshed daily | Filtered for sponsorship-friendly roles

**Result:** End-to-end production-grade pipeline that automatically discovers, ingests, transforms, and filters job postings into a daily curated strike list of high-intent leads

---

## The Origin

I built this project to solve my own problem.

As an international student on OPT looking for Data Engineering roles, I was spending hours every day manually searching job boards, reading through descriptions to check for sponsorship language, and filtering out senior roles I was not qualified for. Most job search tools do not let you filter by visa sponsorship intent. And no tool was smart enough to automatically exclude roles that said "no sponsorship" buried three paragraphs deep in the description.

So I built one.

At the same time, I had been wanting to work with a modern data engineering stack — Dagster for orchestration, dlt for ingestion, dbt for transformation, Snowflake as the warehouse — but had not found a project that motivated me enough to learn all of them at once. This was that project.

The result is a fully automated pipeline that runs daily, pulls live job postings from Google Jobs, filters them through three layers of intelligence, and delivers a clean curated list of roles worth applying to.

---

## The Problem

Manual job searching at scale is broken for international candidates. The specific gaps this pipeline addresses:

1. No way to filter by sponsorship intent across job boards — you have to read every description
2. No automatic exclusion of senior roles when you are early career
3. No deduplication across sources or search queries
4. No structured experience level filtering by role type

---

## System Architecture

The pipeline follows Medallion Architecture — data quality increases as it moves through layers:

```
SerpAPI (Google Jobs)
        |
        v
  BRONZE LAYER          Raw JSON job data, no transformation
  (Snowflake)           Ingested via dlt, orchestrated by Dagster
        |
        v
  SILVER LAYER          Cleaned, enriched, intelligence applied
  (stg_jobs view)       Seniority flags, sponsorship flags, experience extraction
        |
        v
  GOLD LAYER            Business-ready curated strike list
  (fct_jobs table)      Deduplicated, filtered, ranked by discovery time
```

---

## The Pipeline

**Stage 1: Ingestion via dlt and SerpAPI (Bronze)**

Live job postings pulled from Google Jobs using SerpAPI across multiple search queries covering Data Engineer, Analytics Engineer, and Business Analyst roles. The ingestion script dynamically calculates a date 21 weeks ago and appends `after:YYYY-MM-DD` to every query — this prevents pulling stale listings and conserves API credits. dlt handles schema inference, type casting, and loading directly into Snowflake Bronze layer.

**Stage 2: Staging transformation via dbt (Silver)**

Raw Bronze data transformed into a clean staging view (`stg_jobs`) with the following intelligence applied:

Seniority detection: REGEXP_LIKE flags titles containing Senior, Sr., Lead, Principal, Staff, Head, Manager, Director, or VP.

Experience extraction: Snowflake-safe POSIX regex extracts the minimum years of experience from job descriptions. Python-style non-capturing groups do not work in Snowflake — the final regex `([0-9]+)[[:space:]]*(-|to|year|yr)` is POSIX ERE compliant.

Sponsorship detection: Two flags derived from description text. `is_citizen_only` flags descriptions containing "US citizen only" or "security clearance." `is_potential_lead` flags descriptions containing "no sponsorship" or "does not sponsor" as FALSE.

**Stage 3: Gold mart via dbt (Gold)**

Filtered, deduplicated, business-ready fact table (`fct_jobs`) applying tiered experience thresholds by role type:

| Role Category | Experience Filter |
|---|---|
| Data Engineer, Analytics Engineer | 1 to 3 years or not specified |
| Business Analyst, Data Operations | Up to 6 years or not specified |

Deduplication applied using `QUALIFY ROW_NUMBER() OVER (PARTITION BY job_key ORDER BY discovery_time DESC) = 1` to eliminate duplicate postings across search queries.

**Stage 4: Orchestration via Dagster**

Dagster schedules and monitors both the dlt ingestion asset and the dbt transformation run. Assets defined in `orchestration/assets/` with absolute imports to avoid relative import errors when running via Dagster's `-f` flag. The `.stream()` method used for dbt asset execution for real-time logging compatibility.

---

## Key Engineering Decisions and Lessons

**Filter at the source, not the destination.** Adding the `after:` date filter at the SerpAPI query level rather than filtering in dbt saved API credits and Snowflake storage. Ingesting only what you need is always cheaper than ingesting everything and filtering later.

**dbt custom schema macros must live in macros/, not models/.** The `generate_schema_name.sql` macro was initially placed in the wrong folder, causing dbt to create ghost schemas like `SILVER_GOLD` and `SILVER_SILVER`. Moving it to `macros/` gave clean, separate SILVER and GOLD schemas.

**Snowflake uses POSIX ERE, not Python regex.** The experience extraction regex had to be rewritten from Python-style `(?:...)` groups to POSIX ERE `([0-9]+)[[:space:]]*(-|to|year|yr)`. This is a non-obvious but critical difference for anyone building regex logic in Snowflake.

**Dagster API changes break silently.** The `get_artifacts()` method was deprecated in a dagster-dbt update. Switching to `.stream()` fixed real-time logging and prevented silent failures.

---

## Project Structure

```
job-intelligence-pipeline/
│
├── orchestration/
│   ├── assets/
│   │   ├── __init__.py
│   │   ├── ingest.py          # dlt ingestion asset: SerpAPI to Snowflake Bronze
│   │   └── transform.py       # dbt transformation asset: Silver and Gold layers
│   ├── constants.py           # Shared constants: search queries, Snowflake config
│   └── definations.py         # Dagster job and schedule definitions
│
├── dbt_project/
│   ├── macros/
│   │   └── generate_schema_name.sql   # Custom schema macro (must be here, not models/)
│   ├── models/
│   │   ├── staging/
│   │   │   └── stg_jobs.sql           # Silver layer: cleaning + intelligence flags
│   │   ├── marts/
│   │   │   └── fct_jobs.sql           # Gold layer: filtered strike list
│   │   └── sources.yml                # Source definition for Bronze raw_jobs table
│   └── dbt_project.yml
│
├── requirements.txt           # Full dependency list with annotated summary at bottom
├── Documentation.txt          # Technical decisions, errors encountered, and resolutions
├── .gitignore
└── README.md
```

---

## How to Run

**Prerequisites:**
- Snowflake account with BRONZE, SILVER, and GOLD schemas created
- SerpAPI account and API key
- Python 3.10+

**Setup:**

```bash
# Clone the repo
git clone https://github.com/MohnishArora/job-intelligence-pipeline.git
cd job-intelligence-pipeline

# Create virtual environment
python -m venv .venv
source .venv/bin/activate  # On Windows: .venv\Scripts\activate

# Install dependencies
pip install -r requirements.txt

# Set environment variables
cp .env.example .env
# Edit .env with your SerpAPI key and Snowflake credentials
```

**Running the pipeline:**

```bash
# Start Dagster webserver
dagster dev -f orchestration/definations.py

# Open http://localhost:3000
# Click "Materialize All" to run the full pipeline
```

**Viewing results:**

```sql
-- Run in Snowflake
SELECT * FROM GOLD.FCT_JOBS
ORDER BY discovery_time DESC;
```

**Updating filters:**

Edit `dbt_project/models/staging/stg_jobs.sql` or `dbt_project/models/marts/fct_jobs.sql` and run `dbt run` in the terminal. No API credits consumed.

---

## Environment Variables

Create a `.env` file at the project root with the following:

```
SERPAPI_KEY=your_serpapi_key_here
SNOWFLAKE_ACCOUNT=your_account
SNOWFLAKE_USER=your_username
SNOWFLAKE_PASSWORD=your_password
SNOWFLAKE_DATABASE=your_database
SNOWFLAKE_WAREHOUSE=your_warehouse
SNOWFLAKE_ROLE=your_role
```

---

## Limitations and Future Improvements

The sponsorship detection relies on keyword matching in job descriptions and may miss ambiguous phrasing. The experience extraction regex captures the first numeric range found in the description, which may not always reflect the required years. SerpAPI credits limit the number of search queries per run. Future improvements could include an LLM-based description classifier for more accurate sponsorship and seniority detection, a Streamlit or Power BI dashboard on top of the Gold layer, and alert notifications when new high-match roles appear.

---

## Author

**Mohnish Arora** | Built end-to-end to solve a real job search problem while learning a production DE stack

University of Arizona, M.S. Management Information Systems | 2025

mohnisharora22@gmail.com | github.com/MohnishArora | linkedin.com/in/aroramohnish