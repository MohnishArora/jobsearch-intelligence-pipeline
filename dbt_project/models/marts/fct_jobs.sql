-- models/marts/fct_jobs.sql

{{ config(materialized='table') }}

WITH base_jobs AS (
    SELECT * FROM {{ ref('stg_jobs') }}
    WHERE is_citizen_only = FALSE 
    AND is_potential_lead = TRUE
    AND is_senior_title = FALSE 
)

SELECT 
    job_title,
    company_name,
    job_location,
    search_category,
    min_experience_years,
    posted_at_relative,
    transformed_at as discovery_time,
    'https://www.google.com/search?q=' || REPLACE(job_title || ' ' || company_name, ' ', '+') as apply_link
FROM base_jobs
WHERE (
    (search_category IN ('Data Engineer', 'Analytics Engineer') 
     AND (min_experience_years BETWEEN 1 AND 3 OR min_experience_years IS NULL))
    OR
    (search_category = 'Data Operations and Business Analyst' 
     AND (min_experience_years <= 6 OR min_experience_years IS NULL))
)
QUALIFY ROW_NUMBER() OVER (PARTITION BY job_key ORDER BY discovery_time DESC) = 1