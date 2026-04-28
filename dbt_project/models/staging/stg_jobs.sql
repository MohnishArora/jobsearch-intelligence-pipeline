{{ config(materialized='view') }}

SELECT
    job_id AS job_key,
    title AS job_title,
    company_name,
    location AS job_location,
    description,
    search_query_category AS search_category,
    detected_extensions__posted_at AS posted_at_relative,
    
    -- 1. Identify Seniority (Standard groups are safe)
    REGEXP_LIKE(job_title, '.*(Senior|Sr\\.|Lead|Principal|Staff|Head|Manager|Director|VP).*', 'i') as is_senior_title,

    -- 2. Extract Experience (Snowflake Safe Version)
    -- This looks for a number followed by a separator or 'year/yr'
    TRY_TO_NUMBER(
        REGEXP_SUBSTR(description, '([0-9]+)[[:space:]]*(-|to|year|yr)', 1, 1, 'e', 1)
    ) as min_experience_years,

    -- 3. Citizen Only Flag
    CASE 
        WHEN LOWER(description) LIKE '%us citizen only%' 
          OR LOWER(description) LIKE '%u.s. citizen only%' 
          OR LOWER(description) LIKE '%security clearance%'
          OR LOWER(description) LIKE '%must be a us citizen%'
        THEN TRUE ELSE FALSE 
    END AS is_citizen_only,

    -- 4. Sponsorship Potential
    CASE 
        WHEN LOWER(description) LIKE '%no sponsorship%' 
          OR LOWER(description) LIKE '%does not sponsor%' 
          OR LOWER(description) LIKE '%not eligible for sponsorship%'
          OR LOWER(description) LIKE '%unable to sponsor%'
          OR LOWER(description) LIKE '%security clearance%'
        THEN FALSE 
        ELSE TRUE 
    END AS is_potential_lead,

    CURRENT_TIMESTAMP() AS transformed_at
FROM {{ source('serpapi', 'raw_jobs') }}