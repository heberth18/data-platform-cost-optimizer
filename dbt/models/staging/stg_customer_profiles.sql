{{ config(materialized='view') }}

WITH source AS (
    SELECT * FROM {{ source('staging', 'raw_customers') }}
),

cleaned AS (
    SELECT
        customer_id,
        
        -- Name cleaning: trim, proper case, combine
        TRIM(INITCAP(first_name)) || ' ' || TRIM(INITCAP(last_name)) as full_name,
        TRIM(INITCAP(first_name)) as first_name,
        TRIM(INITCAP(last_name)) as last_name,
        
        -- Email: lowercase, trim, validate format
        LOWER(TRIM(email)) as email,
        
        -- Phone: remove all non-digits except +
        REGEXP_REPLACE(phone, '[^0-9+]', '', 'g') as phone,
        
        -- Date parsing: handle various formats
        CASE
            WHEN birth_date ~ '^\d{4}-\d{2}-\d{2}$' THEN birth_date::DATE
            ELSE NULL
        END as birth_date,
        
        -- Type casting
        age::INTEGER as age,
        LOWER(TRIM(gender)) as gender,
        
        -- Address cleaning
        INITCAP(TRIM(city)) as city,
        INITCAP(TRIM(state)) as state,
        UPPER(TRIM(country)) as country,
        TRIM(postal_code) as postal_code,
        TRIM(full_address) as full_address,
        
        -- Professional info
        TRIM(company_name) as company_name,
        TRIM(job_title) as job_title,
        TRIM(department) as department,
        TRIM(university) as university,
        
        -- Banking
        LOWER(TRIM(card_type)) as card_type,
        card_last_4,
        UPPER(iban_country) as iban_country,
        
        -- Metadata
        created_at,
        updated_at
        
    FROM source
    WHERE customer_id IS NOT NULL
    AND email IS NOT NULL
    AND email LIKE '%@%'  -- Basic email validation
)

SELECT * FROM cleaned