{{ config(materialized='view') }}

WITH source AS (
    SELECT * FROM {{ source('staging', 'raw_orders') }}
),

cleaned AS (
    SELECT
        order_id,
        cart_id,
        customer_id,
        product_id,
        
        -- Product name cleaning
        TRIM(product_name) as product_name,
        LOWER(TRIM(product_category)) as product_category,
        
        -- Numeric validations
        quantity,
        price,
        discount_percentage,
        line_total,
        
        -- Recalculate line total for consistency
        ROUND(price * quantity * (1 - discount_percentage/100), 2) as calculated_line_total,
        
        -- Metadata
        created_at
        
    FROM source
    WHERE customer_id IS NOT NULL
    AND quantity > 0
    AND price > 0
)

SELECT * FROM cleaned