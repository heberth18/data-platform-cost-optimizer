{{ config(materialized='view') }}

WITH customer_base AS (
    SELECT * FROM {{ ref('stg_customer_profiles') }}
),

order_aggregates AS (
    SELECT
        customer_id,
        COUNT(DISTINCT cart_id) as total_orders,
        SUM(line_total) as total_spent,
        AVG(line_total) as avg_order_value,
        COUNT(DISTINCT product_id) as total_products,
        SUM(quantity) as total_quantity,
        COUNT(DISTINCT product_category) as unique_categories
    FROM {{ ref('stg_orders') }}
    GROUP BY customer_id
),

customer_metrics AS (
    SELECT
        c.customer_id,
        c.full_name,
        c.email,
        c.age,
        c.country,
        
        -- Transaction metrics
        COALESCE(o.total_orders, 0) as total_orders,
        COALESCE(o.total_spent, 0) as total_spent,
        COALESCE(o.avg_order_value, 0) as avg_order_value,
        COALESCE(o.total_products, 0) as total_products,
        COALESCE(o.total_quantity, 0) as total_quantity,
        COALESCE(o.unique_categories, 0) as unique_categories,
        
        -- Calculated metrics
        CASE
            WHEN COALESCE(o.total_products, 0) > 0
            THEN COALESCE(o.unique_categories, 0)::FLOAT / NULLIF(o.total_products, 0)
            ELSE 0
        END as product_diversity_score,
        
        -- Customer segmentation
        CASE
            WHEN COALESCE(o.total_spent, 0) >= 2000 AND COALESCE(o.total_orders, 0) >= 5 THEN 'premium'
            WHEN COALESCE(o.total_spent, 0) >= 500 AND COALESCE(o.total_orders, 0) >= 2 THEN 'regular'
            ELSE 'new'
        END as customer_segment,
        
        -- Profile completeness
        (
            CASE WHEN c.full_name IS NOT NULL AND c.full_name != '' THEN 1 ELSE 0 END +
            CASE WHEN c.email IS NOT NULL AND c.email != '' THEN 1 ELSE 0 END +
            CASE WHEN c.phone IS NOT NULL AND c.phone != '' THEN 1 ELSE 0 END +
            CASE WHEN c.city IS NOT NULL AND c.city != '' THEN 1 ELSE 0 END +
            CASE WHEN c.country IS NOT NULL AND c.country != '' THEN 1 ELSE 0 END
        )::FLOAT / 5 as profile_completeness,
        
        -- Activity score (simplified)
        LEAST(100, 
            COALESCE(o.total_orders, 0) * 4 + 
            COALESCE(o.total_spent, 0) / 100
        ) as customer_activity_score
        
    FROM customer_base c
    LEFT JOIN order_aggregates o ON c.customer_id = o.customer_id
)

SELECT * FROM customer_metrics