{{ config(materialized='table') }}

WITH customer_metrics AS (
    SELECT * FROM {{ ref('stg_customer_metrics') }}
),

fraud_data AS (
    SELECT 
        customer_id,
        risk_level
    FROM {{ source('staging', 'fraud_scores') }}
),

high_value AS (
    SELECT
        c.customer_id,
        c.full_name,
        c.email,
        c.country,
        c.total_orders,
        c.total_spent,
        c.avg_order_value,
        c.customer_segment,
        
        -- Risk flag
        COALESCE(f.risk_level, 'low') as risk_level,
        
        -- High value criteria
        CASE 
            WHEN c.total_spent >= 10000 THEN 'Tier 1 - VIP'
            WHEN c.total_spent >= 5000 THEN 'Tier 2 - Premium'
            WHEN c.total_spent >= 2000 THEN 'Tier 3 - Gold'
            ELSE 'Standard'
        END as customer_tier,
        
        -- Retention risk
        CASE
            WHEN COALESCE(f.risk_level, 'low') IN ('high', 'critical') THEN 'High Fraud Risk'
            WHEN c.total_orders = 1 THEN 'New Customer - Retention Risk'
            WHEN c.total_orders < 3 THEN 'Low Engagement'
            ELSE 'Stable'
        END as retention_status
        
    FROM customer_metrics c
    LEFT JOIN fraud_data f ON c.customer_id = f.customer_id
    WHERE c.total_spent >= 2000  -- Only high value customers
)

SELECT * FROM high_value
ORDER BY total_spent DESC