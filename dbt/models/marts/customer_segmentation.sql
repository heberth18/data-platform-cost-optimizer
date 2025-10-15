{{ config(materialized='table') }}

WITH customer_base AS (
    SELECT * FROM {{ ref('stg_customer_metrics') }}
),

fraud_data AS (
    SELECT 
        customer_id,
        risk_level,
        composite_risk_score
    FROM {{ source('staging', 'fraud_scores') }}
),

final AS (
    SELECT
        c.customer_id,
        c.full_name,
        c.email,
        c.age,
        c.country,
        
        -- Transaction metrics
        c.total_orders,
        c.total_spent,
        c.avg_order_value,
        c.product_diversity_score,
        
        -- Segmentation
        c.customer_segment,
        c.customer_activity_score,
        c.profile_completeness,
        
        -- Fraud risk
        COALESCE(f.risk_level, 'low') as risk_level,
        COALESCE(f.composite_risk_score, 0) as fraud_risk_score,
        
        -- Customer value tier
        CASE
            WHEN c.total_spent >= 10000 THEN 'VIP'
            WHEN c.total_spent >= 5000 THEN 'High Value'
            WHEN c.total_spent >= 1000 THEN 'Medium Value'
            ELSE 'Standard'
        END as value_tier,
        
        -- RFM-like scoring
        NTILE(5) OVER (ORDER BY c.total_orders DESC) as frequency_score,
        NTILE(5) OVER (ORDER BY c.total_spent DESC) as monetary_score,
        
        -- Flags
        CASE WHEN c.total_spent > 5000 THEN 1 ELSE 0 END as is_high_value,
        CASE WHEN c.total_orders >= 10 THEN 1 ELSE 0 END as is_frequent_buyer,
        CASE WHEN COALESCE(f.risk_level, 'low') IN ('high', 'critical') THEN 1 ELSE 0 END as is_high_risk
        
    FROM customer_base c
    LEFT JOIN fraud_data f ON c.customer_id = f.customer_id
)

SELECT * FROM final
ORDER BY total_spent DESC