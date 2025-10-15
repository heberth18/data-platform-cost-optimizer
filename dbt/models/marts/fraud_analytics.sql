{{ config(materialized='table') }}

WITH customer_base AS (
    SELECT * FROM {{ ref('stg_customer_metrics') }}
),

fraud_scores AS (
    SELECT * FROM {{ source('staging', 'fraud_scores') }}
),

fraud_summary AS (
    SELECT
        c.customer_id,
        c.full_name,
        c.email,
        c.customer_segment,
        c.total_spent,
        c.total_orders,
        
        -- Fraud scores
        f.composite_risk_score,
        f.risk_level,
        f.velocity_risk,
        f.geographic_risk,
        f.behavioral_risk,
        f.profile_risk,
        f.amount_risk,
        f.temporal_risk,
        
        -- Action recommendations
        CASE
            WHEN f.risk_level = 'critical' THEN 'BLOCK_IMMEDIATELY'
            WHEN f.risk_level = 'high' THEN 'MANUAL_REVIEW'
            WHEN f.risk_level = 'medium' THEN 'ENHANCED_MONITORING'
            ELSE 'STANDARD'
        END as action_required,
        
        -- Investigation priority
        CASE
            WHEN f.risk_level = 'critical' AND c.total_spent > 5000 THEN 1
            WHEN f.risk_level = 'critical' THEN 2
            WHEN f.risk_level = 'high' AND c.total_spent > 2000 THEN 3
            WHEN f.risk_level = 'high' THEN 4
            ELSE 5
        END as investigation_priority,
        
        -- Risk category
        CASE
            WHEN f.velocity_risk > 0.7 THEN 'Velocity'
            WHEN f.geographic_risk > 0.7 THEN 'Geographic'
            WHEN f.behavioral_risk > 0.7 THEN 'Behavioral'
            WHEN f.amount_risk > 0.7 THEN 'Amount'
            ELSE 'Multiple Factors'
        END as primary_risk_factor,
        
        f.analyzed_at
        
    FROM customer_base c
    INNER JOIN fraud_scores f ON c.customer_id = f.customer_id
)

SELECT * FROM fraud_summary
ORDER BY investigation_priority, composite_risk_score DESC