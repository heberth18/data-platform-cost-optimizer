
-- 1. Top 10 high-risk customers requiring immediate action
SELECT 
    full_name,
    email,
    risk_level,
    composite_risk_score,
    action_required,
    total_spent
FROM analytics.fraud_analytics
WHERE action_required IN ('BLOCK_IMMEDIATELY', 'MANUAL_REVIEW')
ORDER BY investigation_priority, composite_risk_score DESC
LIMIT 10;

-- 2. Revenue breakdown by customer segment
SELECT 
    customer_segment,
    value_tier,
    COUNT(*) as customers,
    SUM(total_spent) as total_revenue,
    AVG(total_spent) as avg_revenue_per_customer
FROM analytics.customer_segmentation
GROUP BY customer_segment, value_tier
ORDER BY total_revenue DESC;

-- 3. Customer retention risk analysis
SELECT 
    retention_status,
    customer_tier,
    COUNT(*) as customers,
    SUM(total_spent) as at_risk_revenue
FROM analytics.high_value_customers
GROUP BY retention_status, customer_tier
ORDER BY at_risk_revenue DESC;