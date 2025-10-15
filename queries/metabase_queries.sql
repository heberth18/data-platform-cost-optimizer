-- ============================================
-- DASHBOARD 1: CUSTOMER OVERVIEW
-- ============================================

-- Question 1: Total Customers by Segment
SELECT 
customer_segment,
COUNT(*) as customers
FROM analytics.customer_segmentation
GROUP BY customer_segment
ORDER BY customers DESC;

-- Question 2: Revenue by Customer Tier
SELECT 
value_tier,
SUM(total_spent) as total_revenue,
COUNT(*) as customers
FROM analytics.customer_segmentation
GROUP BY value_tier
ORDER BY total_revenue DESC;

-- Question 3: Top 10 Customers
SELECT 
full_name as "Customer",
customer_segment as "Segment",
total_spent as "Total Spent",
total_orders as "Orders"
FROM analytics.customer_segmentation
ORDER BY total_spent DESC
LIMIT 10;

-- Question 4: Customer Activity Distribution
SELECT 
CASE 
    WHEN customer_activity_score >= 75 THEN 'Very Active'
    WHEN customer_activity_score >= 50 THEN 'Active'
    WHEN customer_activity_score >= 25 THEN 'Moderate'
    ELSE 'Low Activity'
END as activity_level,
COUNT(*) as customers
FROM analytics.customer_segmentation
GROUP BY activity_level;


-- ============================================
-- DASHBOARD 2: FRAUD ANALYTICS
-- ============================================

-- Question 1: Risk Level Distribution
SELECT 
risk_level as "Risk Level",
COUNT(*) as "Customers",
ROUND(AVG(composite_risk_score), 3) as "Avg Risk Score"
FROM staging.fraud_scores
GROUP BY risk_level
ORDER BY 
CASE 
    WHEN risk_level = 'critical' THEN 1
    WHEN risk_level = 'high' THEN 2
    WHEN risk_level = 'medium' THEN 3
    ELSE 4
END;

-- Question 2: Top Risk Customers
SELECT 
f.full_name as "Customer",
f.risk_level as "Risk Level",
ROUND(f.composite_risk_score, 3) as "Risk Score",
f.action_required as "Action Required",
c.total_spent as "Total Spent"
FROM analytics.fraud_analytics f
JOIN analytics.customer_segmentation c ON f.customer_id = c.customer_id
ORDER BY f.composite_risk_score DESC
LIMIT 20;

-- Question 3: Risk Factors Breakdown
SELECT 
'Velocity' as "Risk Factor",
ROUND(AVG(velocity_risk), 3) as "Average Score"
FROM staging.fraud_scores
UNION ALL
SELECT 'Geographic', ROUND(AVG(geographic_risk), 3) FROM staging.fraud_scores
UNION ALL
SELECT 'Behavioral', ROUND(AVG(behavioral_risk), 3) FROM staging.fraud_scores
UNION ALL
SELECT 'Profile', ROUND(AVG(profile_risk), 3) FROM staging.fraud_scores
UNION ALL
SELECT 'Amount', ROUND(AVG(amount_risk), 3) FROM staging.fraud_scores
UNION ALL
SELECT 'Temporal', ROUND(AVG(temporal_risk), 3) FROM staging.fraud_scores
ORDER BY "Average Score" DESC;

-- Question 4: Fraud Metrics Summary
SELECT 
COUNT(*) as "Total Analyzed",
COUNT(CASE WHEN risk_level IN ('high', 'critical') THEN 1 END) as "High Risk",
  ROUND(100.0 * COUNT(CASE WHEN risk_level IN ('high', 'critical') THEN 1 END) / COUNT(*), 1) as "High Risk %",
ROUND(AVG(composite_risk_score), 3) as "Avg Risk Score"
FROM staging.fraud_scores;


-- ============================================
-- DASHBOARD 3: BUSINESS INTELLIGENCE
-- ============================================

-- Question 1: VIP Customer Analysis
SELECT 
customer_tier as "Customer Tier",
retention_status as "Retention Status",
COUNT(*) as "Customers",
ROUND(SUM(total_spent), 2) as "Revenue"
FROM analytics.high_value_customers
GROUP BY customer_tier, retention_status
ORDER BY "Revenue" DESC;

-- Question 2: Revenue at Risk
SELECT 
h.retention_status as "Retention Status",
COUNT(*) as "Customers",
ROUND(SUM(h.total_spent), 2) as "Revenue at Risk"
FROM analytics.high_value_customers h
WHERE h.retention_status != 'Stable'
GROUP BY h.retention_status
ORDER BY "Revenue at Risk" DESC;

-- Question 3: Segment Performance Matrix
SELECT 
customer_segment as "Segment",
value_tier as "Value Tier",
COUNT(*) as "Customers",
ROUND(SUM(total_spent), 2) as "Revenue",
ROUND(AVG(customer_activity_score), 1) as "Avg Activity"
FROM analytics.customer_segmentation
GROUP BY customer_segment, value_tier
ORDER BY "Revenue" DESC;

-- Question 4: Customer Lifecycle
SELECT 
CASE 
    WHEN total_orders = 1 THEN '1 - New'
    WHEN total_orders BETWEEN 2 AND 3 THEN '2-3 - Growing'
    WHEN total_orders BETWEEN 4 AND 9 THEN '4-9 - Active'
    ELSE '10+ - Loyal'
END as "Lifecycle Stage",
COUNT(*) as "Customers",
ROUND(SUM(total_spent), 2) as "Revenue"
FROM analytics.customer_segmentation
GROUP BY "Lifecycle Stage"
ORDER BY 
CASE 
    WHEN "Lifecycle Stage" LIKE '1%' THEN 1
    WHEN "Lifecycle Stage" LIKE '2%' THEN 2
    WHEN "Lifecycle Stage" LIKE '4%' THEN 3
    ELSE 4
END;