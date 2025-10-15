SELECT
    customer_id,
    full_name,
    total_spent
FROM {{ ref('customer_segmentation') }}
WHERE total_spent < 0