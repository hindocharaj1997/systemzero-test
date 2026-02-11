CREATE OR REPLACE TABLE customer_features AS
WITH transaction_stats AS (
    SELECT 
        customer_id,
        COUNT(*) as total_orders,
        COUNT(*) FILTER (WHERE COALESCE(total_amount, 0) < 0) as return_count,
        SUM(total_amount) as total_revenue,
        SUM(CASE WHEN COALESCE(total_amount, 0) >= 0 THEN total_amount ELSE 0 END) as gross_revenue,
        SUM(CASE WHEN COALESCE(total_amount, 0) < 0 THEN total_amount ELSE 0 END) as return_amount,
        AVG(total_amount) as avg_order_value,
        MIN(transaction_date) as first_purchase,
        MAX(transaction_date) as last_purchase
    FROM transactions
    GROUP BY customer_id
),
-- RFM scoring
rfm_raw AS (
    SELECT 
        c.customer_id,
        c.full_name,
        c.segment,
        COALESCE(c.total_spend, 0) as total_spend,
        COALESCE(t.total_orders, 0) as total_orders,
        COALESCE(t.return_count, 0) as return_count,
        COALESCE(t.total_revenue, 0) as total_revenue,
        COALESCE(t.gross_revenue, 0) as gross_revenue,
        COALESCE(t.return_amount, 0) as return_amount,
        COALESCE(t.avg_order_value, 0) as avg_order_value,
        t.first_purchase,
        t.last_purchase,
        DATEDIFF('day', TRY_CAST(t.last_purchase AS DATE), CURRENT_DATE) as days_since_last_purchase,
        DATEDIFF('month', TRY_CAST(c.registration_date AS DATE), CURRENT_DATE) as customer_tenure_months,
        CASE 
            WHEN DATEDIFF('month', TRY_CAST(c.registration_date AS DATE), CURRENT_DATE) > 0 
            THEN t.total_orders::FLOAT / DATEDIFF('month', TRY_CAST(c.registration_date AS DATE), CURRENT_DATE)
            ELSE COALESCE(t.total_orders, 0)
        END as purchase_frequency
    FROM customers c
    LEFT JOIN transaction_stats t ON c.customer_id = t.customer_id
),
rfm_scored AS (
    SELECT *,
        -- Recency score (1-5, lower days = higher score)
        CASE 
            WHEN days_since_last_purchase IS NULL THEN 1
            WHEN days_since_last_purchase <= 30 THEN 5
            WHEN days_since_last_purchase <= 90 THEN 4
            WHEN days_since_last_purchase <= 180 THEN 3
            WHEN days_since_last_purchase <= 365 THEN 2
            ELSE 1
        END as recency_score,
        -- Frequency score (1-5)
        CASE 
            WHEN total_orders = 0 THEN 1
            WHEN total_orders <= 2 THEN 2
            WHEN total_orders <= 5 THEN 3
            WHEN total_orders <= 10 THEN 4
            ELSE 5
        END as frequency_score,
        -- Monetary score (1-5)
        CASE 
            WHEN total_revenue = 0 THEN 1
            WHEN total_revenue <= 100 THEN 2
            WHEN total_revenue <= 500 THEN 3
            WHEN total_revenue <= 1000 THEN 4
            ELSE 5
        END as monetary_score
    FROM rfm_raw
)
SELECT 
    customer_id,
    full_name,
    segment,
    total_spend,
    total_orders,
    return_count,
    total_revenue,
    gross_revenue,
    return_amount,
    avg_order_value AS average_order_value,
    first_purchase,
    last_purchase,
    days_since_last_purchase,
    customer_tenure_months,
    purchase_frequency,
    -- CLV: total_spend adjusted for tenure
    CASE 
        WHEN customer_tenure_months > 0 
        THEN (total_revenue / customer_tenure_months) * 12
        ELSE total_revenue
    END as customer_lifetime_value,
    -- RFM composite score (weighted average)
    ROUND((recency_score * 0.35 + frequency_score * 0.35 + monetary_score * 0.30)::NUMERIC, 2) as customer_segment_score,
    recency_score,
    frequency_score,
    monetary_score
FROM rfm_scored
