CREATE OR REPLACE TABLE product_features AS
WITH transaction_stats AS (
    SELECT 
        product_id,
        COUNT(*) as times_sold,
        COUNT(*) FILTER (WHERE COALESCE(total_amount, 0) < 0) as return_count,
        SUM(quantity) as total_quantity_sold,
        SUM(total_amount) as total_revenue,
        SUM(CASE WHEN COALESCE(total_amount, 0) >= 0 THEN total_amount ELSE 0 END) as gross_revenue,
        COUNT(DISTINCT customer_id) as unique_customers,
        MIN(transaction_date) as first_sale,
        MAX(transaction_date) as last_sale
    FROM transactions
    GROUP BY product_id
),
review_stats AS (
    SELECT
        product_id,
        COUNT(*) as review_count,
        AVG(TRY_CAST(rating AS FLOAT)) as avg_rating
    FROM reviews
    GROUP BY product_id
)
SELECT 
    p.product_id,
    p.product_name,
    p.category,
    p.vendor_id,
    TRY_CAST(p.price AS DOUBLE) as price,
    TRY_CAST(p.cost AS DOUBLE) as cost,
    TRY_CAST(p.stock_quantity AS INT) as stock_quantity,
    COALESCE(t.times_sold, 0) as times_sold,
    COALESCE(t.total_quantity_sold, 0) as total_quantity_sold,
    -- revenue_contribution
    COALESCE(t.total_revenue, 0) as revenue_contribution,
    COALESCE(t.unique_customers, 0) as unique_customers,
    COALESCE(r.review_count, 0) as review_count,
    COALESCE(r.avg_rating, 0) as avg_rating,
    -- profit_margin
    CASE 
        WHEN TRY_CAST(p.price AS DOUBLE) > 0 
        THEN (TRY_CAST(p.price AS DOUBLE) - COALESCE(TRY_CAST(p.cost AS DOUBLE), 0)) / TRY_CAST(p.price AS DOUBLE) 
        ELSE 0 
    END as profit_margin,
    -- price_tier
    CASE 
        WHEN TRY_CAST(p.price AS DOUBLE) < 50 THEN 'Low'
        WHEN TRY_CAST(p.price AS DOUBLE) < 200 THEN 'Medium'
        WHEN TRY_CAST(p.price AS DOUBLE) < 500 THEN 'High'
        ELSE 'Premium'
    END as price_tier,
    -- velocity_score: sales per month since first sale
    CASE 
        WHEN DATEDIFF('month', TRY_CAST(t.first_sale AS DATE), TRY_CAST(t.last_sale AS DATE)) > 0
        THEN COALESCE(t.times_sold, 0)::FLOAT / DATEDIFF('month', TRY_CAST(t.first_sale AS DATE), TRY_CAST(t.last_sale AS DATE))
        ELSE COALESCE(t.times_sold, 0)::FLOAT
    END as velocity_score,
    -- stock_turnover_rate: units sold / current stock
    CASE 
        WHEN TRY_CAST(p.stock_quantity AS INT) > 0 
        THEN COALESCE(t.total_quantity_sold, 0)::FLOAT / TRY_CAST(p.stock_quantity AS INT)
        ELSE 0
    END as stock_turnover_rate,
    -- vendor_reliability_weighted_score: avg_rating * vendor reliability / 100
    CASE 
        WHEN v.reliability_score IS NOT NULL AND r.avg_rating IS NOT NULL
        THEN ROUND((r.avg_rating * TRY_CAST(v.reliability_score AS DOUBLE) / 100)::NUMERIC, 2)
        ELSE COALESCE(r.avg_rating, 0)
    END as vendor_reliability_weighted_score
FROM products p
LEFT JOIN transaction_stats t ON p.product_id = t.product_id
LEFT JOIN review_stats r ON p.product_id = r.product_id
LEFT JOIN vendors v ON p.vendor_id = v.vendor_id