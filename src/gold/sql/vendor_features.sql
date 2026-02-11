CREATE OR REPLACE TABLE vendor_features AS
WITH product_stats AS (
    SELECT 
        vendor_id,
        COUNT(*) as total_products_supplied,
        AVG(TRY_CAST(price AS DOUBLE)) as avg_product_price,
        AVG(TRY_CAST(rating AS DOUBLE)) as product_quality_score
    FROM products
    GROUP BY vendor_id
),
invoice_stats AS (
    SELECT 
        vendor_id,
        COUNT(*) as total_invoices,
        SUM(TRY_CAST(total_amount AS DOUBLE)) as total_invoice_amount,
        AVG(TRY_CAST(total_amount AS DOUBLE)) as average_invoice_value,
        COUNT(CASE
            WHEN payment_status = 'paid'
                AND TRY_CAST(payment_date AS DATE) IS NOT NULL
                AND TRY_CAST(due_date AS DATE) IS NOT NULL
                AND TRY_CAST(payment_date AS DATE) <= TRY_CAST(due_date AS DATE)
            THEN 1
        END) as paid_on_time_invoices,
        SUM(CASE 
            WHEN payment_status != 'paid' 
            THEN COALESCE(TRY_CAST(total_amount AS DOUBLE), 0) 
            ELSE 0 
        END) as total_outstanding_balance
    FROM invoices
    GROUP BY vendor_id
),
transaction_revenue AS (
    SELECT 
        p.vendor_id,
        SUM(TRY_CAST(t.total_amount AS DOUBLE)) as revenue_generated
    FROM transactions t
    JOIN products p ON t.product_id = p.product_id
    GROUP BY p.vendor_id
)
SELECT 
    v.vendor_id,
    v.vendor_name,
    v.country,
    v.region,
    TRY_CAST(v.reliability_score AS DOUBLE) as reliability_score,
    v.status,
    COALESCE(p.total_products_supplied, 0) as total_products_supplied,
    COALESCE(p.avg_product_price, 0) as avg_product_price,
    COALESCE(p.product_quality_score, 0) as product_quality_score,
    COALESCE(i.total_invoices, 0) as total_invoices,
    COALESCE(i.total_invoice_amount, 0) as total_invoice_amount,
    COALESCE(i.average_invoice_value, 0) as average_invoice_value,
    COALESCE(tr.revenue_generated, 0) as revenue_generated,
    COALESCE(i.total_outstanding_balance, 0) as total_outstanding_balance,
    -- invoice_payment_rate
    CASE 
        WHEN COALESCE(i.total_invoices, 0) > 0 
        THEN ROUND((COALESCE(i.paid_on_time_invoices, 0)::FLOAT / i.total_invoices)::NUMERIC, 3)
        ELSE 0 
    END as invoice_payment_rate,
    -- revenue per product
    CASE 
        WHEN COALESCE(p.total_products_supplied, 0) > 0 
        THEN COALESCE(tr.revenue_generated, 0) / p.total_products_supplied 
        ELSE 0 
    END as revenue_per_product
FROM vendors v
LEFT JOIN product_stats p ON v.vendor_id = p.vendor_id
LEFT JOIN invoice_stats i ON v.vendor_id = i.vendor_id
LEFT JOIN transaction_revenue tr ON v.vendor_id = tr.vendor_id
