CREATE OR REPLACE TABLE invoice_features AS
WITH {line_items_cte},
base_invoice AS (
    SELECT 
        i.invoice_id,
        i.vendor_id,
        i.invoice_date,
        i.due_date,
        i.payment_date,
        TRY_CAST(i.total_amount AS DOUBLE) as total_amount,
        i.payment_status,
        i.payment_terms,
        -- Extract NET days from payment_terms (NET15→15, NET30→30, etc.)
        TRY_CAST(REGEXP_EXTRACT(i.payment_terms, '\\d+') AS INT) as terms_days,
        -- payment_terms_days from dates
        DATEDIFF('day', TRY_CAST(i.invoice_date AS DATE), TRY_CAST(i.due_date AS DATE)) as payment_terms_days,
        -- days_to_payment
        CASE 
            WHEN i.payment_date IS NOT NULL 
            THEN DATEDIFF('day', TRY_CAST(i.invoice_date AS DATE), CAST(i.payment_date AS DATE))
            ELSE NULL
        END as days_to_payment,
        -- days_overdue
        CASE 
            WHEN i.payment_status != 'paid' AND TRY_CAST(i.due_date AS DATE) < CURRENT_DATE
            THEN DATEDIFF('day', TRY_CAST(i.due_date AS DATE), CURRENT_DATE)
            ELSE 0
        END as days_overdue,
        -- is_overdue
        CASE 
            WHEN i.payment_status != 'paid' AND TRY_CAST(i.due_date AS DATE) < CURRENT_DATE
            THEN true
            ELSE false
        END as is_overdue
    FROM invoices i
)
SELECT 
    b.invoice_id,
    b.vendor_id,
    b.invoice_date,
    b.due_date,
    b.payment_date,
    b.total_amount,
    b.payment_status,
    b.payment_terms,
    COALESCE(b.payment_terms_days, b.terms_days) as payment_terms_days,
    b.days_to_payment,
    b.days_overdue,
    b.is_overdue,
    -- line_item_diversity: unique products per invoice (from parsed line_items_json)
    COALESCE(lis.unique_products, 0) as line_item_diversity,
    -- discount_rate_achieved: % of payment window saved by paying early
    -- e.g., paid in 10 days on NET30 = 1 - (10/30) = 0.667 (saved 66.7% of window)
    CASE 
        WHEN b.days_to_payment IS NOT NULL AND b.terms_days > 0
        THEN ROUND((1.0 - b.days_to_payment::FLOAT / b.terms_days::FLOAT)::NUMERIC, 3)
        ELSE NULL
    END as discount_rate_achieved,
    -- reconciliation_flag: does invoice total match sum of line items?
    CASE 
        WHEN lis.line_items_total IS NOT NULL 
            AND ABS(b.total_amount - lis.line_items_total) / 
                GREATEST(ABS(b.total_amount), 0.01) > 0.1
        THEN 'MISMATCH'
        WHEN lis.line_items_total IS NULL THEN 'NO_REFERENCE'
        ELSE 'OK'
    END as reconciliation_flag
FROM base_invoice b
LEFT JOIN line_item_stats lis ON b.invoice_id = lis.invoice_id