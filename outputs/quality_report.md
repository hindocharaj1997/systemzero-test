# Data Quality Report

**Generated:** 2026-02-10 22:33:11

**Pipeline:** Medallion Architecture (Polars → DuckDB)

---

## Bronze Layer (Ingestion)

| Source | Format | Records | Status |
|--------|--------|---------|--------|
| products | csv | 505 | ✓ |
| customers | json | 1,003 | ✓ |
| transactions | parquet | 5,010 | ✓ |
| vendors | json | 10 | ✓ |
| invoices | csv | 805 | ✓ |
| support_tickets | json | 20 | ✓ |
| reviews | json | 30 | ✓ |
| call_transcripts | json | 10 | ✓ |
| **Total** | | **7,393** | |

---

## Silver Layer (Cleaning & Validation)

| Source | Total | Valid | Quarantined | Deduped | Orphaned | Pass Rate |
|--------|-------|-------|-------------|---------|----------|-----------|
| vendors | 10 | 10 | 0 | 0 | 0 | 100.0% |
| products | 505 | 481 | 19 | 5 | 1 | 95.2% |
| customers | 1,003 | 999 | 1 | 3 | 0 | 99.6% |
| transactions | 5,010 | 4,740 | 260 | 10 | 260 | 94.6% |
| invoices | 805 | 784 | 16 | 5 | 16 | 97.4% |
| reviews | 30 | 21 | 9 | 0 | 7 | 70.0% |
| support_tickets | 20 | 14 | 6 | 0 | 5 | 70.0% |
| call_transcripts | 10 | 7 | 3 | 0 | 3 | 70.0% |
| **Total** | **7,393** | **7,056** | **314** | **23** | **292** | **95.4%** |

### Validation Error Breakdown

| Error Type | Count |
|------------|-------|
| referential_integrity | 292 |
| greater_than_equal | 16 |
| float_parsing | 2 |
| value_error | 2 |
| less_than_equal | 1 |
| int_parsing | 1 |

### Cleaning Rules Applied

| Source | Fields Cleaned |
|--------|---------------|
| vendors | country_code, currency, contact_email, contact_phone, created_at, updated_at, status |
| products | is_active, currency, last_updated |
| customers | email, phone, gender, date_of_birth, preferences_preferred_language, preferences_preferred_currency, metadata_created_at, metadata_updated_at, registration_date, last_purchase_date, is_active |
| transactions | transaction_date, order_status, transaction_timestamp, is_gift, payment_status, payment_method, shipping_method, channel |
| invoices | invoice_date, due_date, payment_date, payment_status, payment_method, currency, created_at, updated_at |
| reviews | sentiment, review_date, response_response_date |
| support_tickets | channel, priority, status, created_at, resolved_at, resolution_type |
| call_transcripts | call_start, sentiment_overall, call_end, call_type, phone_number, language |

### Deduplication

| Source | Duplicates Removed |
|--------|-------------------|
| products | 5 |
| customers | 3 |
| transactions | 10 |
| invoices | 5 |

### Referential Integrity Violations

| Source | Orphaned Records | Details |
|--------|-----------------|---------|
| products | 1 | Invalid foreign key references |
| transactions | 260 | Invalid foreign key references |
| invoices | 16 | Invalid foreign key references |
| reviews | 7 | Invalid foreign key references |
| support_tickets | 5 | Invalid foreign key references |
| call_transcripts | 3 | Invalid foreign key references |

---

## Gold Layer (Feature Engineering)

| Feature Table | Rows | Features |
|---------------|------|----------|
| customer_features | 999 | 20 |
| product_features | 481 | 18 |
| vendor_features | 10 | 16 |
| invoice_features | 784 | 15 |
| **Total** | | **69** |

---

## Data Quality Issues Detected

Based on the README's known issues list:

| Issue | Source | How Handled |
|-------|--------|-------------|
| Duplicate SKUs | products | Deduplication on product_id |
| Negative prices | products | Pydantic: price >= 0 |
| Invalid vendor references | products | FK: vendor_id → vendors |
| Duplicate/invalid emails | customers | Pydantic validation |
| Inconsistent phone formats | customers | phone_normalize cleaning |
| Future dates | customers | Quarantined via validation |
| Negative quantities | transactions | Pydantic validation |
| Invalid customer/product refs | transactions | FK validation |
| Duplicate transactions | transactions | Deduplication on transaction_id |
| Duplicate invoice numbers | invoices | Deduplication on invoice_id |
| Invalid vendor refs in invoices | invoices | FK: vendor_id → vendors |
| Invalid product refs in reviews | reviews | FK: product_id → products |
| Invalid customer refs in tickets | support_tickets | FK: customer_id → customers |
| Satisfaction scores out of range | support_tickets | Pydantic: 1-5 range |
| Missing ticket refs in calls | call_transcripts | FK: ticket_id → tickets |

---

## Quarantine Files

- `call_transcripts_quarantine.json`: 3 records
- `customers_quarantine.json`: 1 records
- `invoices_quarantine.json`: 16 records
- `products_quarantine.json`: 19 records
- `reviews_quarantine.json`: 9 records
- `support_tickets_quarantine.json`: 6 records
- `transactions_quarantine.json`: 260 records
