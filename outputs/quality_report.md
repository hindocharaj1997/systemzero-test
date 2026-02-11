# Data Quality Report

**Generated:** 2026-02-10 22:25:34

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
| customers | 1,003 | 788 | 212 | 3 | 0 | 78.6% |
| transactions | 5,010 | 3,748 | 1252 | 10 | 1252 | 74.8% |
| invoices | 805 | 784 | 16 | 5 | 16 | 97.4% |
| reviews | 30 | 17 | 13 | 0 | 11 | 56.7% |
| support_tickets | 20 | 12 | 8 | 0 | 7 | 60.0% |
| call_transcripts | 10 | 6 | 4 | 0 | 4 | 60.0% |
| **Total** | **7,393** | **5,846** | **1524** | **23** | **1291** | **79.1%** |

### Validation Error Breakdown

| Error Type | Count |
|------------|-------|
| referential_integrity | 1291 |
| value_error | 214 |
| greater_than_equal | 16 |
| float_parsing | 2 |
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
| transactions | 1252 | Invalid foreign key references |
| invoices | 16 | Invalid foreign key references |
| reviews | 11 | Invalid foreign key references |
| support_tickets | 7 | Invalid foreign key references |
| call_transcripts | 4 | Invalid foreign key references |

---

## Gold Layer (Feature Engineering)

| Feature Table | Rows | Features |
|---------------|------|----------|
| customer_features | 788 | 20 |
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

- `call_transcripts_quarantine.json`: 4 records
- `customers_quarantine.json`: 212 records
- `invoices_quarantine.json`: 16 records
- `products_quarantine.json`: 19 records
- `reviews_quarantine.json`: 13 records
- `support_tickets_quarantine.json`: 8 records
- `transactions_quarantine.json`: 1252 records
