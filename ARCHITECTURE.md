# Architecture: E-Commerce Data Pipeline

## Overview

This pipeline implements the **Medallion Architecture** (Bronze → Silver → Gold) to ingest, clean, validate, and engineer features from 8 e-commerce data sources. It uses **Polars** for high-performance data manipulation in Bronze/Silver and **DuckDB SQL** for analytical aggregations in Gold.

## Design Decisions

### Why Medallion Architecture?

The medallion pattern provides clear separation of concerns:
- **Bronze**: Raw data preserved as-is for auditability and reprocessing
- **Silver**: Cleaned, validated, and standardized data as the single source of truth (Polars + Pydantic)
- **Gold**: Pre-computed feature tables optimized for analytics and ML (DuckDB SQL)
- **Graph**: Property graph for advanced relationship analysis (SurrealDB)

This maps directly to the assessment requirements: Tasks 1-2 (Bronze/Silver) and Task 3 (Gold).

### Technology Choices

| Layer | Engine | Rationale |
|-------|--------|-----------|
| Bronze | **Polars** | Native multi-format support (CSV, JSON, Parquet), lazy evaluation, zero-copy reads |
| Silver | **Polars + Pydantic** | Polars for vectorized cleaning (fast), Pydantic for row-level schema validation (strict typing, clear errors) |
| Gold | **DuckDB SQL** | SQL is the natural language for analytical aggregations, CTEs, window functions; persistent queryable database. Queries are externalized in `src/gold/sql/` for maintainability. |

**Why Polars over Pandas?** Polars is ~5-10x faster for data manipulation, has a cleaner API for column expressions, and handles type inference better. It also avoids Pandas' common pitfalls (SettingWithCopyWarning, implicit type coercion).

**Why DuckDB for Gold?** The Gold layer is fundamentally about JOINs and GROUP BY aggregations — SQL is more readable and maintainable than DataFrame code for this. DuckDB also provides a persistent `.duckdb` file that can be queried independently.

### Configuration-Driven Design

All pipeline behavior is externalized into 4 YAML config files:

```
config/
├── pipeline_config.yaml   # Paths, DuckDB settings, layer toggles
├── sources.yaml           # Source file mappings, formats, schema refs
├── schemas.yaml           # Field definitions, types, PK/FK relationships
└── cleaning_rules.yaml    # Cleaning rule definitions
```

**Why YAML?** Human-readable, supports comments, standard in data engineering, easy to diff in version control.

### Silver Layer Processing Pipeline

Each source goes through 4 steps in dependency order:

```
Bronze CSV → Clean (Polars) → Dedup (PK) → FK Check → Validate (Pydantic) → Silver CSV
                                                 ↓
                                          Quarantine (JSON)
```

**Dependency-ordered processing** ensures FK validation works correctly:
1. Vendors (no deps) → 2. Products (FK: vendor) → 3. Customers (no deps) → 4. Transactions (FK: customer, product) → ...

### Quarantine Strategy

Invalid records are quarantined to JSON files with:
- Row index for traceability
- Original record values
- Structured error details (field, type, message)

This supports both debugging and audit requirements.

## Folder Structure

```
outputs/
├── processed/
│   ├── bronze/          # Raw data as CSV (1:1 with sources)
│   ├── silver/          # Cleaned, validated data
│   └── gold/            # Feature tables
├── quarantine/          # Invalid records with error details
├── logs/                # Pipeline execution logs
├── quality_report.md    # Auto-generated quality report
├── pipeline.duckdb      # Persistent analytical database
└── scripts/             # Utility scripts (e.g., demo_graph_queries.py)
```

## Data Quality Issue Categorization

A key design decision is distinguishing **legitimate business edge cases** from **genuinely bad data**. Blindly quarantining everything that deviates from a norm would lose valuable business information (e.g., return transactions). The table below documents each known issue and how the pipeline handles it.

### ✅ Legitimate Business Data (Keep & Flag)

These are valid business events that look unusual but should be preserved in Silver:

| Issue | Source | Rationale | How Handled |
|-------|--------|-----------|-------------|
| Negative quantities | Transactions | Returns/refunds are standard e-commerce events. README says *(returns/refunds)* | **Kept.** Schema allows negative. Gold layer can filter or flag via `is_return` logic |
| Zero quantities | Transactions | Cancelled or placeholder orders | **Kept.** No minimum constraint on quantity |
| Negative total_spend | Customers | Customer returned more than purchased — net refund balance | **Kept.** No `ge=0` constraint on total_spend |
| Negative quantities in invoices | Invoices | Credit notes / return adjustments are standard in AP | **Kept.** No minimum constraint on invoice amounts |
| Inactive vendor status | Vendors | Legitimate business state — vendor went inactive, products remain in catalog | **Kept.** All status values accepted |
| Pending approval status | Vendors | Pre-activation state in vendor onboarding | **Kept.** All status values accepted |
| Missing categories | Products | Not all products are categorized yet, nullable field | **Kept.** Category is Optional |
| Missing contact info | Vendors | Partial vendor records during onboarding | **Kept.** All non-ID fields are Optional |
| Payment dates before invoice dates | Invoices | Prepayments or advance deposits | **Kept.** Flagged in Gold via `reconciliation_flag` |
| Missing ticket references in calls | Call Transcripts | Not all inbound calls are related to a ticket | **Kept.** `ticket_id` is Optional |
| Null satisfaction scores | Support Tickets | Survey not yet completed | **Kept.** `satisfaction_score` is Optional |
| Missing sentiment | Reviews | NLP sentiment not yet computed | **Kept.** `sentiment` is Optional |
| Inconsistent currency codes | Transactions/Invoices | Multi-currency is valid for international e-commerce | **Kept.** Preserved as-is in Silver |

### ❌ Data Quality Errors (Quarantine or Clean)

These represent genuinely corrupted, impossible, or invalid data:

| Issue | Source | Rationale | How Handled |
|-------|--------|-----------|-------------|
| **Duplicate SKUs (Records)** | Products | Same product entered twice (same ID, same SKU) | **Deduplication** on `product_id` |
| **SKU Collisions** | Products | Distinct products share same SKU. See [Deep Dive](docs/product_sku_collision_analysis.md) | **Kept.** Deduplicating by SKU would delete valid active products. Flagged in Quality Report. |
| **Negative prices** | Products | A product cannot cost < $0 — pricing data corruption | **Quarantined.** Pydantic: `price >= 0` |
| **Invalid vendor references** | Products | `VND-INVALID` — no matching vendor exists | **Quarantined.** FK validation against vendor table |
| **Invalid customer/product refs** | Transactions | Orphaned FK — references non-existent entities | **Quarantined.** FK validation via dependency-ordered processing |
| **Duplicate transactions** | Transactions | Same transaction recorded twice | **Deduplication** on `transaction_id` |
| **Duplicate invoice numbers** | Invoices | Same invoice recorded twice | **Deduplication** on `invoice_id` |
| **Invalid vendor refs in invoices** | Invoices | FK to non-existent vendor | **Quarantined.** FK validation |
| **Calculation mismatches** | Invoices | Totals don't match line items | **Flagged** in Gold via `reconciliation_flag` (MISMATCH/OK/NO_REFERENCE) |
| **Duplicate/invalid emails** | Customers | Data entry errors | **Cleaned** (lowercase normalization) + Pydantic type validation |
| **Future registration dates** | Customers | Physically impossible — clock error or data corruption | **Quarantined** via validation (could add explicit date range check) |
| **Inconsistent phone formats** | Customers | `(555) 123-4567` vs `555.987.6543` | **Cleaned.** `phone_normalize` strips all formatting → `5551234567` |
| **Mixed boolean representations** | Products, Transactions | `true/True/1/yes/YES` → inconsistent | **Cleaned.** `boolean_normalize` standardizes to `True`/`False` |
| **Mixed case naming** | Vendors, Customers | `ACTIVE` vs `active` vs `Active` | **Cleaned.** `lowercase`/`uppercase` cleaning rules |
| **Inconsistent channel/status case** | Support Tickets | `email` vs `EMAIL` | **Cleaned.** Case normalization in Silver |
| **Invalid date formats** | Tickets, Reviews | `20-03-2025` vs ISO `2025-03-20` | **Cleaned.** `date_iso` normalization rule |
| **Satisfaction scores > 5** | Support Tickets | Out of range (max is 5) — data entry error | **Quarantined.** Pydantic: `ge=1, le=5` |
| **Negative ratings** | Reviews | Impossible — minimum is 1 | **Quarantined.** Pydantic: `ge=1, le=5` |
| **Empty review_ids** | Reviews | Missing required identifier | **Quarantined.** Custom Pydantic validator rejects empty strings |
| **Empty ticket_ids** | Support Tickets | Missing required identifier | **Quarantined.** Custom Pydantic validator rejects empty strings |
| **Invalid product refs** (`PRD-INVALID`) | Reviews | FK to non-existent product | **Quarantined.** FK validation |
| **Invalid customer refs** (`INVALID_ID`) | Tickets, Calls | FK to non-existent customer | **Quarantined.** FK validation |
| **Negative duration** | Call Transcripts | Impossible measurement | **Quarantined.** Pydantic: `ge=0` |
| **Encoding issues** | Products | Special characters in names | **Kept.** UTF-8 handling preserves special chars |
| **Invalid quality_score** | Call Transcripts | Score outside 0-100 range | **Quarantined.** Pydantic: `ge=0, le=100` |
| **Negative transfers** | Call Transcripts | Transfer count cannot be negative | **Quarantined.** Pydantic: `ge=0` |
| **Invalid tax_rate** | Invoices | Tax rate must be 0.0-1.0 | **Quarantined.** Pydantic: `ge=0.0, le=1.0` |
| **Invalid discount_percent** | Transactions | Discount must be 0-100% | **Quarantined.** Pydantic: `ge=0.0, le=100.0` |

### Summary Table

| Strategy | Count | Examples |
|----------|-------|---------|
| **Keep as-is** | 13 issues | Negative quantities (returns), inactive vendors, null optionals |
| **Clean/Normalize** | 5 issues | Phone format, boolean values, case, dates |
| **Deduplicate** | 3 issues | Duplicate products, transactions, invoices |
| **Quarantine** | 11 issues | Negative prices, invalid FKs, out-of-range scores, empty IDs |
| **Flag in Gold** | 2 issues | Calculation mismatches, payment-before-invoice |

## Cleaning Rules

- **Phone normalization** (`phone_normalize`): Strip `()-.+ ` characters → digits only
- **Boolean normalization** (`boolean_normalize`): Map `true/yes/1/y` → `True`, `false/no/0/n` → `False`
- **Case normalization** (`lowercase` / `uppercase`): Standardize case for emails, statuses, channels
- **Date standardization** (`date_iso`): Convert to ISO 8601 format
- **Payment Method normalization**: Standardize to lowercase (e.g., `Credit Card` → `credit_card`)

## Deduplication

Deduplication is done on primary keys (e.g., `product_id`, `transaction_id`), keeping the first occurrence. This handles: duplicate SKUs, duplicate transactions, duplicate invoice numbers.

## Referential Integrity

Foreign key validation checks that referenced records exist in the parent table. Processing order ensures parent tables are processed first:

1. `vendors` (no deps) → 2. `products` (FK: vendor) → 3. `customers` (no deps) → 4. `transactions` (FK: customer, product) → 5. `invoices` (FK: vendor) → 6. `reviews` (FK: product, customer) → 7. `support_tickets` (FK: customer) → 8. `call_transcripts` (FK: customer, ticket)

## Schema Validation

Pydantic models enforce:
- Required fields (non-null)
- Type constraints (string, int, float, bool)
- Value ranges where physically meaningful (price ≥ 0, rating 1-5, satisfaction 1-5, duration ≥ 0)
- Pattern matching (ID formats: `CUS-\d+`, `PRD-\d+`, etc.)
- Empty string rejection for required identifiers (review_id, ticket_id)

## Feature Engineering

All features are computed using DuckDB SQL with CTEs for clarity:

| Table | Key Features |
|-------|-------------|
| `customer_features` | CLV, RFM score, purchase frequency, recency |
| `product_features` | Revenue contribution, velocity, stock turnover, vendor-weighted score |
| `vendor_features` | Quality score, payment rate, outstanding balance |
| `invoice_features` | Days to payment, overdue, line item diversity, reconciliation flag |

## Error Handling

- Each source processes independently — one failure doesn't stop the pipeline
- Structured logging with `loguru` (component-tagged, debug + file output)
- Graceful degradation: missing Pydantic schemas → skip validation, still output data
- `--fresh` flag for idempotent re-runs

## Graph Layer (SurrealDB)

The optional Graph layer loads Silver data into SurrealDB as a property graph:

- **Technology**: SurrealDB + async Python SDK (`AsyncSurreal`)
- **Schema**: SCHEMAFULL tables with typed `in`/`out` fields for directional edges
- **Node tables** (9): vendor, product, customer, category, region, invoice, support_ticket, call_transcript, agent
- **Edge tables** (13): supplies, belongs_to, purchased, located_in, based_in, billed, invoice_item, reviewed, raised, about, handled_by, includes_transcript, conducted_by
- **Design**: Transactions modeled as edges with properties; categories/regions extracted as first-class nodes; Agents derived from interaction data
- **Queries**: 11 sample graph traversal queries (reliable vendor products, co-purchase, influence scoring, agent performance, customer support journey, etc.)
- **Detail**: See `docs/GRAPH_DESIGN.md` for full schema design rationale

## Testing Strategy

- **Unit tests**: Isolated tests for each layer's core functions
- **Integration test**: End-to-end pipeline run with real data
- **Fixtures**: Shared test data in `conftest.py`
- **Framework**: `pytest` with coverage reporting

## Documentation Index

| Document | Description |
|----------|-------------|
| [README.md](./README.md) | Project entry point, requirements, and setup instructions. |
| [ARCHITECTURE.md](./ARCHITECTURE.md) | High-level system design, technology choices, and data quality strategy. |
| [docs/GRAPH_DESIGN.md](./docs/GRAPH_DESIGN.md) | **Detailed Reference**: Graph schema (nodes/edges), design decisions, and query patterns. |
| [docs/product_sku_collision_analysis.md](./docs/product_sku_collision_analysis.md) | **Deep Dive**: Analysis of product SKU duplication findings. |
| [docs/feature_definitions.md](./docs/feature_definitions.md) | **Reference**: Definitions and formulas for all Gold layer features. |
| [outputs/quality_report.md](./outputs/quality_report.md) | **Generated Report**: Latest data quality metrics and validation results. |
