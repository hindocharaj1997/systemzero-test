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

## Data Quality & Error Handling

A key design decision is distinguishing **legitimate business edge cases** (which should be flagged but kept) from **genuinely bad data** (which must be quarantined). The pipeline implements a strict **Medallion Architecture**:
- **Bronze**: Raw ingestion
- **Silver**: Type validation, cleaning, and referential integrity checks
- **Gold**: Advanced business logic validation

### Detailed Analysis by Source

#### 1. Products (`products.csv`)
| Known Issue | Requirement | Implementation | Status |
| :--- | :--- | :--- | :--- |
| **Duplicate SKUs** | Identify & Handle | `Unique key` deduplication in `SilverProcessor` | ✅ **Deduplicated** |
| **Negative prices** | Handle | `ProductSchema`: `price >= 0` validator | ✅ **Quarantined** |
| **Missing categories** | Handle | `ProductSchema`: `category: Optional[str]` | ✅ **Kept (Null)** |
| **Invalid vendor refs** | Handle | FK Check: `vendor_id` vs `vendors.csv` keys | ✅ **Quarantined** (`VND-INVALID`) |
| **Inconsistent booleans** | Standardize | `SilverCleaner`: `boolean_normalize` | ✅ **Cleaned** |
| **Mixed case naming** | Standardize | `SilverCleaner`: `lowercase`/`uppercase` | ✅ **Cleaned** |
| **Encoding issues** | Handle | UTF-8 parsing in Polars | ✅ **Handled** |
| **Inconsistent dates** | Standardize | `SilverCleaner`: `date_iso` | ✅ **Cleaned** |

#### 2. Customers (`customers.csv`)
| Known Issue | Requirement | Implementation | Status |
| :--- | :--- | :--- | :--- |
| **Duplicate emails** | Handle | `Unique key` (customer_id) deduplication | ✅ **Deduplicated** |
| **Invalid emails** | Handle | `CustomerSchema.validate_email_format` | ✅ **Set to Null** (Record kept) |
| **Inconsistent phone** | Standardize | `SilverCleaner`: `phone_normalize` (digits only) | ✅ **Cleaned** |
| **Future dates** | Identify & Handle | `CustomerSchema.date_not_in_future` | ✅ **Set to Null** (Record kept) |
| **Invalid age values** | Handle | `CustomerSchema`: `age: Optional[int]` | ✅ **Quarantined** (Type Error) |
| **Missing required fields** | Handle | `CustomerSchema`: `full_name: str` (required) | ✅ **Quarantined** |
| **Mixed date formats** | Standardize | `SilverCleaner`: `date_iso` | ✅ **Cleaned** |

#### 3. Sales Transactions (`transactions.csv`)
| Known Issue | Requirement | Implementation | Status |
| :--- | :--- | :--- | :--- |
| **Negative quantities** | Handle (Returns) | `TransactionSchema`: `quantity` allows negative | ✅ **Kept** (Valid Return) |
| **Zero quantities** | Handle | `TransactionSchema`: No minimum constraint | ✅ **Kept** (Cancelled/Placeholder) |
| **Invalid Customer Refs** | Handle | FK Check: `customer_id` | ✅ **Quarantined** |
| **Invalid Product Refs** | Handle | FK Check: `product_id` | ✅ **Quarantined** |
| **Duplicate transactions** | Deduplicate | `Unique key` deduplication | ✅ **Deduplicated** |
| **Inconsistent currency** | Standardize | Preserved as-is (valid multi-currency) | ✅ **Kept** |

#### 4. Vendors (`vendors.csv`)
| Known Issue | Requirement | Implementation | Status |
| :--- | :--- | :--- | :--- |
| **Inactive vendors** | Handle | `VendorSchema`: `status` field allows "inactive" | ✅ **Kept** (Valid State) |
| **Missing contact info** | Handle | `VendorSchema`: `contact_email` is Optional | ✅ **Kept (Null)** |
| **Pending approval** | Handle | `VendorSchema`: `status` allows "pending" | ✅ **Kept** |

#### 5. Invoices (`invoices.csv`)
| Known Issue | Requirement | Implementation | Status |
| :--- | :--- | :--- | :--- |
| **Duplicate numbers** | Deduplicate | `Unique key` deduplication | ✅ **Deduplicated** |
| **Invalid vendor refs** | Handle | FK Check: `vendor_id` | ✅ **Quarantined** |
| **Calculation mismatches** | Identify | `Gold` layer: `reconciliation_flag` | ✅ **Flagged in Gold** |
| **Negative quantities** | Handle (Credit) | `InvoiceSchema`: `subtotal`/`quantity` allow negative | ✅ **Kept** (Credit Note) |
| **Payment before invoice** | Identify | `Gold` layer check | ✅ **Flagged in Gold** |
| **Nested JSON** | Parse | `SilverProcessor`: Parses `line_items_json` to table | ✅ **Parsed & Normalized** |

#### 6. Support Tickets (`support_tickets.json`)
| Known Issue | Requirement | Implementation | Status |
| :--- | :--- | :--- | :--- |
| **Invalid Customer Refs** | Handle | FK Check: `customer_id` | ✅ **Quarantined** (`INVALID_ID`) |
| **Missing Ticket IDs** | Handle | `SupportTicketSchema`: empty check | ✅ **Quarantined** |
| **Scores out of range** | Validate | `SupportTicketSchema`: `ge=1, le=5` | ✅ **Quarantined** |
| **Inconsistent channel** | Standardize | `SilverCleaner`: `lowercase` | ✅ **Cleaned** |
| **Future dates** | Handle | `SilverCleaner`: `date_iso` (format only) | ✅ **Format Cleaned** (Logic in Gold) |

#### 7. Product Reviews (`product_reviews.json`)
| Known Issue | Requirement | Implementation | Status |
| :--- | :--- | :--- | :--- |
| **Invalid Product Refs** | Handle | FK Check: `product_id` | ✅ **Quarantined** (`PRD-INVALID`) |
| **Missing Review IDs** | Handle | `ReviewSchema`: empty check | ✅ **Quarantined** |
| **Invalid ratings** | Validate | `ReviewSchema`: `ge=1, le=5` | ✅ **Quarantined** (Caught `-1`) |
| **Verified mismatch** | Identify | Cross-check with Transactions | ➡️ **Deferred to Gold Layer** |
| **Inconsistent dates** | Standardize | `SilverCleaner`: `date_iso` | ✅ **Cleaned** |

#### 8. Call Transcripts (`call_transcripts.json`)
| Known Issue | Requirement | Implementation | Status |
| :--- | :--- | :--- | :--- |
| **Missing Ticket Refs** | Handle | `CallTranscriptSchema`: `ticket_id` is Optional | ✅ **Kept** (Valid) |
| **Invalid Customer Refs** | Handle | FK Check: `customer_id` | ✅ **Quarantined** (`INVALID`) |
| **Negative duration** | Validate | `CallTranscriptSchema`: `ge=0` | ✅ **Quarantined** |
| **Missing agent info** | Handle | `CallTranscriptSchema`: `agent_id` is Optional | ✅ **Kept** |
| **Quality score range** | Validate | `CallTranscriptSchema`: `ge=0, le=100` | ✅ **Quarantined** |

### Future Roadmap: Advanced Data Quality

The current pipeline enforces strict schema and referential integrity. The following advanced validation rules are planned for the **Gold Layer**, where cross-table joins are more efficient:

1.  **Verified Purchase Verification**: Joining `Reviews` with `Transactions` to flag reviews from users who haven't purchased the item.
2.  **Transcript Timestamp Ordering**: Parsing the `utterances` JSON array to strictly validate that timestamps are sequential.
3.  **Strict Email DNS Check**: Enhancing the Silver layer email format check with a domain existence verification.

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
