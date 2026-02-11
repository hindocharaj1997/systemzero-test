# Schema Comparison: Bronze vs Silver (Final State)

This document confirms 1:1 schema parity between Bronze (source) and Silver (processed) layers for all business data.

## 1. Call Transcripts

| Column Name | In Bronze | In Silver | Notes |
|---|---|---|---|
| call_id | Yes | Yes | |
| ticket_id | Yes | Yes | |
| customer_id | Yes | Yes | |
| agent_id | Yes | Yes | |
| agent_name | Yes | Yes | |
| call_start | Yes | Yes | |
| call_end | Yes | Yes | |
| duration_seconds | Yes | Yes | |
| call_type | Yes | Yes | |
| phone_number | Yes | Yes | |
| queue_wait_seconds | Yes | Yes | |
| hold_time_seconds | Yes | Yes | |
| transfers | Yes | **Yes** | Added |
| language | Yes | **Yes** | Added |
| sentiment_overall | Yes | Yes | |
| resolution_achieved | Yes | Yes | |
| utterances | Yes | Yes | |
| keywords_detected | Yes | Yes | |
| action_items | Yes | Yes | |
| quality_score | Yes | **Yes** | |
| _source_file | Yes | **Yes** | Renamed to `source_file` |
| _loaded_at | Yes | **Yes** | Renamed to `loaded_at` |

## 2. Customers

| Column Name | In Bronze | In Silver | Notes |
|---|---|---|---|
| customer_id | Yes | Yes | |
| first_name | Yes | Yes | |
| last_name | Yes | Yes | |
| full_name | Yes | Yes | |
| email | Yes | Yes | |
| phone | Yes | Yes | |
| gender | Yes | Yes | |
| age | Yes | Yes | |
| date_of_birth | Yes | Yes | |
| address_street | Yes | Yes | |
| address_city | Yes | Yes | |
| address_state | Yes | Yes | |
| address_postal_code | Yes | Yes | |
| address_country | Yes | Yes | |
| segment | Yes | Yes | |
| total_orders | Yes | **Yes** | Added as snapshot |
| total_spend | Yes | Yes | |
| average_order_value | Yes | **Yes** | Added as snapshot |
| registration_date | Yes | Yes | |
| last_purchase_date | Yes | Yes | |
| is_active | Yes | Yes | |
| email_verified | Yes | Yes | |
| phone_verified | Yes | Yes | |
| preferences_newsletter | Yes | Yes | |
| preferences_sms_notifications | Yes | **Yes** | Added |
| preferences_preferred_language | Yes | **Yes** | Added |
| preferences_preferred_currency | Yes | **Yes** | Added |
| metadata_source | Yes | **Yes** | Added |
| metadata_created_at | Yes | Yes | |
| metadata_updated_at | Yes | Yes | |

## 3. Invoices

| Column Name | In Bronze | In Silver | Notes |
|---|---|---|---|
| invoice_id | Yes | Yes | |
| invoice_number | Yes | Yes | |
| vendor_id | Yes | Yes | |
| vendor_name | Yes | **Yes** | Added as snapshot |
| invoice_date | Yes | Yes | |
| due_date | Yes | Yes | |
| payment_terms | Yes | Yes | |
| po_number | Yes | Yes | |
| line_item_count | Yes | No | |
| line_items_json | Yes | No | Parsed into `invoice_line_items` |
| subtotal | Yes | Yes | |
| tax_rate | Yes | **Yes** | Added |
| tax_amount | Yes | Yes | |
| shipping_handling | Yes | Yes | |
| total_amount | Yes | Yes | |
| currency | Yes | Yes | |
| payment_status | Yes | Yes | |
| amount_paid | Yes | **Yes** | Added |
| balance_due | Yes | **Yes** | Added |
| payment_date | Yes | Yes | |
| payment_method | Yes | **Yes** | Added |
| approved_by | Yes | Yes | |
| notes | Yes | Yes | |
| created_at | Yes | Yes | |
| updated_at | Yes | Yes | |

## 4. Products

| Column Name | In Bronze | In Silver | Notes |
|---|---|---|---|
| product_id | Yes | Yes | |
| sku | Yes | Yes | |
| product_name | Yes | Yes | |
| description | Yes | Yes | |
| category | Yes | Yes | |
| subcategory | Yes | Yes | |
| vendor_id | Yes | Yes | |
| price | Yes | Yes | |
| cost | Yes | Yes | |
| currency | Yes | Yes | |
| weight_kg | Yes | Yes | |
| stock_quantity | Yes | Yes | |
| reorder_level | Yes | Yes | |
| rating | Yes | Yes | |
| review_count | Yes | Yes | |
| is_active | Yes | Yes | |
| created_date | Yes | Yes | |
| last_updated | Yes | Yes | |
| tags | Yes | Yes | |

## 5. Reviews

| Column Name | In Bronze | In Silver | Notes |
|---|---|---|---|
| review_id | Yes | Yes | |
| product_id | Yes | Yes | |
| customer_id | Yes | Yes | |
| rating | Yes | Yes | |
| title | Yes | Yes | |
| review_text | Yes | Yes | |
| helpful_votes | Yes | Yes | |
| verified_purchase | Yes | Yes | |
| review_date | Yes | Yes | |
| sentiment | Yes | Yes | |
| images | Yes | Yes | |
| response_responder | Yes | **Yes** | Added |
| response_response_text | Yes | **Yes** | Added |
| response_response_date | Yes | **Yes** | Added |
| response | Yes | Yes | |

## 6. Support Tickets

| Column Name | In Bronze | In Silver | Notes |
|---|---|---|---|
| ticket_id | Yes | Yes | |
| customer_id | Yes | Yes | |
| product_id | Yes | Yes | |
| channel | Yes | Yes | |
| category | Yes | No | |
| priority | Yes | Yes | |
| status | Yes | Yes | |
| created_at | Yes | Yes | |
| resolved_at | Yes | Yes | |
| agent_id | Yes | Yes | |
| satisfaction_score | Yes | Yes | |
| transcript | Yes | Yes | |
| summary | Yes | Yes | |
| tags | Yes | Yes | |
| resolution_type | Yes | Yes | |

## 7. Transactions

| Column Name | In Bronze | In Silver | Notes |
|---|---|---|---|
| transaction_id | Yes | Yes | |
| order_id | Yes | Yes | |
| customer_id | Yes | Yes | |
| product_id | Yes | Yes | |
| transaction_date | Yes | Yes | |
| transaction_timestamp | Yes | Yes | |
| quantity | Yes | Yes | |
| unit_price | Yes | Yes | |
| subtotal | Yes | Yes | |
| discount_percent | Yes | **Yes** | Added |
| discount_amount | Yes | Yes | |
| tax_rate | Yes | **Yes** | Added |
| tax_amount | Yes | Yes | |
| shipping_cost | Yes | Yes | |
| total_amount | Yes | Yes | |
| currency | Yes | No | (Implicit) |
| payment_method | Yes | Yes | |
| payment_status | Yes | **Yes** | Added |
| order_status | Yes | Yes | |
| shipping_method | Yes | Yes | |
| channel | Yes | Yes | |
| region | Yes | Yes | |
| is_gift | Yes | **Yes** | Added |
| notes | Yes | **Yes** | Added |

## 8. Vendors

| Column Name | In Bronze | In Silver | Notes |
|---|---|---|---|
| vendor_id | Yes | Yes | |
| vendor_name | Yes | Yes | |
| vendor_code | Yes | Yes | |
| country | Yes | Yes | |
| country_code | Yes | **Yes** | Added |
| region | Yes | Yes | |
| status | Yes | Yes | |
| reliability_score | Yes | Yes | |
| lead_time_days | Yes | Yes | |
| payment_terms | Yes | Yes | |
| currency | Yes | Yes | |
| contact_primary_name | Yes | Yes | |
| contact_email | Yes | Yes | |
| contact_phone | Yes | Yes | |
| address_street | Yes | Yes | |
| address_city | Yes | Yes | |
| address_state | Yes | Yes | |
| address_postal_code | Yes | Yes | |
| address_country | Yes | Yes | |
| categories | Yes | Yes | |
| certifications | Yes | Yes | |
| created_at | Yes | Yes | |
| updated_at | Yes | Yes | |
