-- ============================================================
-- NODE TABLES
-- ============================================================

-- Vendor node
REMOVE TABLE IF EXISTS vendor;
DEFINE TABLE vendor SCHEMAFULL;
DEFINE FIELD vendor_name    ON TABLE vendor TYPE string;
DEFINE FIELD country        ON TABLE vendor TYPE option<string>;
DEFINE FIELD region         ON TABLE vendor TYPE option<string>;
DEFINE FIELD reliability_score ON TABLE vendor TYPE option<float>;
DEFINE FIELD status         ON TABLE vendor TYPE option<string>;
DEFINE FIELD source_file    ON TABLE vendor TYPE option<string>;
DEFINE FIELD loaded_at      ON TABLE vendor TYPE option<string>;

-- Product node
REMOVE TABLE IF EXISTS product;
DEFINE TABLE product SCHEMAFULL;
DEFINE FIELD product_name   ON TABLE product TYPE string;
DEFINE FIELD sku            ON TABLE product TYPE string;
DEFINE FIELD category       ON TABLE product TYPE option<string>;
DEFINE FIELD price          ON TABLE product TYPE option<float>;
DEFINE FIELD cost           ON TABLE product TYPE option<float>;
DEFINE FIELD stock_quantity ON TABLE product TYPE option<int>;
DEFINE FIELD rating         ON TABLE product TYPE option<float>;
DEFINE FIELD is_active      ON TABLE product TYPE option<bool>;
DEFINE FIELD source_file    ON TABLE product TYPE option<string>;
DEFINE FIELD loaded_at      ON TABLE product TYPE option<string>;

-- Customer node
REMOVE TABLE IF EXISTS customer;
DEFINE TABLE customer SCHEMAFULL;
DEFINE FIELD full_name          ON TABLE customer TYPE string;
DEFINE FIELD email              ON TABLE customer TYPE option<string>;
DEFINE FIELD phone              ON TABLE customer TYPE option<string>;
DEFINE FIELD segment            ON TABLE customer TYPE option<string>;
DEFINE FIELD total_spend        ON TABLE customer TYPE option<float>;
DEFINE FIELD registration_date  ON TABLE customer TYPE option<string>;
DEFINE FIELD last_purchase_date ON TABLE customer TYPE option<string>;
DEFINE FIELD is_active          ON TABLE customer TYPE option<bool>;
DEFINE FIELD source_file        ON TABLE customer TYPE option<string>;
DEFINE FIELD loaded_at          ON TABLE customer TYPE option<string>;

-- Category node (derived from product categories)
REMOVE TABLE IF EXISTS category;
DEFINE TABLE category SCHEMAFULL;
DEFINE FIELD name ON TABLE category TYPE string;

-- Region node (derived from vendor regions)
REMOVE TABLE IF EXISTS region;
DEFINE TABLE region SCHEMAFULL;
DEFINE FIELD name ON TABLE region TYPE string;

-- Invoice node
REMOVE TABLE IF EXISTS invoice;
DEFINE TABLE invoice SCHEMAFULL;
DEFINE FIELD invoice_date   ON TABLE invoice TYPE option<string>;
DEFINE FIELD due_date       ON TABLE invoice TYPE option<string>;
DEFINE FIELD payment_date   ON TABLE invoice TYPE option<string>;
DEFINE FIELD total_amount   ON TABLE invoice TYPE option<float>;
DEFINE FIELD payment_status ON TABLE invoice TYPE option<string>;
DEFINE FIELD payment_terms  ON TABLE invoice TYPE option<string>;
DEFINE FIELD source_file    ON TABLE invoice TYPE option<string>;
DEFINE FIELD loaded_at      ON TABLE invoice TYPE option<string>;

-- Support Ticket node
REMOVE TABLE IF EXISTS support_ticket;
DEFINE TABLE support_ticket SCHEMAFULL;
DEFINE FIELD status             ON TABLE support_ticket TYPE string;
DEFINE FIELD priority           ON TABLE support_ticket TYPE string;
DEFINE FIELD satisfaction_score ON TABLE support_ticket TYPE option<int>;
DEFINE FIELD created_at         ON TABLE support_ticket TYPE option<string>;
DEFINE FIELD resolved_at        ON TABLE support_ticket TYPE option<string>;
DEFINE FIELD source_file        ON TABLE support_ticket TYPE option<string>;
DEFINE FIELD loaded_at          ON TABLE support_ticket TYPE option<string>;

-- Call Transcript node
REMOVE TABLE IF EXISTS call_transcript;
DEFINE TABLE call_transcript SCHEMAFULL;
DEFINE FIELD sentiment_overall  ON TABLE call_transcript TYPE option<string>;
DEFINE FIELD duration_seconds   ON TABLE call_transcript TYPE option<int>;
DEFINE FIELD quality_score      ON TABLE call_transcript TYPE option<float>;
DEFINE FIELD transfers          ON TABLE call_transcript TYPE option<int>;
DEFINE FIELD call_start         ON TABLE call_transcript TYPE option<string>;
DEFINE FIELD source_file        ON TABLE call_transcript TYPE option<string>;
DEFINE FIELD loaded_at          ON TABLE call_transcript TYPE option<string>;

-- Agent node
REMOVE TABLE IF EXISTS agent;
DEFINE TABLE agent SCHEMAFULL;
DEFINE FIELD name ON TABLE agent TYPE string;

-- ============================================================
-- EDGE TABLES (relationships)
-- ============================================================

-- vendor -> product (vendor supplies a product)
REMOVE TABLE IF EXISTS supplies;
DEFINE TABLE supplies SCHEMAFULL TYPE RELATION IN vendor OUT product;
DEFINE INDEX unique_supplies ON TABLE supplies COLUMNS in, out UNIQUE;

-- product -> category
REMOVE TABLE IF EXISTS belongs_to;
DEFINE TABLE belongs_to SCHEMAFULL TYPE RELATION IN product OUT category;
DEFINE INDEX unique_belongs_to ON TABLE belongs_to COLUMNS in, out UNIQUE;

-- customer -> product (with transaction details on the edge)
REMOVE TABLE IF EXISTS purchased;
DEFINE TABLE purchased SCHEMAFULL TYPE RELATION IN customer OUT product;
DEFINE FIELD transaction_id   ON TABLE purchased TYPE string;
DEFINE FIELD quantity         ON TABLE purchased TYPE option<int>;
DEFINE FIELD total_amount     ON TABLE purchased TYPE option<float>;
DEFINE FIELD transaction_date ON TABLE purchased TYPE option<string>;
DEFINE FIELD order_status     ON TABLE purchased TYPE option<string>;
DEFINE FIELD discount_percent ON TABLE purchased TYPE option<float>;
DEFINE FIELD tax_rate         ON TABLE purchased TYPE option<float>;
DEFINE FIELD payment_status   ON TABLE purchased TYPE option<string>;

-- customer -> region
REMOVE TABLE IF EXISTS located_in;
DEFINE TABLE located_in SCHEMAFULL TYPE RELATION IN customer OUT region;
DEFINE INDEX unique_located_in ON TABLE located_in COLUMNS in, out UNIQUE;

-- vendor -> region
REMOVE TABLE IF EXISTS based_in;
DEFINE TABLE based_in SCHEMAFULL TYPE RELATION IN vendor OUT region;
DEFINE INDEX unique_based_in ON TABLE based_in COLUMNS in, out UNIQUE;

-- vendor -> invoice (vendor billed us)
REMOVE TABLE IF EXISTS billed;
DEFINE TABLE billed SCHEMAFULL TYPE RELATION IN vendor OUT invoice;
DEFINE INDEX unique_billed ON TABLE billed COLUMNS in, out UNIQUE;

-- invoice -> product (line items)
REMOVE TABLE IF EXISTS invoice_item;
DEFINE TABLE invoice_item SCHEMAFULL TYPE RELATION IN invoice OUT product;
DEFINE FIELD line_number ON TABLE invoice_item TYPE option<int>;
DEFINE FIELD quantity    ON TABLE invoice_item TYPE option<int>;
DEFINE FIELD unit_cost   ON TABLE invoice_item TYPE option<float>;
DEFINE FIELD line_total  ON TABLE invoice_item TYPE option<float>;

-- customer -> product (review edge)
REMOVE TABLE IF EXISTS reviewed;
DEFINE TABLE reviewed SCHEMAFULL TYPE RELATION IN customer OUT product;
DEFINE FIELD review_id        ON TABLE reviewed TYPE string;
DEFINE FIELD rating           ON TABLE reviewed TYPE option<int>;
DEFINE FIELD sentiment        ON TABLE reviewed TYPE option<string>;
DEFINE FIELD verified_purchase ON TABLE reviewed TYPE option<bool>;
DEFINE FIELD response_text     ON TABLE reviewed TYPE option<string>;
DEFINE FIELD response_date     ON TABLE reviewed TYPE option<string>;

-- customer -> support_ticket
REMOVE TABLE IF EXISTS raised;
DEFINE TABLE raised SCHEMAFULL TYPE RELATION IN customer OUT support_ticket;

-- support_ticket -> product
REMOVE TABLE IF EXISTS about;
DEFINE TABLE about SCHEMAFULL TYPE RELATION IN support_ticket OUT product;

-- support_ticket -> agent
REMOVE TABLE IF EXISTS handled_by;
DEFINE TABLE handled_by SCHEMAFULL TYPE RELATION IN support_ticket OUT agent;

-- support_ticket -> call_transcript
REMOVE TABLE IF EXISTS includes_transcript;
DEFINE TABLE includes_transcript SCHEMAFULL TYPE RELATION IN support_ticket OUT call_transcript;

-- call_transcript -> agent
REMOVE TABLE IF EXISTS conducted_by;
DEFINE TABLE conducted_by SCHEMAFULL TYPE RELATION IN call_transcript OUT agent;
DEFINE INDEX unique_reviewed ON TABLE reviewed COLUMNS review_id UNIQUE;

-- product -> product (similar_to based on category)
REMOVE TABLE IF EXISTS similar_to;
DEFINE TABLE similar_to SCHEMAFULL TYPE RELATION IN product OUT product;
DEFINE INDEX unique_similar_to ON TABLE similar_to COLUMNS in, out UNIQUE;
