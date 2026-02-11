"""
Sample SurrealDB Graph Queries.

These queries demonstrate the graph traversal capabilities for
the e-commerce GraphRAG use case. Each query is documented with
the business question it answers and the graph traversal path.
"""

from typing import Dict


# Named collection of sample queries
GRAPH_QUERIES: Dict[str, Dict[str, str]] = {
    # ─────────────────────────────────────────────────────────────────
    # 1. Products from reliable vendors
    # ─────────────────────────────────────────────────────────────────
    "reliable_vendor_products": {
        "description": (
            "Find all products supplied by vendors with a reliability "
            "score above 90. This traverses vendor->supplies->product."
        ),
        "query": """
            SELECT
                id,
                vendor_name,
                reliability_score,
                ->supplies->product.{product_name, price, category, rating} AS products
            FROM vendor
            WHERE reliability_score > 90
            ORDER BY reliability_score DESC;
        """,
    },

    # ─────────────────────────────────────────────────────────────────
    # 2. Customer purchase history with vendor details
    # ─────────────────────────────────────────────────────────────────
    "customer_purchase_history": {
        "description": (
            "Get a customer's complete purchase history including the "
            "product details and the vendor who supplies each product. "
            "Path: customer->purchased->product<-supplies<-vendor."
        ),
        "query": """
            SELECT
                id,
                full_name,
                ->purchased->product.{
                    product_name,
                    price,
                    category,
                    vendor: <-supplies<-vendor.vendor_name
                } AS purchases
            FROM customer
            LIMIT 10;
        """,
    },

    # ─────────────────────────────────────────────────────────────────
    # 3. Related products (same category + same vendor + co-purchased)
    # ─────────────────────────────────────────────────────────────────
    "related_products": {
        "description": (
            "Find products related to a given product through three "
            "relationships: (a) same category, (b) same vendor, "
            "(c) co-purchased by the same customers. "
            "Path: product->belongs_to->category<-belongs_to<-product "
            "and product<-supplies<-vendor->supplies->product "
            "and product<-purchased<-customer->purchased->product."
        ),
        "query": """
            -- Same category
            SELECT
                id AS source_product,
                product_name,
                ->belongs_to->category<-belongs_to<-product.product_name AS same_category_products
            FROM product
            LIMIT 5;
        """,
    },

    "co_purchased_products": {
        "description": (
            "Find products frequently bought by the same customers who "
            "purchased a given product. "
            "Path: product<-purchased<-customer->purchased->product."
        ),
        "query": """
            SELECT
                id AS source_product,
                product_name,
                <-purchased<-customer->purchased->product.product_name AS also_bought
            FROM product
            LIMIT 5;
        """,
    },

    # ─────────────────────────────────────────────────────────────────
    # 4. Top customers by vendor
    # ─────────────────────────────────────────────────────────────────
    "top_customers_by_vendor": {
        "description": (
            "For each vendor, find the top customers by total spend on "
            "that vendor's products. "
            "Path: vendor->supplies->product<-purchased<-customer."
        ),
        "query": """
            SELECT
                id,
                vendor_name,
                ->supplies->product<-purchased<-customer.full_name AS customers
            FROM vendor;
        """,
    },

    # ─────────────────────────────────────────────────────────────────
    # 5. Vendor influence score (reach)
    # ─────────────────────────────────────────────────────────────────
    "vendor_influence": {
        "description": (
            "Count the distinct customers reachable from each vendor — "
            "i.e. customers who have purchased at least one product "
            "supplied by that vendor. "
            "Path: vendor->supplies->product<-purchased<-customer."
        ),
        "query": """
            SELECT
                id,
                vendor_name,
                count(->supplies->product<-purchased<-customer) AS customer_reach,
                count(->supplies->product) AS product_count
            FROM vendor;
        """,
    },

    # ─────────────────────────────────────────────────────────────────
    # 6. Invoice reconciliation
    # ─────────────────────────────────────────────────────────────────
    "invoice_reconciliation": {
        "description": (
            "For each invoice, sum the line item totals and compare "
            "against the invoice total_amount to detect discrepancies. "
            "Path: invoice->invoice_item->product."
        ),
        "query": """
            SELECT
                id,
                total_amount AS invoice_total,
                ->invoice_item.{
                    product: out.product_name,
                    quantity,
                    line_total
                } AS line_items,
                math::sum(->invoice_item.line_total) AS calculated_total
            FROM invoice
            LIMIT 10;
        """,
    },

    # ─────────────────────────────────────────────────────────────────
    # 7. Overdue vendors and affected products
    # ─────────────────────────────────────────────────────────────────
    "overdue_vendors": {
        "description": (
            "Find vendors with overdue invoices and list the products "
            "they supply. This helps identify supply chain risk. "
            "Path: vendor->billed->invoice (filter overdue), then "
            "vendor->supplies->product."
        ),
        "query": """
            SELECT
                id,
                vendor_name,
                ->billed->invoice[WHERE payment_status != 'paid'] AS overdue_invoices,
                ->supplies->product.{product_name, stock_quantity} AS supplied_products
            FROM vendor
            WHERE count(->billed->invoice[WHERE payment_status != 'paid']) > 0;
        """,
    },

    # ─────────────────────────────────────────────────────────────────
    # 8. Vendor payment vs. sales correlation
    # ─────────────────────────────────────────────────────────────────
    "vendor_payment_vs_sales": {
        "description": (
            "Compare what we owe each vendor (invoice totals) against "
            "what we earn from their products (transaction totals). "
            "Paths: vendor->billed->invoice (cost side) and "
            "vendor->supplies->product<-purchased (revenue side)."
        ),
        "query": """
            SELECT
                id,
                vendor_name,
                math::sum(->billed->invoice.total_amount) AS total_invoiced,
                math::sum(->supplies->product<-purchased.total_amount) AS total_revenue
            FROM vendor;
        """,
    },

    # ─────────────────────────────────────────────────────────────────
    # 9. Products with high support burden
    # ─────────────────────────────────────────────────────────────────
    "product_support_burden": {
        "description": (
            "Identify products that generate the most support tickets. "
            "Useful for identifying quality issues. "
            "Path: product<-about<-support_ticket."
        ),
        "query": """
            SELECT
                id,
                product_name,
                count(<-about<-support_ticket) AS ticket_count,
                math::mean(<-about<-support_ticket.satisfaction_score) AS avg_satisfaction
            FROM product
            ORDER BY ticket_count DESC
            LIMIT 10;
        """,
    },

    # ─────────────────────────────────────────────────────────────────
    # 10. Agent performance (Ticket Satisfaction & Call Sentiment)
    # ─────────────────────────────────────────────────────────────────
    "agent_performance": {
        "description": (
            "Analyze agent performance by combining ticket satisfaction "
            "and call transcript sentiment. "
            "Path: agent<-handled_by<-support_ticket and "
            "agent<-conducted_by<-call_transcript."
        ),
        "query": """
            SELECT
                id,
                name,
                count(<-handled_by<-support_ticket) AS tickets_handled,
                math::mean(<-handled_by<-support_ticket.satisfaction_score) AS avg_ticket_sat,
                count(<-conducted_by<-call_transcript) AS calls_conducted,
                math::mean(<-conducted_by<-call_transcript.quality_score) AS avg_call_quality
            FROM agent
            ORDER BY tickets_handled DESC;
        """,
    },

    # ─────────────────────────────────────────────────────────────────
    # 11. Customer Support Journey
    # ─────────────────────────────────────────────────────────────────
    "customer_support_journey": {
        "description": (
            "Trace a customer''s interactions: Purchases -> Tickets -> Calls. "
            "Path: customer->raised->ticket->includes_transcript->call."
        ),
        "query": """
            SELECT
                id,
                full_name,
                ->raised->support_ticket.{
                    ticket_id: id,
                    status,
                    priority,
                    about_product: ->about->product.product_name,
                    calls: ->includes_transcript->call_transcript.{
                        duration_seconds,
                        sentiment_overall
                    }
                } AS history
            FROM customer
            LIMIT 5;
        """,
    },
}


def get_query(name: str) -> str:
    """
    Get a named query string.

    Args:
        name: Query identifier (key in GRAPH_QUERIES).

    Returns:
        The SurrealQL query string.

    Raises:
        KeyError: If the query name is not found.
    """
    return GRAPH_QUERIES[name]["query"].strip()


def list_queries() -> list[str]:
    """List all available query names."""
    return list(GRAPH_QUERIES.keys())
