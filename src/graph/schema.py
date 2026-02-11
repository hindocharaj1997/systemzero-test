"""
SurrealDB Graph Schema Definition.

Defines SCHEMAFULL tables for nodes and edges with typed fields,
constraints, and unique indexes.

Node tables: vendor, product, customer, category, region, invoice
Edge tables: supplies, belongs_to, purchased, located_in, based_in,
             billed, invoice_item, reviewed
"""

# ──────────────────────────────────────────────────────────────────────────────
# Schema DDL
# ──────────────────────────────────────────────────────────────────────────────

from pathlib import Path

SCHEMA_PATH = Path(__file__).parent / "schema.surql"


def get_schema_statements() -> list[str]:
    """
    Parse the DDL string from schema.surql into individual executable statements.

    Uses semicolon-based splitting to safely handle multi-line statements.

    Returns:
        List of SurrealQL statements ready for execution.
    """
    if not SCHEMA_PATH.exists():
        raise FileNotFoundError(f"Schema file not found at {SCHEMA_PATH}")

    with open(SCHEMA_PATH, "r") as f:
        schema_ddl = f.read()

    statements = []
    for raw_stmt in schema_ddl.strip().split(";"):
        # Remove comment lines and blank lines
        lines = [
            l for l in raw_stmt.split("\n")
            if l.strip() and not l.strip().startswith("--")
        ]
        cleaned = " ".join(l.strip() for l in lines).strip()
        if cleaned:
            statements.append(cleaned)
    return statements


# Node table names for iteration
NODE_TABLES = ["vendor", "product", "customer", "category", "region", "invoice", "support_ticket", "call_transcript", "agent"]

# Edge table names for iteration
EDGE_TABLES = ["supplies", "belongs_to", "purchased", "located_in", "based_in",
               "billed", "invoice_item", "reviewed", "similar_to",
               "raised", "about", "handled_by", "includes_transcript", "conducted_by"]
