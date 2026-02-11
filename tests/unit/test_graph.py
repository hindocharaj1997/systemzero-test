"""
Unit tests for the Graph layer.

Tests schema generation, data transforms, and query structure
without requiring a live SurrealDB instance.
"""

import pytest
from pathlib import Path

from src.graph.schema import (
    get_schema_statements,
    SCHEMA_PATH,
    NODE_TABLES,
    EDGE_TABLES,
)
from src.graph.queries import GRAPH_QUERIES, get_query, list_queries


class TestGraphSchema:
    """Tests for SurrealDB schema definitions."""

    def test_schema_ddl_not_empty(self):
        """Schema DDL string should not be empty."""
        assert SCHEMA_PATH.exists()
        with open(SCHEMA_PATH, "r") as f:
            ddl = f.read()
        assert len(ddl.strip()) > 0

    def test_get_schema_statements_returns_list(self):
        """get_schema_statements should return a list of strings."""
        stmts = get_schema_statements()
        assert isinstance(stmts, list)
        assert len(stmts) > 0
        for stmt in stmts:
            assert isinstance(stmt, str)
            assert len(stmt.strip()) > 0

    def test_schema_statements_are_valid_surrealql(self):
        """Each statement should start with a known SurrealQL keyword."""
        valid_prefixes = ("DEFINE TABLE", "DEFINE FIELD", "DEFINE INDEX", "REMOVE TABLE")
        stmts = get_schema_statements()
        for stmt in stmts:
            assert stmt.startswith(valid_prefixes), f"Invalid statement: {stmt[:60]}"

    def test_all_node_tables_defined(self):
        """Schema should define all expected node tables."""
        stmts = get_schema_statements()
        table_defs = [s for s in stmts if s.startswith("DEFINE TABLE")]
        defined_tables = [s.split()[2] for s in table_defs]
        for table in NODE_TABLES:
            assert table in defined_tables, f"Missing node table: {table}"

    def test_all_edge_tables_defined(self):
        """Schema should define all expected edge tables."""
        stmts = get_schema_statements()
        table_defs = [s for s in stmts if s.startswith("DEFINE TABLE")]
        defined_tables = [s.split()[2] for s in table_defs]
        for table in EDGE_TABLES:
            assert table in defined_tables, f"Missing edge table: {table}"

    def test_edge_tables_have_in_out_fields(self):
        """Every edge table should define in/out via TYPE RELATION or explicit FIELD definitions."""
        stmts = get_schema_statements()
        for edge in EDGE_TABLES:
            # Check for TYPE RELATION IN ... OUT ... pattern (preferred SurrealDB idiom)
            has_relation_type = any(
                f"DEFINE TABLE {edge}" in s and "TYPE RELATION" in s and "IN " in s and "OUT " in s
                for s in stmts
            )
            # Fallback: check for explicit DEFINE FIELD in/out
            has_explicit_in = any(
                "DEFINE FIELD in" in s and f"ON TABLE {edge}" in s for s in stmts
            )
            has_explicit_out = any(
                "DEFINE FIELD out" in s and f"ON TABLE {edge}" in s for s in stmts
            )
            assert has_relation_type or (has_explicit_in and has_explicit_out), (
                f"Edge {edge} missing in/out definition (no TYPE RELATION or explicit fields)"
            )

    def test_all_tables_are_schemafull(self):
        """All tables should be SCHEMAFULL."""
        stmts = get_schema_statements()
        table_defs = [s for s in stmts if s.startswith("DEFINE TABLE")]
        for s in table_defs:
            assert "SCHEMAFULL" in s, f"Table not SCHEMAFULL: {s}"

    def test_node_table_count(self):
        """Should have exactly 9 node tables."""
        assert len(NODE_TABLES) == 9

    def test_edge_table_count(self):
        """Should have exactly 14 edge tables."""
        assert len(EDGE_TABLES) == 14


class TestGraphQueries:
    """Tests for SurrealDB graph queries."""

    def test_all_queries_have_description(self):
        """Every query should include a description."""
        for name, q in GRAPH_QUERIES.items():
            assert "description" in q, f"Query {name} missing description"
            assert len(q["description"]) > 10, f"Query {name} has empty description"

    def test_all_queries_have_query_string(self):
        """Every query should include a query string."""
        for name, q in GRAPH_QUERIES.items():
            assert "query" in q, f"Query {name} missing query"
            assert len(q["query"].strip()) > 10, f"Query {name} has empty query"

    def test_query_count(self):
        """Should have at least 8 sample queries."""
        assert len(GRAPH_QUERIES) >= 8

    def test_get_query_returns_string(self):
        """get_query should return a trimmed query string."""
        for name in list_queries():
            q = get_query(name)
            assert isinstance(q, str)
            assert len(q) > 0
            # Should be trimmed
            assert q == q.strip()

    def test_get_query_raises_on_invalid_name(self):
        """get_query should raise KeyError for unknown queries."""
        with pytest.raises(KeyError):
            get_query("nonexistent_query_name")

    def test_list_queries_matches_dict(self):
        """list_queries should return all query names."""
        names = list_queries()
        assert set(names) == set(GRAPH_QUERIES.keys())

    def test_queries_contain_graph_traversal_syntax(self):
        """Queries should use SurrealDB graph traversal syntax (-> or <-)."""
        traversal_queries = [
            "reliable_vendor_products",
            "customer_purchase_history",
            "co_purchased_products",
            "top_customers_by_vendor",
            "vendor_influence",
            "invoice_reconciliation",
            "overdue_vendors",
            "vendor_payment_vs_sales",
        ]
        for name in traversal_queries:
            q = get_query(name)
            has_traversal = "->" in q or "<-" in q
            assert has_traversal, f"Query {name} missing graph traversal syntax"

    def test_required_queries_present(self):
        """Should have all the queries required by the README."""
        required = [
            "reliable_vendor_products",
            "customer_purchase_history",
            "vendor_influence",
            "invoice_reconciliation",
            "overdue_vendors",
            "vendor_payment_vs_sales",
        ]
        available = list_queries()
        for name in required:
            assert name in available, f"Required query missing: {name}"


class TestGraphLoaderTransforms:
    """Tests for data transformation logic (no live SurrealDB needed)."""

    def test_safe_float_valid(self):
        from src.graph.loader import GraphLoader
        assert GraphLoader._safe_float("3.14") == 3.14
        assert GraphLoader._safe_float("100") == 100.0

    def test_safe_float_invalid(self):
        from src.graph.loader import GraphLoader
        assert GraphLoader._safe_float("") is None
        assert GraphLoader._safe_float("null") is None
        assert GraphLoader._safe_float("abc") is None
        assert GraphLoader._safe_float(None) is None

    def test_safe_int_valid(self):
        from src.graph.loader import GraphLoader
        assert GraphLoader._safe_int("42") == 42
        assert GraphLoader._safe_int("100.0") == 100  # handles float strings

    def test_safe_int_invalid(self):
        from src.graph.loader import GraphLoader
        assert GraphLoader._safe_int("") is None
        assert GraphLoader._safe_int("null") is None
        assert GraphLoader._safe_int("abc") is None

    def test_safe_bool_true(self):
        from src.graph.loader import GraphLoader
        assert GraphLoader._safe_bool("true") is True
        assert GraphLoader._safe_bool("1") is True
        assert GraphLoader._safe_bool("yes") is True

    def test_safe_bool_false(self):
        from src.graph.loader import GraphLoader
        assert GraphLoader._safe_bool("false") is False
        assert GraphLoader._safe_bool("0") is False
        assert GraphLoader._safe_bool("no") is False

    def test_safe_bool_none(self):
        from src.graph.loader import GraphLoader
        assert GraphLoader._safe_bool("") is None
        assert GraphLoader._safe_bool("null") is None

    def test_safe_str(self):
        from src.graph.loader import GraphLoader
        assert GraphLoader._safe_str("hello") == "hello"
        assert GraphLoader._safe_str("  trimmed  ") == "trimmed"
        assert GraphLoader._safe_str("") is None
        assert GraphLoader._safe_str("null") is None
        assert GraphLoader._safe_str(None) is None

    def test_sanitize_id_strips_whitespace(self):
        from src.graph.loader import GraphLoader
        assert GraphLoader._sanitize_id("  VND-001  ") == "VND-001"

    def test_sanitize_id_removes_backticks(self):
        from src.graph.loader import GraphLoader
        result = GraphLoader._sanitize_id("VND`001")
        assert "`" not in result
        assert result == "VND001"

    def test_sanitize_id_removes_control_chars(self):
        from src.graph.loader import GraphLoader
        assert GraphLoader._sanitize_id("VND-001\x00") == "VND-001"
        assert GraphLoader._sanitize_id("VND-001\x01\x02") == "VND-001"
