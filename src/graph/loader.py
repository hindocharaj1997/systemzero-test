"""
SurrealDB Graph Loader.

Loads Silver-layer data into SurrealDB as graph nodes and edges.
Uses the async SurrealDB Python SDK with individual record operations.

Usage:
    loader = GraphLoader(silver_dir, config)
    await loader.load_all()
    loader.close()
"""

import asyncio
import csv
from pathlib import Path
from typing import Dict, Any, List, Optional
from dataclasses import dataclass, field

from loguru import logger

try:
    from surrealdb import AsyncSurreal
except ImportError:
    AsyncSurreal = None  # Graceful degradation if not installed

from .schema import get_schema_statements, NODE_TABLES, EDGE_TABLES


@dataclass
class LoadResult:
    """Result of loading a single table."""
    table: str
    table_type: str  # "node" or "edge"
    records_loaded: int = 0
    errors: int = 0


class GraphLoader:
    """
    Loads Silver data into SurrealDB as a graph.

    Loading order (respects dependencies):
    1. Schema DDL (define all tables)
    2. Derived nodes: categories, regions
    3. Core nodes: vendors, customers
    4. Dependent nodes: products, invoices
    5. Edges: supplies, belongs_to, based_in, located_in
    6. Edges: purchased, billed, invoice_item, reviewed
    """

    def __init__(
        self,
        silver_dir: Path,
        config: Dict[str, Any],
    ):
        """
        Initialize the graph loader.

        Args:
            silver_dir: Path to Silver layer CSV files.
            config: SurrealDB connection config with keys:
                     url, namespace, database, username, password
        """
        if AsyncSurreal is None:
            raise ImportError(
                "surrealdb package not installed. "
                "Install with: pip install surrealdb>=0.3.0"
            )

        self.silver_dir = Path(silver_dir)
        self.config = config
        self.db: Optional[AsyncSurreal] = None
        self.logger = logger.bind(component="GraphLoader")

    async def connect(self) -> None:
        """Establish connection to SurrealDB."""
        url = self.config.get("url", "ws://localhost:8000/rpc")
        ns = self.config.get("namespace", "test")
        db_name = self.config.get("database", "ecommerce")
        username = self.config.get("username", "root")
        password = self.config.get("password", "root")

        self.db = AsyncSurreal(url)
        await self.db.connect()
        await self.db.signin({"username": username, "password": password})
        await self.db.use(ns, db_name)
        self.logger.info(f"Connected to SurrealDB: {url} ({ns}/{db_name})")

    async def close(self) -> None:
        """Close SurrealDB connection."""
        if self.db:
            await self.db.close()
            self.logger.debug("SurrealDB connection closed")

    async def apply_schema(self) -> None:
        """Apply the graph schema DDL to SurrealDB."""
        statements = get_schema_statements()
        self.logger.info(f"Applying schema: {len(statements)} statements")
        for stmt in statements:
            try:
                await self.db.query(stmt)
            except Exception as e:
                self.logger.warning(f"Schema statement failed: {stmt[:60]}... — {e}")
        self.logger.info("Schema applied successfully")

    async def load_all(self) -> Dict[str, LoadResult]:
        """
        Load all Silver data into SurrealDB.

        Returns:
            Dictionary of table name → LoadResult.
        """
        self.logger.info("=" * 60)
        self.logger.info("GRAPH LAYER: Loading into SurrealDB")
        self.logger.info("=" * 60)

        await self.connect()
        await self.apply_schema()

        results: Dict[str, LoadResult] = {}

        # Phase 1: Derived nodes (categories, regions, agents)
        results["category"] = await self._load_categories()
        results["region"] = await self._load_regions()
        results["agent"] = await self._load_agents()

        # Phase 2: Core nodes
        results["vendor"] = await self._load_nodes_from_csv(
            "vendors.csv", "vendor", "vendor_id", self._vendor_transform
        )
        results["customer"] = await self._load_nodes_from_csv(
            "customers.csv", "customer", "customer_id", self._customer_transform
        )

        # Phase 3: Dependent nodes
        results["product"] = await self._load_nodes_from_csv(
            "products.csv", "product", "product_id", self._product_transform
        )
        results["invoice"] = await self._load_nodes_from_csv(
            "invoices.csv", "invoice", "invoice_id", self._invoice_transform
        )
        results["support_ticket"] = await self._load_nodes_from_csv(
            "support_tickets.csv", "support_ticket", "ticket_id", self._ticket_transform
        )
        results["call_transcript"] = await self._load_nodes_from_csv(
            "call_transcripts.csv", "call_transcript", "call_id", self._call_transform
        )

        # Phase 4: Edges
        results["supplies"] = await self._load_supplies_edges()
        results["belongs_to"] = await self._load_belongs_to_edges()
        results["based_in"] = await self._load_based_in_edges()
        results["located_in"] = await self._load_located_in_edges()
        results["purchased"] = await self._load_purchased_edges()
        results["billed"] = await self._load_billed_edges()
        results["invoice_item"] = await self._load_invoice_item_edges()
        results["reviewed"] = await self._load_reviewed_edges()
        results["similar_to"] = await self._load_similar_to_edges()
        
        # Phase 5: Support Edges
        results["raised"] = await self._load_raised_edges()
        results["about"] = await self._load_about_edges()
        results["handled_by"] = await self._load_handled_by_edges()
        results["includes_transcript"] = await self._load_includes_transcript_edges()
        results["conducted_by"] = await self._load_conducted_by_edges()

        # Summary
        total_nodes = sum(r.records_loaded for r in results.values() if r.table_type == "node")
        total_edges = sum(r.records_loaded for r in results.values() if r.table_type == "edge")
        self.logger.info(f"Graph complete: {total_nodes} nodes, {total_edges} edges")

        return results

    # ──────────────────────────────────────────────────────────────────────
    # CSV helpers
    # ──────────────────────────────────────────────────────────────────────

    def _read_csv(self, filename: str) -> List[Dict[str, str]]:
        """Read a Silver CSV file into a list of dicts."""
        path = self.silver_dir / filename
        if not path.exists():
            self.logger.warning(f"Silver file not found: {path}")
            return []
        with open(path, newline="", encoding="utf-8") as f:
            return list(csv.DictReader(f))

    @staticmethod
    def _safe_float(val: str) -> Optional[float]:
        """Convert string to float, returning None on failure."""
        if val is None or val == "" or val == "null":
            return None
        try:
            return float(val)
        except (ValueError, TypeError):
            return None

    @staticmethod
    def _safe_int(val: str) -> Optional[int]:
        """Convert string to int, returning None on failure."""
        if val is None or val == "" or val == "null":
            return None
        try:
            return int(float(val))  # handle "100.0" → 100
        except (ValueError, TypeError):
            return None

    @staticmethod
    def _safe_bool(val: str) -> Optional[bool]:
        """Convert string to bool, returning None on failure."""
        if val is None or val == "" or val == "null":
            return None
        return str(val).lower() in ("true", "1", "yes")

    @staticmethod
    def _safe_str(val: str) -> Optional[str]:
        """Return None for empty/null strings."""
        if val is None or val.strip() == "" or val == "null":
            return None
        return val.strip()

    @staticmethod
    def _sanitize_id(raw_id: str) -> str:
        """
        Sanitize a record ID for use as a SurrealDB record key.

        Removes backticks (which would break backtick-quoted IDs)
        and control characters. Whitespace is trimmed.
        """
        cleaned = raw_id.strip()
        # Remove backticks that would break `id` quoting
        cleaned = cleaned.replace("`", "")
        # Remove null bytes and control characters
        cleaned = "".join(c for c in cleaned if c.isprintable())
        return cleaned

    # ──────────────────────────────────────────────────────────────────────
    # Transform functions (CSV row → SurrealDB fields dict)
    # ──────────────────────────────────────────────────────────────────────

    def _vendor_transform(self, row: Dict[str, str]) -> Dict[str, Any]:
        return {
            "vendor_name": row.get("vendor_name", ""),
            "country": self._safe_str(row.get("country")),
            "region": self._safe_str(row.get("region")),
            "reliability_score": self._safe_float(row.get("reliability_score")),
            "status": self._safe_str(row.get("status")),
            "source_file": self._safe_str(row.get("source_file")),
            "loaded_at": self._safe_str(row.get("loaded_at")),
        }

    def _product_transform(self, row: Dict[str, str]) -> Dict[str, Any]:
        return {
            "product_name": row.get("product_name", ""),
            "sku": row.get("sku", ""),
            "category": self._safe_str(row.get("category")),
            "price": self._safe_float(row.get("price")),
            "cost": self._safe_float(row.get("cost")),
            "stock_quantity": self._safe_int(row.get("stock_quantity")),
            "rating": self._safe_float(row.get("rating")),
            "is_active": self._safe_bool(row.get("is_active")),
            "source_file": self._safe_str(row.get("source_file")),
            "loaded_at": self._safe_str(row.get("loaded_at")),
        }

    def _customer_transform(self, row: Dict[str, str]) -> Dict[str, Any]:
        return {
            "full_name": row.get("full_name", ""),
            "email": self._safe_str(row.get("email")),
            "phone": self._safe_str(row.get("phone")),
            "segment": self._safe_str(row.get("segment")),
            "total_spend": self._safe_float(row.get("total_spend")),
            "registration_date": self._safe_str(row.get("registration_date")),
            "last_purchase_date": self._safe_str(row.get("last_purchase_date")),
            "is_active": self._safe_bool(row.get("is_active")),
            "source_file": self._safe_str(row.get("source_file")),
            "loaded_at": self._safe_str(row.get("loaded_at")),
        }

    def _invoice_transform(self, row: Dict[str, str]) -> Dict[str, Any]:
        return {
            "invoice_date": self._safe_str(row.get("invoice_date")),
            "due_date": self._safe_str(row.get("due_date")),
            "payment_date": self._safe_str(row.get("payment_date")),
            "total_amount": self._safe_float(row.get("total_amount")),
            "payment_status": self._safe_str(row.get("payment_status")),
            "payment_terms": self._safe_str(row.get("payment_terms")),
            "source_file": self._safe_str(row.get("source_file")),
            "loaded_at": self._safe_str(row.get("loaded_at")),
        }

    # ──────────────────────────────────────────────────────────────────────
    # Node loaders
    # ──────────────────────────────────────────────────────────────────────

    async def _load_nodes_from_csv(
        self,
        csv_filename: str,
        table: str,
        id_column: str,
        transform_fn,
    ) -> LoadResult:
        """Load nodes from a Silver CSV file."""
        result = LoadResult(table=table, table_type="node")
        rows = self._read_csv(csv_filename)

        for row in rows:
            record_id = row.get(id_column, "")
            if not record_id:
                result.errors += 1
                continue

            data = transform_fn(row)
            try:
                # Use backtick ID format to match _relate
                clean_id = self._sanitize_id(record_id)
                record_str = f"{table}:`{clean_id}`"
                await self.db.query(
                    f"CREATE {record_str} CONTENT $data",
                    {"data": data},
                )
                result.records_loaded += 1
            except Exception as e:
                result.errors += 1
                self.logger.debug(f"Failed to create {table}:{record_id}: {e}")

        self.logger.info(
            f"✓ {table}: {result.records_loaded} nodes"
            + (f" ({result.errors} errors)" if result.errors else "")
        )
        return result

    async def _load_categories(self) -> LoadResult:
        """Load derived category nodes from product categories."""
        result = LoadResult(table="category", table_type="node")
        rows = self._read_csv("products.csv")

        categories = set()
        for row in rows:
            cat = self._safe_str(row.get("category"))
            if cat:
                categories.add(cat)

        for cat in sorted(categories):
            cat_id = cat.replace(" ", "_").replace("&", "and")
            try:
                # Use backtick ID format
                record_str = f"category:`{cat_id}`"
                await self.db.query(
                    f"CREATE {record_str} CONTENT $data",
                    {"data": {"name": cat}},
                )
                result.records_loaded += 1
            except Exception as e:
                result.errors += 1
                self.logger.debug(f"Failed to create category:{cat_id}: {e}")

        self.logger.info(f"✓ category: {result.records_loaded} nodes (derived)")
        return result

    async def _load_regions(self) -> LoadResult:
        """Load derived region nodes from vendor regions."""
        result = LoadResult(table="region", table_type="node")
        rows = self._read_csv("vendors.csv")

        regions = set()
        for row in rows:
            region = self._safe_str(row.get("region"))
            if region:
                regions.add(region)

        for region in sorted(regions):
            region_id = region.replace(" ", "_").replace("&", "and")
            try:
                # Use backtick ID format
                record_str = f"region:`{region_id}`"
                await self.db.query(
                    f"CREATE {record_str} CONTENT $data",
                    {"data": {"name": region}},
                )
                result.records_loaded += 1
            except Exception as e:
                result.errors += 1
                self.logger.debug(f"Failed to create region:{region_id}: {e}")

        self.logger.info(f"✓ region: {result.records_loaded} nodes (derived)")
        return result

    # ──────────────────────────────────────────────────────────────────────
    # Edge loaders
    # ──────────────────────────────────────────────────────────────────────

    async def _relate(
        self,
        from_table: str,
        from_id: str,
        edge_table: str,
        to_table: str,
        to_id: str,
        data: Optional[Dict[str, Any]] = None,
    ) -> bool:
        """
        Create an edge between two nodes.

        Args:
            from_table: Source node table
            from_id: Source node ID (raw)
            edge_table: Edge table name
            to_table: Target node table
            to_id: Target node ID (raw)
            data: Optional edge properties

        Returns:
            True if successful, False otherwise
        """
        try:
            # Sanitize IDs
            clean_from = self._sanitize_id(from_id)
            clean_to = self._sanitize_id(to_id)
            
            # Construct record IDs formatted as table:`id`
            # We MUST use backticks around the ID part if it contains special chars like '-'
            # Example: vendor:`VND-003`
            from_record = f"{from_table}:`{clean_from}`"
            to_record = f"{to_table}:`{clean_to}`"

            query = f"RELATE {from_record}->{edge_table}->{to_record}"
            vars = {}

            if data:
                clean_data = {k: v for k, v in data.items() if v is not None}
                if clean_data:
                    query += " CONTENT $data"
                    vars["data"] = clean_data
            
            response = await self.db.query(query, vars)
            
            # Debug: Check for silent errors in response
            if isinstance(response, list) and response:
                first_res = response[0]
                if isinstance(first_res, dict) and first_res.get("status") == "ERR":
                     self.logger.error(f"RELATE Error: {first_res} | Query: {query}")
                     return False
            
            return True
        except Exception as e:
            self.logger.debug(
                f"RELATE {from_table}:{from_id}->{edge_table}->{to_table}:{to_id} failed: {e}"
            )
            return False

    async def _load_supplies_edges(self) -> LoadResult:
        """vendor -> product (from products.vendor_id)."""
        result = LoadResult(table="supplies", table_type="edge")
        rows = self._read_csv("products.csv")

        for row in rows:
            vendor_id = self._safe_str(row.get("vendor_id"))
            product_id = self._safe_str(row.get("product_id"))
            if vendor_id and product_id:
                ok = await self._relate("vendor", vendor_id, "supplies", "product", product_id)
                if ok:
                    result.records_loaded += 1
                else:
                    result.errors += 1

        self.logger.info(f"✓ supplies: {result.records_loaded} edges")
        return result

    async def _load_belongs_to_edges(self) -> LoadResult:
        """product -> category (from products.category)."""
        result = LoadResult(table="belongs_to", table_type="edge")
        rows = self._read_csv("products.csv")

        for row in rows:
            product_id = self._safe_str(row.get("product_id"))
            category = self._safe_str(row.get("category"))
            if product_id and category:
                cat_id = category.replace(" ", "_").replace("&", "and")
                ok = await self._relate("product", product_id, "belongs_to", "category", cat_id)
                if ok:
                    result.records_loaded += 1
                else:
                    result.errors += 1

        self.logger.info(f"✓ belongs_to: {result.records_loaded} edges")
        return result

    async def _load_based_in_edges(self) -> LoadResult:
        """vendor -> region."""
        result = LoadResult(table="based_in", table_type="edge")
        rows = self._read_csv("vendors.csv")

        for row in rows:
            vendor_id = self._safe_str(row.get("vendor_id"))
            region = self._safe_str(row.get("region"))
            if vendor_id and region:
                region_id = region.replace(" ", "_").replace("&", "and")
                ok = await self._relate("vendor", vendor_id, "based_in", "region", region_id)
                if ok:
                    result.records_loaded += 1
                else:
                    result.errors += 1

        self.logger.info(f"✓ based_in: {result.records_loaded} edges")
        return result

    async def _load_located_in_edges(self) -> LoadResult:
        """customer -> region (from customer address data if available)."""
        result = LoadResult(table="located_in", table_type="edge")
        rows = self._read_csv("customers.csv")

        if not rows:
            self.logger.info("⊘ located_in: skipped (no customer data)")
            return result

        # Check if address-derived columns exist
        sample = rows[0]
        region_col = None
        for candidate in ["region", "address_region", "address_state", "state", "address_country", "country"]:
            if candidate in sample:
                region_col = candidate
                break

        if region_col is None:
            self.logger.info(
                "⊘ located_in: skipped (no region/address column in Silver customers). "
                "In production, geocode from address fields."
            )
            return result

        # Build country -> region map from vendors
        region_rows = self._read_csv("vendors.csv")
        country_to_region = {}
        for r in region_rows:
            country = self._safe_str(r.get("country"))
            region = self._safe_str(r.get("region"))
            if country and region:
                country_to_region[country] = region

        for row in rows:
            customer_id = self._safe_str(row.get("customer_id"))
            # improved detection: check country first as it maps to region
            country = self._safe_str(row.get("address_country")) or self._safe_str(row.get("country"))
            
            # Lookup region from country
            region_name = country_to_region.get(country) if country else None
            
            # Fallback: if data already has region column (unlikely but possible)
            if not region_name:
                 region_name = self._safe_str(row.get("region"))

            if customer_id and region_name:
                region_id = region_name.replace(" ", "_").replace("&", "and")
                ok = await self._relate(
                    "customer", customer_id, "located_in", "region", region_id
                )
                if ok:
                    result.records_loaded += 1
                else:
                    result.errors += 1

        self.logger.info(f"✓ located_in: {result.records_loaded} edges")
        return result

    async def _load_purchased_edges(self) -> LoadResult:
        """customer -> product (from transactions with edge properties)."""
        result = LoadResult(table="purchased", table_type="edge")
        rows = self._read_csv("transactions.csv")

        for row in rows:
            customer_id = self._safe_str(row.get("customer_id"))
            product_id = self._safe_str(row.get("product_id"))
            txn_id = self._safe_str(row.get("transaction_id"))
            if customer_id and product_id and txn_id:
                data = {
                    "transaction_id": txn_id,
                    "quantity": self._safe_int(row.get("quantity")),
                    "total_amount": self._safe_float(row.get("total_amount")),
                    "transaction_date": self._safe_str(row.get("transaction_date")),
                    "order_status": self._safe_str(row.get("order_status")),
                    "discount_percent": self._safe_float(row.get("discount_percent")),
                    "tax_rate": self._safe_float(row.get("tax_rate")),
                    "payment_status": self._safe_str(row.get("payment_status")),
                }
                ok = await self._relate(
                    "customer", customer_id, "purchased", "product", product_id, data
                )
                if ok:
                    result.records_loaded += 1
                else:
                    result.errors += 1

        self.logger.info(f"✓ purchased: {result.records_loaded} edges")
        return result

    async def _load_billed_edges(self) -> LoadResult:
        """vendor -> invoice (from invoices.vendor_id)."""
        result = LoadResult(table="billed", table_type="edge")
        rows = self._read_csv("invoices.csv")

        for row in rows:
            vendor_id = self._safe_str(row.get("vendor_id"))
            invoice_id = self._safe_str(row.get("invoice_id"))
            if vendor_id and invoice_id:
                ok = await self._relate("vendor", vendor_id, "billed", "invoice", invoice_id)
                if ok:
                    result.records_loaded += 1
                else:
                    result.errors += 1

        self.logger.info(f"✓ billed: {result.records_loaded} edges")
        return result

    async def _load_invoice_item_edges(self) -> LoadResult:
        """invoice -> product (from invoice_line_items)."""
        result = LoadResult(table="invoice_item", table_type="edge")
        rows = self._read_csv("invoice_line_items.csv")

        for row in rows:
            invoice_id = self._safe_str(row.get("invoice_id"))
            product_id = self._safe_str(row.get("product_id"))
            if invoice_id and product_id:
                data = {
                    "line_number": self._safe_int(row.get("line_number")),
                    "quantity": self._safe_int(row.get("quantity")),
                    "unit_cost": self._safe_float(row.get("unit_cost")),
                    "line_total": self._safe_float(row.get("line_total")),
                }
                ok = await self._relate(
                    "invoice", invoice_id, "invoice_item", "product", product_id, data
                )
                if ok:
                    result.records_loaded += 1
                else:
                    result.errors += 1

        self.logger.info(f"✓ invoice_item: {result.records_loaded} edges")
        return result

    async def _load_reviewed_edges(self) -> LoadResult:
        """customer -> product (from reviews)."""
        result = LoadResult(table="reviewed", table_type="edge")
        rows = self._read_csv("reviews.csv")

        for row in rows:
            customer_id = self._safe_str(row.get("customer_id"))
            product_id = self._safe_str(row.get("product_id"))
            review_id = self._safe_str(row.get("review_id"))
            if customer_id and product_id and review_id:
                data = {
                    "review_id": review_id,
                    "rating": self._safe_int(row.get("rating")),
                    "sentiment": self._safe_str(row.get("sentiment")),
                    "verified_purchase": self._safe_bool(row.get("verified_purchase")),
                    "response_text": self._safe_str(row.get("response_response_text")),
                    "response_date": self._safe_str(row.get("response_response_date")),
                }
                ok = await self._relate(
                    "customer", customer_id, "reviewed", "product", product_id, data
                )
                if ok:
                    result.records_loaded += 1
                else:
                    result.errors += 1

        self.logger.info(f"✓ reviewed: {result.records_loaded} edges")
        return result

    async def _load_similar_to_edges(self) -> LoadResult:
        """product -> product (similar_to based on category)."""
        result = LoadResult(table="similar_to", table_type="edge")
        rows = self._read_csv("products.csv")

        # Group products by category
        from collections import defaultdict
        products_by_category = defaultdict(list)
        for row in rows:
            product_id = self._safe_str(row.get("product_id"))
            category = self._safe_str(row.get("category"))
            if product_id and category:
                products_by_category[category].append(product_id)

        # Create edges between products in the same category
        # To avoid explosion (N^2), we link each product to the NEXT product in the same category (ring topology)
        # OR we link to all others.
        # Strategy: Link to up to 5 other products in the same category to keep graph dense but manageable.
        
        for category, product_ids in products_by_category.items():
            # If too many products, just link a few neighbors. 
            # Simple approach: A->B, B->C, C->D, D->A (ring) + A->C (strided).
            # The prompt asks for "Similar To" which implies a cluster.
            # Let's do full mesh but key is limited by category size. 
            # If category > 20 items, this is too big.
            # Let's do a windowed approach: each product connects to next 3 neighbors.
            
            n = len(product_ids)
            if n < 2:
                continue
                
            sorted_pids = sorted(product_ids)
            
            for i in range(n):
                p1 = sorted_pids[i]
                # Connect to next 3 neighbors (wrapping around)
                # This ensures connectivity and "similarity" without N^2
                neighbors_count = min(n - 1, 3) 
                
                for j in range(1, neighbors_count + 1):
                    neighbor_idx = (i + j) % n
                    p2 = sorted_pids[neighbor_idx]
                    
                    if p1 == p2: continue
                    
                    ok = await self._relate("product", p1, "similar_to", "product", p2)
                    if ok:
                        result.records_loaded += 1
                    else:
                        result.errors += 1

        self.logger.info(f"✓ similar_to: {result.records_loaded} edges")
        return result

    # ──────────────────────────────────────────────────────────────────────
    # Support domain loaders
    # ──────────────────────────────────────────────────────────────────────

    def _ticket_transform(self, row: Dict[str, str]) -> Dict[str, Any]:
        return {
            "status": self._safe_str(row.get("status")),
            "priority": self._safe_str(row.get("priority")),
            "satisfaction_score": self._safe_int(row.get("satisfaction_score")),
            "created_at": self._safe_str(row.get("created_at")),
            "resolved_at": self._safe_str(row.get("resolved_at")),
            "source_file": self._safe_str(row.get("source_file")),
            "loaded_at": self._safe_str(row.get("loaded_at")),
        }

    def _call_transform(self, row: Dict[str, str]) -> Dict[str, Any]:
        return {
            "sentiment_overall": self._safe_str(row.get("sentiment_overall")),
            "duration_seconds": self._safe_int(row.get("duration_seconds")),
            "quality_score": self._safe_float(row.get("quality_score")),
            "transfers": self._safe_int(row.get("transfers")),
            "call_start": self._safe_str(row.get("call_start")),
            "source_file": self._safe_str(row.get("source_file")),
            "loaded_at": self._safe_str(row.get("loaded_at")),
        }

    async def _load_agents(self) -> LoadResult:
        """Load agent nodes from tickets and calls."""
        result = LoadResult(table="agent", table_type="node")
        
        agents = {}  # id -> name
        
        # Scan tickets
        ticket_rows = self._read_csv("support_tickets.csv")
        for row in ticket_rows:
            agent_id = self._safe_str(row.get("agent_id"))
            if agent_id:
                agents[agent_id] = f"Agent {agent_id}"  # Name not always in tickets
        
        # Scan calls (better source for names)
        call_rows = self._read_csv("call_transcripts.csv")
        for row in call_rows:
            agent_id = self._safe_str(row.get("agent_id"))
            agent_name = self._safe_str(row.get("agent_name"))
            if agent_id:
                if agent_name:
                    agents[agent_id] = agent_name
                elif agent_id not in agents:
                    # Keep existing placeholder if we already found it in tickets w/o name
                    pass
                else: 
                     agents[agent_id] = f"Agent {agent_id}"

        for agent_id, name in agents.items():
            try:
                clean_id = self._sanitize_id(agent_id)
                record_str = f"agent:`{clean_id}`"
                await self.db.query(
                    f"CREATE {record_str} CONTENT $data",
                    {"data": {"name": name}},
                )
                result.records_loaded += 1
            except Exception as e:
                result.errors += 1
                self.logger.debug(f"Failed to create agent:{agent_id}: {e}")

        self.logger.info(f"✓ agent: {result.records_loaded} nodes (derived)")
        return result

    async def _load_raised_edges(self) -> LoadResult:
        """customer -> support_ticket."""
        result = LoadResult(table="raised", table_type="edge")
        rows = self._read_csv("support_tickets.csv")

        for row in rows:
            customer_id = self._safe_str(row.get("customer_id"))
            ticket_id = self._safe_str(row.get("ticket_id"))
            if customer_id and ticket_id:
                ok = await self._relate("customer", customer_id, "raised", "support_ticket", ticket_id)
                if ok:
                    result.records_loaded += 1
                else:
                    result.errors += 1

        self.logger.info(f"✓ raised: {result.records_loaded} edges")
        return result

    async def _load_about_edges(self) -> LoadResult:
        """support_ticket -> product."""
        result = LoadResult(table="about", table_type="edge")
        rows = self._read_csv("support_tickets.csv")

        for row in rows:
            ticket_id = self._safe_str(row.get("ticket_id"))
            product_id = self._safe_str(row.get("product_id"))
            if ticket_id and product_id:
                ok = await self._relate("support_ticket", ticket_id, "about", "product", product_id)
                if ok:
                    result.records_loaded += 1
                else:
                    result.errors += 1

        self.logger.info(f"✓ about: {result.records_loaded} edges")
        return result

    async def _load_handled_by_edges(self) -> LoadResult:
        """support_ticket -> agent."""
        result = LoadResult(table="handled_by", table_type="edge")
        rows = self._read_csv("support_tickets.csv")

        for row in rows:
            ticket_id = self._safe_str(row.get("ticket_id"))
            agent_id = self._safe_str(row.get("agent_id"))
            if ticket_id and agent_id:
                ok = await self._relate("support_ticket", ticket_id, "handled_by", "agent", agent_id)
                if ok:
                    result.records_loaded += 1
                else:
                    result.errors += 1

        self.logger.info(f"✓ handled_by: {result.records_loaded} edges")
        return result

    async def _load_includes_transcript_edges(self) -> LoadResult:
        """support_ticket -> call_transcript."""
        result = LoadResult(table="includes_transcript", table_type="edge")
        rows = self._read_csv("call_transcripts.csv")

        for row in rows:
            ticket_id = self._safe_str(row.get("ticket_id"))
            call_id = self._safe_str(row.get("call_id"))
            if ticket_id and call_id:
                ok = await self._relate("support_ticket", ticket_id, "includes_transcript", "call_transcript", call_id)
                if ok:
                    result.records_loaded += 1
                else:
                    result.errors += 1

        self.logger.info(f"✓ includes_transcript: {result.records_loaded} edges")
        return result

    async def _load_conducted_by_edges(self) -> LoadResult:
        """call_transcript -> agent."""
        result = LoadResult(table="conducted_by", table_type="edge")
        rows = self._read_csv("call_transcripts.csv")

        for row in rows:
            call_id = self._safe_str(row.get("call_id"))
            agent_id = self._safe_str(row.get("agent_id"))
            if call_id and agent_id:
                ok = await self._relate("call_transcript", call_id, "conducted_by", "agent", agent_id)
                if ok:
                    result.records_loaded += 1
                else:
                    result.errors += 1

        self.logger.info(f"✓ conducted_by: {result.records_loaded} edges")
        return result
