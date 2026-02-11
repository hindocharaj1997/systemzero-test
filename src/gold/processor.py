"""
Gold Layer Processor - DuckDB SQL.

Computes feature tables from Silver data using SQL aggregations.
Includes all features required by the assessment README.
"""

from pathlib import Path
from typing import Dict, Any, List
from datetime import datetime
from dataclasses import dataclass

import duckdb
from loguru import logger


@dataclass
class FeatureResult:
    """Result of feature computation."""
    feature_table: str
    row_count: int
    columns: List[str]


class GoldProcessor:
    """
    Computes Gold layer feature tables using DuckDB SQL.
    
    Feature tables:
    - customer_features: CLV, RFM score, frequency, recency
    - product_features: revenue, velocity, turnover, vendor-weighted score
    - vendor_features: quality, payment rate, outstanding balance
    - invoice_features: payment speed, overdue, line item diversity, reconciliation
    """
    
    def __init__(
        self,
        silver_dir: Path,
        output_dir: Path,
        db_path: Path = None,
    ):
        self.silver_dir = Path(silver_dir)
        self.output_dir = Path(output_dir) / "gold"
        self.output_dir.mkdir(parents=True, exist_ok=True)
        self.db_path = db_path
        self.logger = logger.bind(component="GoldProcessor")
        
        if db_path:
            db_path.parent.mkdir(parents=True, exist_ok=True)
            self.conn = duckdb.connect(str(db_path))
            self.logger.info(f"DuckDB: {db_path}")
        else:
            self.conn = duckdb.connect(":memory:")
            self.logger.info("DuckDB: in-memory")
    
    def process_all(self) -> Dict[str, FeatureResult]:
        """Compute all feature tables."""
        self.logger.info("=" * 60)
        self.logger.info("GOLD LAYER: Computing features (DuckDB SQL)")
        self.logger.info("=" * 60)
        
        self._load_silver_data()
        
        results = {}
        results["customer_features"] = self._compute_customer_features()
        results["product_features"] = self._compute_product_features()
        results["vendor_features"] = self._compute_vendor_features()
        results["invoice_features"] = self._compute_invoice_features()
        
        total_features = sum(len(r.columns) for r in results.values())
        self.logger.info(f"Gold complete: {len(results)} tables, {total_features} features")
        
        return results
    
    def _load_silver_data(self) -> None:
        """Load Silver CSV files into DuckDB views."""
        for csv_file in self.silver_dir.glob("*.csv"):
            table_name = csv_file.stem
            self.conn.execute(f"""
                CREATE OR REPLACE VIEW {table_name} AS 
                SELECT * FROM read_csv('{csv_file}')
            """)
            self.logger.debug(f"Loaded view: {table_name}")
    
    def _export_and_describe(self, table_name: str) -> FeatureResult:
        """Export table to CSV and return metadata."""
        csv_path = self.output_dir / f"{table_name}.csv"
        self.conn.execute(f"COPY {table_name} TO '{csv_path}' (HEADER, DELIMITER ',')")
        
        desc = self.conn.execute(f"SELECT * FROM {table_name} LIMIT 0").description
        columns = [col[0] for col in desc]
        row_count = self.conn.execute(f"SELECT COUNT(*) FROM {table_name}").fetchone()[0]
        
        self.logger.info(f"✓ {table_name}: {row_count} rows, {len(columns)} columns")
        
        return FeatureResult(feature_table=table_name, row_count=row_count, columns=columns)

    def _load_sql(self, filename: str) -> str:
        """Load SQL query from file."""
        # Locate sql directory relative to this file
        sql_path = Path(__file__).parent / "sql" / filename
        if not sql_path.exists():
            raise FileNotFoundError(f"SQL file not found: {sql_path}")
        return sql_path.read_text()
    
    def _compute_customer_features(self) -> FeatureResult:
        """
        Customer features including RFM-based segment score.
        
        README required:
        - customer_lifetime_value ✓
        - purchase_frequency ✓
        - average_order_value ✓
        - days_since_last_purchase ✓
        - customer_segment_score (RFM) ✓
        """
        self.logger.info("Computing customer_features...")
        
        sql = self._load_sql("customer_features.sql")
        self.conn.execute(sql)
        return self._export_and_describe("customer_features")
    
    def _compute_product_features(self) -> FeatureResult:
        """
        Product features including velocity, turnover, and vendor-weighted score.
        
        README required:
        - revenue_contribution ✓
        - velocity_score ✓
        - stock_turnover_rate ✓
        - price_tier ✓
        - vendor_reliability_weighted_score ✓
        """
        self.logger.info("Computing product_features...")
        
        sql = self._load_sql("product_features.sql")
        self.conn.execute(sql)
        return self._export_and_describe("product_features")
    
    def _compute_vendor_features(self) -> FeatureResult:
        """
        Vendor features including outstanding balance.
        
        README required:
        - total_products_supplied ✓
        - revenue_generated ✓
        - product_quality_score ✓
        - invoice_payment_rate ✓
        - average_invoice_value ✓
        - total_outstanding_balance ✓
        """
        self.logger.info("Computing vendor_features...")
        
        sql = self._load_sql("vendor_features.sql")
        self.conn.execute(sql)
        return self._export_and_describe("vendor_features")
    
    def _compute_invoice_features(self) -> FeatureResult:
        """
        Invoice features including line item diversity and reconciliation.
        
        README required:
        - days_to_payment ✓
        - days_overdue ✓
        - line_item_diversity ✓
        - discount_rate_achieved ✓
        - reconciliation_flag ✓
        """
        self.logger.info("Computing invoice_features...")
        
        # Check if parsed line items exist
        line_items_path = self.silver_dir / "invoice_line_items.csv"
        has_line_items = line_items_path.exists()
        
        if has_line_items:
            self.conn.execute(f"""
                CREATE OR REPLACE VIEW invoice_line_items AS 
                SELECT * FROM read_csv('{line_items_path}')
            """)
        
        # Build line items subquery based on data availability
        if has_line_items:
            line_items_cte = """
            line_item_stats AS (
                SELECT 
                    invoice_id,
                    COUNT(DISTINCT product_id) as unique_products,
                    SUM(TRY_CAST(line_total AS DOUBLE)) as line_items_total
                FROM invoice_line_items
                GROUP BY invoice_id
            )"""
        else:
            # Fallback: approximate from transactions per vendor
            line_items_cte = """
            line_item_stats AS (
                SELECT 
                    i2.invoice_id,
                    COALESCE(v_stats.unique_products, 0) as unique_products,
                    COALESCE(v_stats.expected_cost, 0) as line_items_total  
                FROM invoices i2
                LEFT JOIN (
                    SELECT 
                        p.vendor_id,
                        COUNT(DISTINCT t.product_id) as unique_products,
                        SUM(TRY_CAST(p.cost AS DOUBLE) * t.quantity) as expected_cost
                    FROM transactions t
                    JOIN products p ON t.product_id = p.product_id
                    GROUP BY p.vendor_id
                ) v_stats ON i2.vendor_id = v_stats.vendor_id
            )"""
        
        # Interpolate the CTE into the SQL template
        # We can't use simple file loading here because of the f-string interpolation for `line_items_cte`
        # But we can load the base SQL and duplicate the CTE injection logic? 
        # Or better: make the SQL file a template with {line_items_cte} placeholder.
        
        sql_template = self._load_sql("invoice_features.sql")
        
        # Determine which CTE to use (same logic as before)
        # Note: I'll move the large CTE strings here or keep them? 
        # The SQL file I created HAS `{line_items_cte}` placeholder.
        # So I just need to format it.
        
        sql = sql_template.format(line_items_cte=line_items_cte)
        
        self.conn.execute(sql)
        return self._export_and_describe("invoice_features")
    
    def close(self) -> None:
        """Close DuckDB connection."""
        if self.conn:
            self.conn.close()
            self.logger.debug("DuckDB connection closed")
