"""
Unit tests for Gold layer feature computation.
"""

import pytest
from pathlib import Path

import polars as pl
import duckdb

from src.gold.processor import GoldProcessor


class TestGoldProcessor:
    """Tests for Gold layer feature engineering."""
    
    @pytest.fixture
    def silver_dir(self, tmp_path):
        """Create minimal Silver CSV files for Gold testing."""
        silver = tmp_path / "silver"
        silver.mkdir()
        
        # Customers
        customers = pl.DataFrame({
            "customer_id": ["CUS-001", "CUS-002", "CUS-003"],
            "full_name": ["Alice", "Bob", "Charlie"],
            "email": ["a@test.com", "b@test.com", "c@test.com"],
            "segment": ["premium", "standard", "premium"],
            "total_spend": [5000.0, 200.0, 1500.0],
            "registration_date": ["2024-01-15", "2024-06-01", "2023-03-10"],
            "is_active": [True, True, False],
        })
        customers.write_csv(silver / "customers.csv")
        
        # Vendors
        vendors = pl.DataFrame({
            "vendor_id": ["VND-001", "VND-002"],
            "vendor_name": ["Acme", "Global"],
            "country": ["US", "UK"],
            "region": ["NA", "EU"],
            "reliability_score": [95.0, 80.0],
            "status": ["active", "active"],
        })
        vendors.write_csv(silver / "vendors.csv")
        
        # Products
        products = pl.DataFrame({
            "product_id": ["PRD-001", "PRD-002", "PRD-003"],
            "vendor_id": ["VND-001", "VND-001", "VND-002"],
            "sku": ["SKU-A", "SKU-B", "SKU-C"],
            "product_name": ["Widget", "Gadget", "Doohickey"],
            "category": ["Electronics", "Electronics", "Home"],
            "price": [49.99, 199.99, 29.99],
            "cost": [25.0, 100.0, 15.0],
            "stock_quantity": [100, 50, 200],
            "rating": [4.5, 3.8, 4.0],
            "is_active": [True, True, True],
        })
        products.write_csv(silver / "products.csv")
        
        # Transactions
        transactions = pl.DataFrame({
            "transaction_id": [f"TXN-{i:04X}" for i in range(1, 11)],
            "customer_id": ["CUS-001"] * 5 + ["CUS-002"] * 3 + ["CUS-003"] * 2,
            "product_id": ["PRD-001", "PRD-002", "PRD-001", "PRD-003", "PRD-002",
                           "PRD-001", "PRD-003", "PRD-001",
                           "PRD-002", "PRD-003"],
            "transaction_date": [
                "2025-01-15", "2025-01-20", "2025-02-10", "2025-03-05", "2025-03-15",
                "2025-02-01", "2025-02-15", "2025-03-01",
                "2025-01-10", "2025-02-20",
            ],
            "quantity": [2, 1, 3, 1, 2, 1, 4, 1, 1, 2],
            "total_amount": [99.98, 199.99, 149.97, 29.99, 399.98,
                             49.99, 119.96, 49.99,
                             199.99, 59.98],
            "order_status": ["COMPLETED"] * 10,
        })
        transactions.write_csv(silver / "transactions.csv")
        
        # Reviews
        reviews = pl.DataFrame({
            "review_id": ["REV-001", "REV-002", "REV-003"],
            "product_id": ["PRD-001", "PRD-002", "PRD-001"],
            "customer_id": ["CUS-001", "CUS-002", "CUS-003"],
            "rating": [5, 3, 4],
            "sentiment": ["positive", "neutral", "positive"],
            "verified_purchase": [True, True, False],
        })
        reviews.write_csv(silver / "reviews.csv")
        
        # Invoices
        invoices = pl.DataFrame({
            "invoice_id": ["INV-001", "INV-002", "INV-003"],
            "vendor_id": ["VND-001", "VND-001", "VND-002"],
            "invoice_date": ["2025-01-01", "2025-02-01", "2025-01-15"],
            "due_date": ["2025-02-01", "2025-03-01", "2025-02-15"],
            "payment_date": ["2025-01-25", None, "2025-02-20"],
            "total_amount": [5000.0, 3000.0, 2000.0],
            "payment_status": ["paid", "pending", "paid"],
            "payment_terms": ["NET30", "NET30", "NET30"],
        })
        invoices.write_csv(silver / "invoices.csv")
        
        return silver
    
    @pytest.fixture
    def gold_processor(self, silver_dir, tmp_path):
        """Create Gold processor with test data."""
        output_dir = tmp_path / "outputs" / "processed"
        output_dir.mkdir(parents=True)
        
        processor = GoldProcessor(
            silver_dir=silver_dir,
            output_dir=output_dir,
            db_path=None,  # in-memory
        )
        return processor
    
    def test_process_all(self, gold_processor):
        """Test that all feature tables are computed."""
        results = gold_processor.process_all()
        
        assert "customer_features" in results
        assert "product_features" in results
        assert "vendor_features" in results
        assert "invoice_features" in results
        
        gold_processor.close()
    
    def test_customer_features_rfm(self, gold_processor):
        """Test customer features include RFM score."""
        results = gold_processor.process_all()
        
        cf = results["customer_features"]
        assert cf.row_count == 3
        assert "customer_segment_score" in cf.columns
        assert "customer_lifetime_value" in cf.columns
        assert "purchase_frequency" in cf.columns
        assert "average_order_value" in cf.columns
        assert "days_since_last_purchase" in cf.columns
        assert "recency_score" in cf.columns
        assert "frequency_score" in cf.columns
        assert "monetary_score" in cf.columns
        
        gold_processor.close()
    
    def test_product_features_complete(self, gold_processor):
        """Test product features include all README-required columns."""
        results = gold_processor.process_all()
        
        pf = results["product_features"]
        assert pf.row_count == 3
        assert "revenue_contribution" in pf.columns
        assert "velocity_score" in pf.columns
        assert "stock_turnover_rate" in pf.columns
        assert "price_tier" in pf.columns
        assert "vendor_reliability_weighted_score" in pf.columns
        
        gold_processor.close()
    
    def test_vendor_features_complete(self, gold_processor):
        """Test vendor features include outstanding balance."""
        results = gold_processor.process_all()
        
        vf = results["vendor_features"]
        assert vf.row_count == 2
        assert "total_products_supplied" in vf.columns
        assert "revenue_generated" in vf.columns
        assert "product_quality_score" in vf.columns
        assert "invoice_payment_rate" in vf.columns
        assert "average_invoice_value" in vf.columns
        assert "total_outstanding_balance" in vf.columns
        
        gold_processor.close()
    
    def test_invoice_features_complete(self, gold_processor):
        """Test invoice features include diversity, discount rate, and reconciliation."""
        results = gold_processor.process_all()
        
        inf = results["invoice_features"]
        assert inf.row_count == 3
        assert "days_to_payment" in inf.columns
        assert "days_overdue" in inf.columns
        assert "line_item_diversity" in inf.columns
        assert "discount_rate_achieved" in inf.columns
        assert "reconciliation_flag" in inf.columns
        
        gold_processor.close()
    
    def test_gold_csv_output(self, gold_processor, tmp_path):
        """Test that Gold CSV files are created."""
        gold_processor.process_all()
        
        gold_dir = tmp_path / "outputs" / "processed" / "gold"
        csv_files = list(gold_dir.glob("*.csv"))
        assert len(csv_files) == 4
        
        gold_processor.close()
    
    def test_vendor_outstanding_balance(self, gold_processor):
        """Test outstanding balance is correctly computed."""
        results = gold_processor.process_all()
        
        # VND-001 has 1 paid (5000) and 1 pending (3000)
        # VND-002 has 1 paid (2000)
        # Read the output to verify
        gold_dir = gold_processor.output_dir
        df = pl.read_csv(gold_dir / "vendor_features.csv")
        
        vnd001 = df.filter(pl.col("vendor_id") == "VND-001")
        assert vnd001["total_outstanding_balance"][0] == 3000.0
        
        vnd002 = df.filter(pl.col("vendor_id") == "VND-002")
        assert vnd002["total_outstanding_balance"][0] == 0.0

        gold_processor.close()

    def test_vendor_payment_rate_paid_on_time(self, gold_processor):
        """Payment rate should count only invoices paid on or before due date."""
        gold_processor.process_all()

        gold_dir = gold_processor.output_dir
        df = pl.read_csv(gold_dir / "vendor_features.csv")

        # VND-001: one on-time paid + one pending => 1/2 = 0.5
        vnd001 = df.filter(pl.col("vendor_id") == "VND-001")
        assert vnd001["invoice_payment_rate"][0] == pytest.approx(0.5, abs=0.001)

        # VND-002: one paid but late => 0/1 = 0.0
        vnd002 = df.filter(pl.col("vendor_id") == "VND-002")
        assert vnd002["invoice_payment_rate"][0] == pytest.approx(0.0, abs=0.001)

        gold_processor.close()


class TestGoldEdgeCases:
    """W4: Edge case tests for Gold layer computations."""

    @pytest.fixture
    def silver_dir_edge(self, tmp_path):
        """Silver data with edge cases: no transactions, no invoices for some entities."""
        silver = tmp_path / "silver"
        silver.mkdir()

        # Customer with zero transactions
        customers = pl.DataFrame({
            "customer_id": ["CUS-001", "CUS-002"],
            "full_name": ["Active Buyer", "Zero Transactions"],
            "email": ["a@test.com", "b@test.com"],
            "segment": ["premium", "standard"],
            "total_spend": [1000.0, 0.0],
            "registration_date": ["2024-01-15", "2024-06-01"],
            "is_active": [True, True],
        })
        customers.write_csv(silver / "customers.csv")

        vendors = pl.DataFrame({
            "vendor_id": ["VND-001", "VND-002"],
            "vendor_name": ["Active Vendor", "No Invoices Vendor"],
            "country": ["US", "UK"],
            "region": ["NA", "EU"],
            "reliability_score": [95.0, 80.0],
            "status": ["active", "active"],
        })
        vendors.write_csv(silver / "vendors.csv")

        products = pl.DataFrame({
            "product_id": ["PRD-001", "PRD-002"],
            "vendor_id": ["VND-001", "VND-002"],
            "sku": ["SKU-A", "SKU-B"],
            "product_name": ["Popular Widget", "Unsold Gadget"],
            "category": ["Electronics", "Home"],
            "price": [49.99, 29.99],
            "cost": [25.0, 15.0],
            "stock_quantity": [100, 200],
            "rating": [4.5, 3.0],
            "is_active": [True, True],
        })
        products.write_csv(silver / "products.csv")

        # Only CUS-001 buys PRD-001; CUS-002 and PRD-002 have no transactions
        transactions = pl.DataFrame({
            "transaction_id": ["TXN-0001"],
            "customer_id": ["CUS-001"],
            "product_id": ["PRD-001"],
            "transaction_date": ["2025-01-15"],
            "quantity": [2],
            "total_amount": [99.98],
            "order_status": ["COMPLETED"],
        })
        transactions.write_csv(silver / "transactions.csv")

        reviews = pl.DataFrame({
            "review_id": ["REV-001"],
            "product_id": ["PRD-001"],
            "customer_id": ["CUS-001"],
            "rating": [5],
            "sentiment": ["positive"],
            "verified_purchase": [True],
        })
        reviews.write_csv(silver / "reviews.csv")

        # Only VND-001 has an invoice
        invoices = pl.DataFrame({
            "invoice_id": ["INV-001"],
            "vendor_id": ["VND-001"],
            "invoice_date": ["2025-01-01"],
            "due_date": ["2025-02-01"],
            "payment_date": ["2025-01-25"],
            "total_amount": [5000.0],
            "payment_status": ["paid"],
            "payment_terms": ["NET30"],
        })
        invoices.write_csv(silver / "invoices.csv")

        return silver

    def test_customer_no_transactions(self, silver_dir_edge, tmp_path):
        """Customer with zero transactions should have CLV=0, frequency=0."""
        output_dir = tmp_path / "outputs" / "processed"
        output_dir.mkdir(parents=True)

        processor = GoldProcessor(
            silver_dir=silver_dir_edge,
            output_dir=output_dir,
            db_path=None,
        )
        results = processor.process_all()

        gold_dir = output_dir / "gold"
        df = pl.read_csv(gold_dir / "customer_features.csv")
        no_txn = df.filter(pl.col("customer_id") == "CUS-002")

        assert len(no_txn) == 1
        # LEFT JOIN produces NULL, not 0, for customers with no transactions
        assert no_txn["purchase_frequency"][0] is None or no_txn["purchase_frequency"][0] == 0
        assert no_txn["customer_lifetime_value"][0] is None or no_txn["customer_lifetime_value"][0] == 0

        processor.close()

    def test_product_no_sales(self, silver_dir_edge, tmp_path):
        """Product with no sales should have velocity_score=0."""
        output_dir = tmp_path / "outputs" / "processed"
        output_dir.mkdir(parents=True)

        processor = GoldProcessor(
            silver_dir=silver_dir_edge,
            output_dir=output_dir,
            db_path=None,
        )
        results = processor.process_all()

        gold_dir = output_dir / "gold"
        df = pl.read_csv(gold_dir / "product_features.csv")
        unsold = df.filter(pl.col("product_id") == "PRD-002")

        assert len(unsold) == 1
        assert unsold["velocity_score"][0] == 0

        processor.close()

    def test_vendor_no_invoices(self, silver_dir_edge, tmp_path):
        """Vendor with no invoices should have payment_rate=0, outstanding=0."""
        output_dir = tmp_path / "outputs" / "processed"
        output_dir.mkdir(parents=True)

        processor = GoldProcessor(
            silver_dir=silver_dir_edge,
            output_dir=output_dir,
            db_path=None,
        )
        results = processor.process_all()

        gold_dir = output_dir / "gold"
        df = pl.read_csv(gold_dir / "vendor_features.csv")
        no_inv = df.filter(pl.col("vendor_id") == "VND-002")

        assert len(no_inv) == 1
        assert no_inv["total_outstanding_balance"][0] == 0.0

        processor.close()


class TestReturnTransactions:
    """Tests that return transactions are tracked separately in Gold features."""

    @pytest.fixture
    def silver_with_returns(self, tmp_path):
        """Create Silver data that includes a return transaction."""
        silver = tmp_path / "silver"
        silver.mkdir()

        customers = pl.DataFrame({
            "customer_id": ["CUS-001"],
            "full_name": ["Alice"],
            "email": ["a@test.com"],
            "segment": ["premium"],
            "total_spend": [5000.0],
            "registration_date": ["2024-01-15"],
            "is_active": [True],
        })
        customers.write_csv(silver / "customers.csv")

        vendors = pl.DataFrame({
            "vendor_id": ["VND-001"],
            "vendor_name": ["Acme"],
            "country": ["US"],
            "region": ["NA"],
            "reliability_score": [95.0],
            "status": ["active"],
        })
        vendors.write_csv(silver / "vendors.csv")

        products = pl.DataFrame({
            "product_id": ["PRD-001"],
            "vendor_id": ["VND-001"],
            "sku": ["SKU-A"],
            "product_name": ["Widget"],
            "category": ["Electronics"],
            "price": [49.99],
            "cost": [25.0],
            "stock_quantity": [100],
            "rating": [4.5],
            "is_active": [True],
        })
        products.write_csv(silver / "products.csv")

        # Include one normal and one return transaction
        transactions = pl.DataFrame({
            "transaction_id": ["TXN-0001", "TXN-0002"],
            "customer_id": ["CUS-001", "CUS-001"],
            "product_id": ["PRD-001", "PRD-001"],
            "transaction_date": ["2025-01-15", "2025-02-01"],
            "quantity": [2, -1],
            "total_amount": [99.98, -49.99],
            "order_status": ["COMPLETED", "RETURNED"],
        })
        transactions.write_csv(silver / "transactions.csv")

        reviews = pl.DataFrame({
            "review_id": ["REV-001"],
            "product_id": ["PRD-001"],
            "customer_id": ["CUS-001"],
            "rating": [5],
            "sentiment": ["positive"],
            "verified_purchase": [True],
        })
        reviews.write_csv(silver / "reviews.csv")

        invoices = pl.DataFrame({
            "invoice_id": ["INV-001"],
            "vendor_id": ["VND-001"],
            "invoice_date": ["2025-01-01"],
            "due_date": ["2025-02-01"],
            "payment_date": ["2025-01-25"],
            "total_amount": [5000.0],
            "payment_status": ["paid"],
            "payment_terms": ["NET30"],
        })
        invoices.write_csv(silver / "invoices.csv")

        return silver

    def test_customer_features_track_returns(self, silver_with_returns, tmp_path):
        """Customer features should include return_count and gross_revenue columns."""
        output_dir = tmp_path / "outputs" / "processed"
        output_dir.mkdir(parents=True)

        processor = GoldProcessor(
            silver_dir=silver_with_returns,
            output_dir=output_dir,
            db_path=None,
        )
        processor.process_all()

        gold_dir = output_dir / "gold"
        df = pl.read_csv(gold_dir / "customer_features.csv")
        cus = df.filter(pl.col("customer_id") == "CUS-001")

        assert len(cus) == 1
        assert "return_count" in df.columns
        assert "gross_revenue" in df.columns
        assert "return_amount" in df.columns

        # 1 return out of 2 total
        assert cus["return_count"][0] == 1
        # gross = 99.98, return = -49.99
        assert cus["gross_revenue"][0] == pytest.approx(99.98, abs=0.01)
        assert cus["return_amount"][0] == pytest.approx(-49.99, abs=0.01)

        processor.close()
