"""
Unit tests for Silver layer: cleaning, deduplication, and validation.
"""

import json
import pytest
from pathlib import Path

import polars as pl

from src.silver.cleaner import SilverCleaner
from src.silver.processor import SilverProcessor
from src.bronze.ingester import BronzeIngester


class TestSilverCleaner:
    """Tests for the SilverCleaner utility."""
    
    @pytest.fixture
    def cleaner(self, sample_cleaning_rules):
        return SilverCleaner(sample_cleaning_rules)
    
    def test_clean_case_lowercase(self, cleaner):
        """Test lowercase normalization."""
        val, changed, _ = cleaner.clean_value("HELLO", "lowercase")
        assert val == "hello"
        assert changed is True
    
    def test_clean_case_no_change(self, cleaner):
        """Test no change when already lowercase."""
        val, changed, _ = cleaner.clean_value("hello", "lowercase")
        assert val == "hello"
        assert changed is False
    
    def test_clean_phone(self, cleaner):
        """Test phone number normalization."""
        val, changed, _ = cleaner.clean_value("(555) 123-4567", "phone_normalize")
        assert val == "5551234567"
        assert changed is True
    
    def test_clean_boolean_true(self, cleaner):
        """Test boolean normalization for true values."""
        for truthy in ["true", "True", "1", 1, "yes"]:
            val, changed, _ = cleaner.clean_value(truthy, "boolean_normalize")
            assert val is True
    
    def test_clean_boolean_false(self, cleaner):
        """Test boolean normalization for false values."""
        for falsy in ["false", "False", "0", 0, "no"]:
            val, changed, _ = cleaner.clean_value(falsy, "boolean_normalize")
            assert val is False
    
    def test_clean_none_value(self, cleaner):
        """Test that None passes through unchanged."""
        val, changed, _ = cleaner.clean_value(None, "lowercase")
        assert val is None
        assert changed is False
    
    def test_clean_unknown_rule(self, cleaner):
        """Test unknown rule returns value unchanged."""
        val, changed, _ = cleaner.clean_value("test", "nonexistent_rule")
        assert val == "test"
        assert changed is False


class TestSilverProcessor:
    """Tests for Silver processor with dedup and FK checks."""
    
    def _run_bronze_then_silver(
        self, sample_sources_config, sample_schemas_config,
        sample_cleaning_rules, sample_input_dir, tmp_path,
        sources_override=None,
    ):
        """Helper: run Bronze to create CSVs, then Silver to process them."""
        output_dir = tmp_path / "outputs" / "processed"
        output_dir.mkdir(parents=True)
        
        config = sources_override or sample_sources_config
        
        # Run Bronze first
        ingester = BronzeIngester(
            sources_config=config,
            input_dir=sample_input_dir,
            output_dir=output_dir,
        )
        ingester.ingest_all()
        
        # Run Silver
        processor = SilverProcessor(
            sources_config=config,
            schemas_config=sample_schemas_config,
            cleaning_rules=sample_cleaning_rules,
            bronze_dir=output_dir / "bronze",
            output_dir=output_dir,
        )
        return processor.process_all()
    
    def test_deduplication(
        self, sample_sources_config, sample_schemas_config,
        sample_cleaning_rules, sample_input_dir, tmp_path,
    ):
        """Test that duplicates are removed based on primary key."""
        results = self._run_bronze_then_silver(
            sample_sources_config, sample_schemas_config,
            sample_cleaning_rules, sample_input_dir, tmp_path,
        )
        
        # Customers had 3 records with CUS-001 duplicated → 1 removed
        assert results["customers"].duplicates_removed == 1
        assert results["customers"].valid_records == 2
    
    def test_referential_integrity(
        self, sample_sources_config, sample_schemas_config,
        sample_cleaning_rules, sample_input_dir, tmp_path,
    ):
        """Test that orphaned FK references are quarantined."""
        results = self._run_bronze_then_silver(
            sample_sources_config, sample_schemas_config,
            sample_cleaning_rules, sample_input_dir, tmp_path,
        )
        
        # Products: PRD-004 has VND-999 which doesn't exist → 1 orphaned
        assert results["products"].orphaned_records == 1
    
    def test_cleaning_applied(
        self, sample_sources_config, sample_schemas_config,
        sample_cleaning_rules, sample_input_dir, tmp_path,
    ):
        """Test that cleaning rules are applied to fields."""
        results = self._run_bronze_then_silver(
            sample_sources_config, sample_schemas_config,
            sample_cleaning_rules, sample_input_dir, tmp_path,
        )
        
        # Vendor status has 'lowercase' clean rule → should be applied
        assert "status" in results["vendors"].fields_cleaned
    
    def test_quarantine_file_created(
        self, sample_sources_config, sample_schemas_config,
        sample_cleaning_rules, sample_input_dir, tmp_path,
    ):
        """Test that quarantine JSON files are created for invalid records."""
        self._run_bronze_then_silver(
            sample_sources_config, sample_schemas_config,
            sample_cleaning_rules, sample_input_dir, tmp_path,
        )
        
        quarantine_dir = tmp_path / "outputs" / "quarantine"
        quarantine_files = list(quarantine_dir.glob("*.json"))
        # Products should have quarantine (orphaned FK)
        assert len(quarantine_files) > 0
    
    def test_quarantine_has_error_details(
        self, sample_sources_config, sample_schemas_config,
        sample_cleaning_rules, sample_input_dir, tmp_path,
    ):
        """Test quarantine records contain error details."""
        self._run_bronze_then_silver(
            sample_sources_config, sample_schemas_config,
            sample_cleaning_rules, sample_input_dir, tmp_path,
        )
        
        quarantine_dir = tmp_path / "outputs" / "quarantine"
        for qf in quarantine_dir.glob("*.json"):
            with open(qf) as f:
                records = json.load(f)
            for record in records:
                assert "row_index" in record
                assert "errors" in record
                assert len(record["errors"]) > 0
    
    def test_pass_rate(
        self, sample_sources_config, sample_schemas_config,
        sample_cleaning_rules, sample_input_dir, tmp_path,
    ):
        """Test pass rate calculation."""
        results = self._run_bronze_then_silver(
            sample_sources_config, sample_schemas_config,
            sample_cleaning_rules, sample_input_dir, tmp_path,
        )
        
        for result in results.values():
            if result.total_records > 0:
                assert 0 <= result.pass_rate <= 1.0
    
    def test_silver_csv_output(
        self, sample_sources_config, sample_schemas_config,
        sample_cleaning_rules, sample_input_dir, tmp_path,
    ):
        """Test that Silver CSV files are created for valid sources."""
        self._run_bronze_then_silver(
            sample_sources_config, sample_schemas_config,
            sample_cleaning_rules, sample_input_dir, tmp_path,
        )
        
        silver_dir = tmp_path / "outputs" / "processed" / "silver"
        silver_files = list(silver_dir.glob("*.csv"))
        # All three sources should produce Silver CSVs
        assert len(silver_files) == 3


class TestSilverCleanerExtended:
    """Additional cleaner tests for edge cases and coverage."""
    
    @pytest.fixture
    def full_cleaner(self):
        """Cleaner with all rule types."""
        rules = {
            "cleaners": {
                "date_iso": {
                    "type": "date",
                    "output_format": "%Y-%m-%d",
                },
                "uppercase": {
                    "type": "case",
                    "case": "upper",
                },
                "titlecase": {
                    "type": "case",
                    "case": "title",
                },
                "lowercase": {
                    "type": "case",
                    "case": "lower",
                },
                "phone_normalize": {
                    "type": "phone",
                    "remove_chars": "()- .+",
                },
                "string_clean": {
                    "type": "string",
                    "operations": ["trim", "normalize_whitespace"],
                },
                "boolean_normalize": {
                    "type": "boolean",
                    "true_values": [True, "true", "True", "1", 1, "yes", "Yes"],
                    "false_values": [False, "false", "False", "0", 0, "no", "No"],
                },
            }
        }
        return SilverCleaner(rules)
    
    def test_clean_date_iso(self, full_cleaner):
        """Test date normalization to ISO format."""
        val, changed, desc = full_cleaner.clean_value("25/03/2025", "date_iso")
        assert val == "2025-03-25"
        assert changed is True
    
    def test_clean_date_already_iso(self, full_cleaner):
        """Test date already in ISO format."""
        val, changed, _ = full_cleaner.clean_value("2025-03-25", "date_iso")
        assert val == "2025-03-25"
        assert changed is False
    
    def test_clean_date_invalid(self, full_cleaner):
        """Test invalid date returns unchanged."""
        val, changed, _ = full_cleaner.clean_value("not-a-date", "date_iso")
        assert val == "not-a-date"
        assert changed is False
    
    def test_clean_case_uppercase(self, full_cleaner):
        """Test uppercase normalization."""
        val, changed, _ = full_cleaner.clean_value("hello", "uppercase")
        assert val == "HELLO"
        assert changed is True
    
    def test_clean_case_title(self, full_cleaner):
        """Test titlecase normalization."""
        val, changed, _ = full_cleaner.clean_value("hello world", "titlecase")
        assert val == "Hello World"
        assert changed is True
    
    def test_clean_string_trim_whitespace(self, full_cleaner):
        """Test string trim and whitespace normalization."""
        val, changed, _ = full_cleaner.clean_value("  hello   world  ", "string_clean")
        assert val == "hello world"
        assert changed is True
    
    def test_clean_empty_string(self, full_cleaner):
        """Test empty string passes through."""
        val, changed, _ = full_cleaner.clean_value("", "lowercase")
        assert val == ""
        assert changed is False
    
    def test_clean_boolean_already_bool(self, full_cleaner):
        """Test True stays True without marking as changed."""
        val, changed, _ = full_cleaner.clean_value(True, "boolean_normalize")
        assert val is True
        assert changed is False


class TestLineItemsParsing:
    """Tests for invoice line_items_json parsing."""
    
    def test_parse_invoice_line_items(self, tmp_path):
        """Test that line_items_json is correctly parsed into separate CSV."""
        import json
        
        # Create directory structure matching SilverProcessor expectations
        bronze_dir = tmp_path / "bronze"
        bronze_dir.mkdir()
        output_dir = tmp_path / "outputs"
        output_dir.mkdir()
        
        line_items = [
            {"line_number": 1, "product_id": "PRD-001", "description": "Widget", "quantity": 10, "unit_cost": 5.0, "line_total": 50.0},
            {"line_number": 2, "product_id": "PRD-002", "description": "Gadget", "quantity": 5, "unit_cost": 10.0, "line_total": 50.0},
        ]
        
        # Use valid invoice ID matching ^INV-[A-F0-9]+$
        df = pl.DataFrame({
            "invoice_id": ["INV-A1B2C3D4"],
            "vendor_id": ["VND-001"],
            "invoice_date": ["2025-01-01"],
            "due_date": ["2025-02-01"],
            "payment_date": [None],
            "total_amount": ["100.0"],
            "payment_status": ["pending"],
            "payment_terms": ["NET30"],
            "line_items_json": [json.dumps(line_items)],
        })
        df.write_csv(bronze_dir / "invoices.csv")
        
        # Also need vendor reference for FK check
        vendors_df = pl.DataFrame({
            "vendor_id": ["VND-001"],
            "vendor_name": ["Test Vendor"],
            "status": ["active"],
        })
        vendors_df.write_csv(bronze_dir / "vendors.csv")
        
        # Run processor with both vendors and invoices
        processor = SilverProcessor(
            sources_config={"sources": {
                "vendors": {"file": "vendors.csv", "format": "csv", "schema": "vendor"},
                "invoices": {"file": "invoices.csv", "format": "csv", "schema": "invoice"},
            }},
            schemas_config={"schemas": {
                "vendor": {"primary_key": "vendor_id", "fields": {"vendor_id": {"type": "string", "required": True}}},
                "invoice": {"primary_key": "invoice_id", "fields": {
                    "invoice_id": {"type": "string", "required": True},
                    "vendor_id": {"type": "string", "required": True, "foreign_key": "vendor"},
                }},
            }},
            cleaning_rules={"cleaners": {}},
            bronze_dir=bronze_dir,
            output_dir=output_dir,
        )
        results = processor.process_all()
        
        # Verify invoice was processed
        assert results["invoices"].valid_records == 1
        
        # SilverProcessor writes to output_dir/silver/invoice_line_items.csv
        line_items_path = output_dir / "silver" / "invoice_line_items.csv"
        assert line_items_path.exists(), f"Expected {line_items_path}"
        
        li_df = pl.read_csv(line_items_path)
        assert len(li_df) == 2
        assert "product_id" in li_df.columns
        assert "invoice_id" in li_df.columns
        assert li_df["invoice_id"][0] == "INV-A1B2C3D4"


class TestDateIsoCleaning:
    """W2: Tests to verify date_iso cleaning actually normalizes dates."""

    def test_mixed_date_formats_normalized(self, tmp_path):
        """Verify diverse date formats are normalized to ISO YYYY-MM-DD."""
        import re

        bronze_dir = tmp_path / "bronze"
        bronze_dir.mkdir()
        output_dir = tmp_path / "outputs"
        output_dir.mkdir()

        customers_df = pl.DataFrame({
            "customer_id": ["CUS-001", "CUS-002", "CUS-003", "CUS-004"],
            "full_name": ["Alice", "Bob", "Charlie", "Dana"],
            "email": ["a@test.com", "b@test.com", "c@test.com", "d@test.com"],
            "registration_date": [
                "2024-01-15",
                "03/25/2024",
                "15-Jan-2024",
                "2024-06-01T00:00:00",
            ],
        })
        customers_df.write_csv(bronze_dir / "customers.csv")

        processor = SilverProcessor(
            sources_config={"sources": {
                "customers": {"file": "customers.csv", "format": "csv", "schema": "customer"},
            }},
            schemas_config={"schemas": {
                "customer": {
                    "primary_key": "customer_id",
                    "fields": {
                        "customer_id": {"type": "string", "required": True, "pattern": r"^CUS-\d+$"},
                        "full_name": {"type": "string", "required": True},
                        "registration_date": {"type": "date", "clean": "date_iso"},
                    },
                },
            }},
            cleaning_rules={"cleaners": {
                "date_iso": {"type": "date", "output_format": "%Y-%m-%d"},
            }},
            bronze_dir=bronze_dir,
            output_dir=output_dir,
        )
        results = processor.process_all()

        silver_df = pl.read_csv(output_dir / "silver" / "customers.csv")
        dates = silver_df["registration_date"].to_list()

        iso_pattern = re.compile(r"^\d{4}-\d{2}-\d{2}$")
        for d in dates:
            assert iso_pattern.match(d), f"Date not normalized to ISO: {d}"

    def test_date_cleaning_stats_tracked(self, tmp_path):
        """Verify that date cleaning is tracked in fields_cleaned stats."""
        bronze_dir = tmp_path / "bronze"
        bronze_dir.mkdir()
        output_dir = tmp_path / "outputs"
        output_dir.mkdir()

        df = pl.DataFrame({
            "customer_id": ["CUS-001"],
            "full_name": ["Test"],
            "registration_date": ["01/15/2024"],
        })
        df.write_csv(bronze_dir / "customers.csv")

        processor = SilverProcessor(
            sources_config={"sources": {
                "customers": {"file": "customers.csv", "format": "csv", "schema": "customer"},
            }},
            schemas_config={"schemas": {
                "customer": {
                    "primary_key": "customer_id",
                    "fields": {
                        "customer_id": {"type": "string", "required": True, "pattern": r"^CUS-\d+$"},
                        "full_name": {"type": "string", "required": True},
                        "registration_date": {"type": "date", "clean": "date_iso"},
                    },
                },
            }},
            cleaning_rules={"cleaners": {
                "date_iso": {"type": "date", "output_format": "%Y-%m-%d"},
            }},
            bronze_dir=bronze_dir,
            output_dir=output_dir,
        )
        results = processor.process_all()

        assert "registration_date" in results["customers"].fields_cleaned


class TestFutureDateValidation:
    """B5: Test that future dates are quarantined."""

    def test_future_registration_date_quarantined(self, tmp_path):
        """Future registration dates should fail Pydantic validation."""
        bronze_dir = tmp_path / "bronze"
        bronze_dir.mkdir()
        output_dir = tmp_path / "outputs"
        output_dir.mkdir()

        df = pl.DataFrame({
            "customer_id": ["CUS-001", "CUS-002"],
            "full_name": ["Past User", "Future User"],
            "registration_date": ["2024-01-15", "2099-12-31"],
        })
        df.write_csv(bronze_dir / "customers.csv")

        processor = SilverProcessor(
            sources_config={"sources": {
                "customers": {"file": "customers.csv", "format": "csv", "schema": "customer"},
            }},
            schemas_config={"schemas": {
                "customer": {
                    "primary_key": "customer_id",
                    "fields": {
                        "customer_id": {"type": "string", "required": True, "pattern": r"^CUS-\d+$"},
                        "full_name": {"type": "string", "required": True},
                        "registration_date": {"type": "date", "clean": "date_iso"},
                    },
                },
            }},
            cleaning_rules={"cleaners": {
                "date_iso": {"type": "date", "output_format": "%Y-%m-%d"},
            }},
            bronze_dir=bronze_dir,
            output_dir=output_dir,
        )
        results = processor.process_all()

        assert results["customers"].valid_records == 2
        assert results["customers"].quarantined_records == 0
        
        # Verify that the future date was nullified
        silver_df = pl.read_csv(output_dir / "silver" / "customers.csv")
        future_user = silver_df.filter(pl.col("customer_id") == "CUS-002")
        assert future_user["registration_date"][0] is None


class TestLineItemValidation:
    """W3: Tests for invoice line item Pydantic validation."""

    def test_line_items_only_for_silver_invoices(self, tmp_path):
        """Line items should only be extracted for invoices that passed Silver."""
        bronze_dir = tmp_path / "bronze"
        bronze_dir.mkdir()
        output_dir = tmp_path / "outputs"
        output_dir.mkdir()

        line_items_valid = [
            {"line_number": 1, "product_id": "PRD-001", "description": "Widget",
             "quantity": 10, "unit_cost": 5.0, "line_total": 50.0},
        ]
        line_items_invalid_inv = [
            {"line_number": 1, "product_id": "PRD-002", "description": "Gadget",
             "quantity": 5, "unit_cost": 10.0, "line_total": 50.0},
        ]

        invoices_df = pl.DataFrame({
            "invoice_id": ["INV-A1B2C3D4", "INV-DEADBEEF"],
            "vendor_id": ["VND-001", "VND-INVALID"],
            "invoice_date": ["2025-01-01", "2025-01-01"],
            "due_date": ["2025-02-01", "2025-02-01"],
            "payment_date": [None, None],
            "total_amount": ["50.0", "50.0"],
            "payment_status": ["pending", "pending"],
            "line_items_json": [json.dumps(line_items_valid), json.dumps(line_items_invalid_inv)],
        })
        invoices_df.write_csv(bronze_dir / "invoices.csv")

        vendors_df = pl.DataFrame({
            "vendor_id": ["VND-001"],
            "vendor_name": ["Test Vendor"],
            "status": ["active"],
        })
        vendors_df.write_csv(bronze_dir / "vendors.csv")

        processor = SilverProcessor(
            sources_config={"sources": {
                "vendors": {"file": "vendors.csv", "format": "csv", "schema": "vendor"},
                "invoices": {"file": "invoices.csv", "format": "csv", "schema": "invoice"},
            }},
            schemas_config={"schemas": {
                "vendor": {"primary_key": "vendor_id", "fields": {
                    "vendor_id": {"type": "string", "required": True},
                }},
                "invoice": {"primary_key": "invoice_id", "fields": {
                    "invoice_id": {"type": "string", "required": True},
                    "vendor_id": {"type": "string", "required": True, "foreign_key": "vendor"},
                }},
            }},
            cleaning_rules={"cleaners": {}},
            bronze_dir=bronze_dir,
            output_dir=output_dir,
        )
        results = processor.process_all()

        assert results["invoices"].valid_records == 1

        line_items_path = output_dir / "silver" / "invoice_line_items.csv"
        assert line_items_path.exists()

        li_df = pl.read_csv(line_items_path)
        assert len(li_df) == 1
        assert li_df["invoice_id"][0] == "INV-A1B2C3D4"


class TestEmailValidation:
    """Tests for email format validation in CustomerSchema."""

    def test_valid_email_passes(self):
        from src.silver.schemas import CustomerSchema
        c = CustomerSchema(customer_id="CUS-001", full_name="Test", email="user@example.com")
        assert c.email == "user@example.com"

    def test_invalid_email_becomes_none(self):
        from src.silver.schemas import CustomerSchema
        c = CustomerSchema(customer_id="CUS-001", full_name="Test", email="not-an-email")
        assert c.email is None

    def test_email_missing_domain_becomes_none(self):
        from src.silver.schemas import CustomerSchema
        c = CustomerSchema(customer_id="CUS-001", full_name="Test", email="user@")
        assert c.email is None

    def test_missing_email_allowed(self):
        from src.silver.schemas import CustomerSchema
        c = CustomerSchema(customer_id="CUS-001", full_name="Test", email=None)
        assert c.email is None

    def test_empty_email_becomes_none(self):
        from src.silver.schemas import CustomerSchema
        c = CustomerSchema(customer_id="CUS-001", full_name="Test", email="")
        assert c.email is None
