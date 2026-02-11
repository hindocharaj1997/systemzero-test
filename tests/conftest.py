"""
Shared test fixtures for the data pipeline tests.
"""

import pytest
import tempfile
import json
import os
from pathlib import Path

import polars as pl


@pytest.fixture
def tmp_dir(tmp_path):
    """Provide a temporary directory for test outputs."""
    return tmp_path


@pytest.fixture
def sample_sources_config():
    """Minimal sources.yaml equivalent."""
    return {
        "sources": {
            "customers": {
                "file": "customers.json",
                "format": "json",
                "data_key": "customers",
                "schema": "customer",
            },
            "products": {
                "file": "products.csv",
                "format": "csv",
                "schema": "product",
            },
            "vendors": {
                "file": "vendors.json",
                "format": "json",
                "data_key": "vendors",
                "schema": "vendor",
            },
        }
    }


@pytest.fixture
def sample_schemas_config():
    """Minimal schemas.yaml equivalent."""
    return {
        "schemas": {
            "customer": {
                "primary_key": "customer_id",
                "fields": {
                    "customer_id": {"type": "string", "required": True, "pattern": "^CUS-\\d+$"},
                    "full_name": {"type": "string", "required": True},
                    "email": {"type": "string", "clean": "lowercase"},
                    "phone": {"type": "string", "clean": "phone_normalize"},
                    "segment": {"type": "string"},
                    "total_spend": {"type": "float", "min": 0},
                    "is_active": {"type": "boolean", "clean": "boolean_normalize"},
                },
            },
            "product": {
                "primary_key": "product_id",
                "fields": {
                    "product_id": {"type": "string", "required": True, "pattern": "^PRD-\\d+$"},
                    "vendor_id": {"type": "string", "required": True, "foreign_key": "vendor"},
                    "sku": {"type": "string", "required": True},
                    "product_name": {"type": "string", "required": True},
                    "price": {"type": "float", "min": 0},
                    "is_active": {"type": "boolean", "clean": "boolean_normalize"},
                },
            },
            "vendor": {
                "primary_key": "vendor_id",
                "fields": {
                    "vendor_id": {"type": "string", "required": True, "pattern": "^VND-\\d+$"},
                    "vendor_name": {"type": "string", "required": True},
                    "country": {"type": "string"},
                    "status": {"type": "string", "clean": "lowercase"},
                },
            },
        }
    }


@pytest.fixture
def sample_cleaning_rules():
    """Minimal cleaning_rules.yaml equivalent."""
    return {
        "cleaners": {
            "lowercase": {"type": "case", "case": "lower"},
            "uppercase": {"type": "case", "case": "upper"},
            "phone_normalize": {"type": "phone", "remove_chars": "()- .+"},
            "boolean_normalize": {
                "type": "boolean",
                "true_values": [True, "true", "True", "1", 1, "yes"],
                "false_values": [False, "false", "False", "0", 0, "no"],
            },
            "date_iso": {"type": "date", "output_format": "%Y-%m-%d"},
        }
    }


@pytest.fixture
def sample_input_dir(tmp_path):
    """Create sample input files for Bronze ingestion."""
    input_dir = tmp_path / "data"
    input_dir.mkdir()
    
    # CSV file
    csv_content = "product_id,vendor_id,sku,product_name,category,price,cost,stock_quantity,rating,is_active\n"
    csv_content += "PRD-001,VND-001,SKU-001,Widget A,Electronics,29.99,15.00,100,4.5,true\n"
    csv_content += "PRD-002,VND-001,SKU-002,Widget B,Electronics,49.99,25.00,50,3.8,false\n"
    csv_content += "PRD-003,VND-002,SKU-003,Gadget C,Home,99.99,60.00,200,4.2,true\n"
    csv_content += "PRD-003,VND-002,SKU-003,Gadget C,Home,99.99,60.00,200,4.2,true\n"  # Duplicate
    csv_content += "PRD-004,VND-999,SKU-004,Bad Ref,Home,19.99,10.00,30,3.0,true\n"    # Bad vendor FK
    (input_dir / "products.csv").write_text(csv_content)
    
    # JSON file
    customers = {
        "customers": [
            {
                "customer_id": "CUS-001",
                "full_name": "John Doe",
                "email": "JOHN@EXAMPLE.COM",
                "phone": "(555) 123-4567",
                "segment": "premium",
                "total_spend": 1500.0,
                "is_active": "yes",
            },
            {
                "customer_id": "CUS-002",
                "full_name": "Jane Smith",
                "email": "Jane@Example.com",
                "phone": "555.987.6543",
                "segment": "standard",
                "total_spend": 250.0,
                "is_active": "true",
            },
            {
                "customer_id": "CUS-001",  # Duplicate
                "full_name": "John Doe Dup",
                "email": "john2@example.com",
                "phone": "5551234567",
                "segment": "standard",
                "total_spend": 100.0,
                "is_active": "false",
            },
        ]
    }
    (input_dir / "customers.json").write_text(json.dumps(customers))
    
    # Vendors JSON
    vendors = {
        "vendors": [
            {
                "vendor_id": "VND-001",
                "vendor_name": "Acme Corp",
                "country": "US",
                "region": "North America",
                "reliability_score": 95.0,
                "status": "ACTIVE",
            },
            {
                "vendor_id": "VND-002",
                "vendor_name": "Global Tech",
                "country": "UK",
                "region": "Europe",
                "reliability_score": 88.5,
                "status": "Active",
            },
        ]
    }
    (input_dir / "vendors.json").write_text(json.dumps(vendors))
    
    return input_dir


@pytest.fixture
def bronze_output_dir(tmp_path):
    """Directory for Bronze outputs."""
    d = tmp_path / "outputs" / "processed" / "bronze"
    d.mkdir(parents=True)
    return d


@pytest.fixture
def silver_output_dir(tmp_path):
    """Directory for Silver outputs (parent of 'processed')."""
    d = tmp_path / "outputs" / "processed"
    d.mkdir(parents=True, exist_ok=True)
    return d
