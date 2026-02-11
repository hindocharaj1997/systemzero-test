"""
Unit tests for BronzeIngester.
"""

import json
import pytest
from pathlib import Path

import polars as pl

from src.bronze.ingester import BronzeIngester


class TestBronzeIngester:
    """Tests for Bronze layer ingestion."""
    
    def test_ingest_csv(self, sample_sources_config, sample_input_dir, tmp_path):
        """Test CSV ingestion produces correct output."""
        output_dir = tmp_path / "outputs" / "processed"
        output_dir.mkdir(parents=True)
        
        # Only test products (CSV)
        config = {
            "sources": {
                "products": sample_sources_config["sources"]["products"]
            }
        }
        
        ingester = BronzeIngester(
            sources_config=config,
            input_dir=sample_input_dir,
            output_dir=output_dir,
        )
        
        results = ingester.ingest_all()
        
        assert "products" in results
        assert results["products"]["success"] is True
        assert results["products"]["row_count"] == 5  # Including duplicate
        assert results["products"]["format"] == "csv"
        
        # Verify output CSV exists
        csv_path = output_dir / "bronze" / "products.csv"
        assert csv_path.exists()
        
        # Verify metadata columns added
        df = pl.read_csv(csv_path)
        assert "_source_file" in df.columns
        assert "_loaded_at" in df.columns
    
    def test_ingest_json(self, sample_sources_config, sample_input_dir, tmp_path):
        """Test JSON ingestion with data_key extraction."""
        output_dir = tmp_path / "outputs" / "processed"
        output_dir.mkdir(parents=True)
        
        config = {
            "sources": {
                "customers": sample_sources_config["sources"]["customers"]
            }
        }
        
        ingester = BronzeIngester(
            sources_config=config,
            input_dir=sample_input_dir,
            output_dir=output_dir,
        )
        
        results = ingester.ingest_all()
        
        assert results["customers"]["success"] is True
        assert results["customers"]["row_count"] == 3
    
    def test_ingest_missing_file(self, tmp_path):
        """Test graceful failure for missing source files."""
        output_dir = tmp_path / "outputs" / "processed"
        output_dir.mkdir(parents=True)
        
        config = {
            "sources": {
                "missing": {
                    "file": "nonexistent.csv",
                    "format": "csv",
                }
            }
        }
        
        ingester = BronzeIngester(
            sources_config=config,
            input_dir=tmp_path / "data",
            output_dir=output_dir,
        )
        
        results = ingester.ingest_all()
        
        assert results["missing"]["success"] is False
        assert "error" in results["missing"]
    
    def test_ingest_all_sources(self, sample_sources_config, sample_input_dir, tmp_path):
        """Test ingesting multiple sources at once."""
        output_dir = tmp_path / "outputs" / "processed"
        output_dir.mkdir(parents=True)
        
        ingester = BronzeIngester(
            sources_config=sample_sources_config,
            input_dir=sample_input_dir,
            output_dir=output_dir,
        )
        
        results = ingester.ingest_all()
        
        # All three should succeed
        for source_name in ["products", "customers", "vendors"]:
            assert results[source_name]["success"] is True
            assert results[source_name]["row_count"] > 0
    
    def test_metadata_columns(self, sample_sources_config, sample_input_dir, tmp_path):
        """Test that metadata columns are correctly added."""
        output_dir = tmp_path / "outputs" / "processed"
        output_dir.mkdir(parents=True)
        
        config = {
            "sources": {
                "vendors": sample_sources_config["sources"]["vendors"]
            }
        }
        
        ingester = BronzeIngester(
            sources_config=config,
            input_dir=sample_input_dir,
            output_dir=output_dir,
        )
        
        results = ingester.ingest_all()
        
        df = pl.read_csv(output_dir / "bronze" / "vendors.csv")
        assert "_source_file" in df.columns
        assert "_loaded_at" in df.columns
        # Source file should be the original filename
        assert df["_source_file"][0] == "vendors.json"
