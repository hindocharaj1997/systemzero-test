"""
Integration test: end-to-end pipeline run.
"""

import pytest
from pathlib import Path

from run_pipeline import run_pipeline


class TestPipelineIntegration:
    """End-to-end pipeline integration test using real data."""
    
    def test_full_pipeline(self):
        """
        Run the full pipeline (Bronze → Silver → Gold) on real data.
        
        Validates:
        - All layers complete successfully
        - Bronze: all 8 sources ingested
        - Silver: dedup and FK validation applied
        - Gold: all 4 feature tables computed
        - Quality report generated
        """
        results = run_pipeline(
            layers=["bronze", "silver", "gold"],
            verbose=False,
            fresh=True,
        )
        
        assert results["status"] == "success"
        
        # Bronze: all 8 sources
        bronze = results["layers"]["bronze"]
        assert len(bronze) == 8
        for source, result in bronze.items():
            assert result["success"] is True, f"Bronze failed for {source}"
        
        # Silver
        silver = results["layers"]["silver"]
        assert len(silver) == 8
        
        # Verify dedup happened
        total_deduped = sum(r["duplicates_removed"] for r in silver.values())
        assert total_deduped > 0, "Expected some duplicates to be removed"
        
        # Verify FK validation happened
        total_orphaned = sum(r["orphaned_records"] for r in silver.values())
        assert total_orphaned > 0, "Expected some orphaned records"
        
        # All pass rates should be > 0
        for source, result in silver.items():
            assert result["pass_rate"] > 0, f"Silver pass rate 0 for {source}"
        
        # Gold: all 4 feature tables
        gold = results["layers"]["gold"]
        assert len(gold) == 4
        for table in ["customer_features", "product_features", "vendor_features", "invoice_features"]:
            assert table in gold, f"Missing gold table: {table}"
            assert gold[table]["rows"] > 0
            assert gold[table]["features"] > 0
    
    def test_pipeline_bronze_only(self):
        """Test running only Bronze layer."""
        results = run_pipeline(layers=["bronze"], fresh=True)
        
        assert results["status"] == "success"
        assert "bronze" in results["layers"]
        assert "silver" not in results["layers"]
        assert "gold" not in results["layers"]
    
    def test_quality_report_generated(self):
        """Test that quality report is generated after full pipeline."""
        results = run_pipeline(
            layers=["bronze", "silver", "gold"],
            fresh=True,
        )
        
        report_path = Path("outputs/quality_report.md")
        assert report_path.exists(), "Quality report not generated"
        
        content = report_path.read_text()
        assert "# Data Quality Report" in content
        assert "Bronze Layer" in content
        assert "Silver Layer" in content
        assert "Gold Layer" in content
        assert "Deduplication" in content or "Referential Integrity" in content
    
    def test_pipeline_idempotent(self):
        """Test that pipeline can be re-run safely with --fresh."""
        # Run twice
        r1 = run_pipeline(layers=["bronze", "silver", "gold"], fresh=True)
        r2 = run_pipeline(layers=["bronze", "silver", "gold"], fresh=True)
        
        assert r1["status"] == "success"
        assert r2["status"] == "success"
        
        # Results should be identical
        for source in r1["layers"]["silver"]:
            assert r1["layers"]["silver"][source]["valid"] == r2["layers"]["silver"][source]["valid"]
