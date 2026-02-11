"""
Bronze Layer Ingester - Polars-based.

Loads raw data from source files using Polars.
Flattens nested structures and outputs to CSV.
"""

import json
from pathlib import Path
from typing import Dict, Any, Optional
from datetime import datetime

import polars as pl
import yaml
from loguru import logger


class BronzeIngester:
    """
    Ingests raw data into Bronze layer using Polars.
    
    - Loads all file formats (CSV, JSON, Parquet)
    - Flattens nested JSON structures
    - Adds metadata columns (_source_file, _loaded_at)
    - Exports to CSV
    """
    
    def __init__(
        self,
        sources_config: Dict[str, Any],
        input_dir: Path,
        output_dir: Path,
    ):
        """
        Initialize Bronze ingester.
        
        Args:
            sources_config: Source configuration from sources.yaml
            input_dir: Directory containing source files
            output_dir: Directory for Bronze CSV exports
        """
        self.sources = sources_config.get("sources", {})
        self.input_dir = Path(input_dir)
        self.output_dir = Path(output_dir) / "bronze"
        self.output_dir.mkdir(parents=True, exist_ok=True)
        self.logger = logger.bind(component="BronzeIngester")
    
    def ingest_all(self) -> Dict[str, Dict[str, Any]]:
        """
        Ingest all configured sources into Bronze layer.
        
        Returns:
            Dictionary with ingestion results per source.
        """
        self.logger.info("=" * 60)
        self.logger.info("BRONZE LAYER: Ingesting raw data")
        self.logger.info("=" * 60)
        
        results = {}
        load_timestamp = datetime.now().isoformat()
        
        for source_name, config in self.sources.items():
            try:
                result = self._ingest_source(source_name, config, load_timestamp)
                results[source_name] = result
                
                status = "✓" if result["success"] else "✗"
                self.logger.info(
                    f"{status} {source_name}: {result.get('row_count', 0)} rows"
                )
                
            except Exception as e:
                self.logger.error(f"Failed to ingest {source_name}: {e}")
                results[source_name] = {
                    "success": False,
                    "error": str(e),
                }
        
        # Summary
        success_count = sum(1 for r in results.values() if r.get("success"))
        total_rows = sum(r.get("row_count", 0) for r in results.values())
        self.logger.info(f"Bronze complete: {success_count}/{len(results)} sources, {total_rows} total rows")
        
        return results
    
    def _ingest_source(
        self,
        source_name: str,
        config: Dict[str, Any],
        load_timestamp: str,
    ) -> Dict[str, Any]:
        """Ingest a single source into Bronze."""
        file_name = config["file"]
        file_format = config["format"]
        file_path = self.input_dir / file_name
        
        if not file_path.exists():
            raise FileNotFoundError(f"Source file not found: {file_path}")
        
        # Load based on format using Polars
        if file_format == "csv":
            df = self._load_csv(file_path)
        elif file_format == "json":
            df = self._load_json(file_path, config.get("data_key"))
        elif file_format == "parquet":
            df = self._load_parquet(file_path)
        else:
            raise ValueError(f"Unsupported format: {file_format}")
        
        # Add metadata columns
        df = df.with_columns([
            pl.lit(file_name).alias("_source_file"),
            pl.lit(load_timestamp).alias("_loaded_at"),
        ])
        
        # Export to CSV
        csv_path = self.output_dir / f"{source_name}.csv"
        df.write_csv(csv_path)
        
        return {
            "success": True,
            "row_count": len(df),
            "source_file": file_name,
            "format": file_format,
            "columns": df.columns,
            "csv_export": str(csv_path),
        }
    
    def _load_csv(self, file_path: Path) -> pl.DataFrame:
        """Load CSV file with Polars."""
        return pl.read_csv(
            file_path,
            infer_schema_length=None,  # Scan all rows for schema
            try_parse_dates=False,      # Keep dates as strings for Bronze
        )
    
    def _load_json(self, file_path: Path, data_key: Optional[str] = None) -> pl.DataFrame:
        """Load JSON file with Polars."""
        with open(file_path, 'r') as f:
            raw = json.load(f)
        
        # Extract data array
        if data_key:
            records = raw.get(data_key, [])
        elif isinstance(raw, list):
            records = raw
        else:
            # Find first list in the dict
            for v in raw.values():
                if isinstance(v, list):
                    records = v
                    break
            else:
                records = []
        
        if not records:
            return pl.DataFrame()
        
        # Flatten nested structures
        flattened_records = [self._flatten_record(r) for r in records]
        
        return pl.DataFrame(flattened_records, infer_schema_length=None)
    
    def _load_parquet(self, file_path: Path) -> pl.DataFrame:
        """Load Parquet file with Polars."""
        return pl.read_parquet(file_path)
    
    def _flatten_record(self, record: Dict[str, Any], prefix: str = "") -> Dict[str, Any]:
        """
        Flatten nested dictionaries and convert complex types to JSON strings.
        
        Args:
            record: Record to flatten
            prefix: Prefix for nested keys
            
        Returns:
            Flattened record with JSON-serialized complex types
        """
        result = {}
        
        for key, value in record.items():
            full_key = f"{prefix}{key}" if prefix else key
            
            if isinstance(value, dict):
                # Check if it's a simple nested dict to flatten OR complex to serialize
                if self._is_simple_dict(value):
                    # Flatten simple nested dicts (like address)
                    nested = self._flatten_record(value, f"{full_key}_")
                    result.update(nested)
                else:
                    # Serialize complex dicts as JSON
                    result[full_key] = json.dumps(value, default=str)
            elif isinstance(value, list):
                # Serialize lists as JSON
                result[full_key] = json.dumps(value, default=str)
            elif isinstance(value, (datetime,)):
                result[full_key] = value.isoformat()
            else:
                result[full_key] = value
        
        return result
    
    def _is_simple_dict(self, d: Dict[str, Any]) -> bool:
        """Check if dict contains only simple types (no nested dicts/lists)."""
        for v in d.values():
            if isinstance(v, (dict, list)):
                return False
        return True
