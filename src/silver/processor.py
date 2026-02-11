"""
Silver Layer Processor - Polars + Pydantic.

Orchestrates cleaning, deduplication, referential integrity, and validation
from Bronze to Silver using Polars for data manipulation and Pydantic for
schema validation.
"""

from pathlib import Path
from typing import Dict, Any, List, Optional, Set
from datetime import datetime
from dataclasses import dataclass, field
import json

import polars as pl
import yaml
from dateutil import parser as date_parser
from pydantic import ValidationError
from loguru import logger

from .cleaner import SilverCleaner
from .schemas import get_pydantic_schema


@dataclass
class ProcessingResult:
    """Result of processing a single source."""
    source_name: str
    total_records: int = 0
    valid_records: int = 0
    quarantined_records: int = 0
    duplicates_removed: int = 0
    orphaned_records: int = 0
    fields_cleaned: Dict[str, int] = field(default_factory=dict)
    error_counts: Dict[str, int] = field(default_factory=dict)
    
    @property
    def pass_rate(self) -> float:
        if self.total_records == 0:
            return 0.0
        return self.valid_records / self.total_records


class SilverProcessor:
    """
    Processes data from Bronze to Silver layer using Polars.
    
    Pipeline per source:
    1. Read Bronze CSV
    2. Apply Polars-based cleaning (case, phone, boolean normalization)
    3. Deduplicate on primary key
    4. Validate referential integrity (foreign keys)
    5. Validate each row with Pydantic schemas
    6. Write valid records to Silver, quarantine invalid
    """
    
    def __init__(
        self,
        sources_config: Dict[str, Any],
        schemas_config: Dict[str, Any],
        cleaning_rules: Dict[str, Any],
        bronze_dir: Path,
        output_dir: Path,
    ):
        self.sources = sources_config.get("sources", {})
        self.schemas_config = schemas_config.get("schemas", {})
        self.cleaner = SilverCleaner(cleaning_rules)
        self.bronze_dir = Path(bronze_dir)
        self.output_dir = Path(output_dir) / "silver"
        self.quarantine_dir = Path(output_dir).parent / "quarantine"
        self.output_dir.mkdir(parents=True, exist_ok=True)
        self.quarantine_dir.mkdir(parents=True, exist_ok=True)
        self.logger = logger.bind(component="SilverProcessor")
        
        # Cache of valid primary keys per entity for referential integrity
        self._valid_keys: Dict[str, Set[str]] = {}
    
    def process_all(self) -> Dict[str, ProcessingResult]:
        """
        Process all sources from Bronze to Silver.
        
        Processing order matters for referential integrity:
        vendors → products → customers → transactions → invoices → reviews → tickets → calls
        """
        self.logger.info("=" * 60)
        self.logger.info("SILVER LAYER: Cleaning and validating (Polars + Pydantic)")
        self.logger.info("=" * 60)
        
        # Process in dependency order so FK lookups work
        ordered_sources = self._get_processing_order()
        
        results = {}
        
        for source_name in ordered_sources:
            if source_name not in self.sources:
                continue
            config = self.sources[source_name]
            schema_name = config.get("schema", source_name)
            result = self._process_source(source_name, schema_name)
            results[source_name] = result
            
            status = "✓" if result.valid_records > 0 else "✗"
            extras = []
            if result.duplicates_removed > 0:
                extras.append(f"{result.duplicates_removed} deduped")
            if result.orphaned_records > 0:
                extras.append(f"{result.orphaned_records} orphaned")
            extra_str = f" ({', '.join(extras)})" if extras else ""
            
            self.logger.info(
                f"{status} {source_name}: {result.valid_records}/{result.total_records} "
                f"valid ({result.pass_rate:.1%}){extra_str}"
            )
        
        # Post-processing: parse line_items_json from invoices
        if "invoices" in results and results["invoices"].valid_records > 0:
            self._parse_invoice_line_items()
        
        # Summary
        total_valid = sum(r.valid_records for r in results.values())
        total_quarantined = sum(r.quarantined_records for r in results.values())
        total_deduped = sum(r.duplicates_removed for r in results.values())
        total_orphaned = sum(r.orphaned_records for r in results.values())
        total_cleaned = sum(sum(r.fields_cleaned.values()) for r in results.values())
        
        self.logger.info(
            f"Silver complete: {total_valid} valid, {total_quarantined} quarantined, "
            f"{total_deduped} deduped, {total_orphaned} orphaned, {total_cleaned} fields cleaned"
        )
        
        return results
    
    def _get_processing_order(self) -> List[str]:
        """Return sources in dependency order for FK validation."""
        return [
            "vendors",        # No dependencies
            "products",       # FK: vendor_id → vendors
            "customers",      # No dependencies
            "transactions",   # FK: customer_id, product_id
            "invoices",       # FK: vendor_id
            "reviews",        # FK: product_id, customer_id
            "support_tickets",# FK: customer_id, product_id
            "call_transcripts", # FK: customer_id, ticket_id
        ]
    
    def _process_source(
        self,
        source_name: str,
        schema_name: str,
    ) -> ProcessingResult:
        """Process a single source: clean → dedup → FK check → validate."""
        result = ProcessingResult(source_name=source_name)
        
        # Read Bronze CSV
        bronze_path = self.bronze_dir / f"{source_name}.csv"
        if not bronze_path.exists():
            self.logger.error(f"Bronze file not found: {bronze_path}")
            return result
        
        df = pl.read_csv(bronze_path, infer_schema_length=None)
        result.total_records = len(df)
        
        # Get schema definition
        schema_def = self.schemas_config.get(schema_name, {})
        fields = schema_def.get("fields", {})
        primary_key = schema_def.get("primary_key")
        
        # Step 1: Apply Polars-based cleaning
        df, cleaning_stats = self._apply_cleaning(df, fields)
        result.fields_cleaned = cleaning_stats
        
        # Step 2: Deduplicate on primary key
        if primary_key and primary_key in df.columns:
            before = len(df)
            df = df.unique(subset=[primary_key], keep="first")
            result.duplicates_removed = before - len(df)
        
        # Step 3: Validate referential integrity
        orphan_indices = set()
        fk_errors: Dict[int, List[Dict]] = {}
        
        for field_name, field_def in fields.items():
            fk_target = field_def.get("foreign_key")
            if not fk_target or field_name not in df.columns:
                continue
            
            valid_keys = self._valid_keys.get(fk_target, set())
            if not valid_keys:
                continue
            
            # Check each value
            col_values = df[field_name].to_list()
            for idx, val in enumerate(col_values):
                if val is not None and str(val) != "" and str(val) not in valid_keys:
                    orphan_indices.add(idx)
                    if idx not in fk_errors:
                        fk_errors[idx] = []
                    fk_errors[idx].append({
                        "field": field_name,
                        "value": str(val),
                        "target": fk_target,
                        "type": "referential_integrity",
                        "msg": f"No matching {fk_target} record for {field_name}={val}",
                    })
        
        result.orphaned_records = len(orphan_indices)
        
        # Step 4: Validate with Pydantic
        try:
            pydantic_schema = get_pydantic_schema(schema_name)
        except ValueError:
            self.logger.warning(f"No Pydantic schema for {schema_name}, skipping validation")
            df.write_csv(self.output_dir / f"{source_name}.csv")
            result.valid_records = len(df)
            self._cache_valid_keys(schema_name, primary_key, df)
            return result
        
        valid_records = []
        quarantined_records = []
        
        for row_idx, row in enumerate(df.iter_rows(named=True)):
            # Check FK violations first
            if row_idx in orphan_indices:
                result.quarantined_records += 1
                result.error_counts["referential_integrity"] = (
                    result.error_counts.get("referential_integrity", 0) + 1
                )
                quarantined_records.append({
                    "row_index": row_idx,
                    "record": {k: str(v) if v is not None else None for k, v in row.items()},
                    "errors": fk_errors.get(row_idx, []),
                })
                continue
            
            try:
                validated = pydantic_schema.model_validate(row)
                valid_records.append(validated.model_dump())
            except ValidationError as e:
                result.quarantined_records += 1
                for err in e.errors():
                    err_type = err.get("type", "unknown")
                    result.error_counts[err_type] = result.error_counts.get(err_type, 0) + 1
                quarantined_records.append({
                    "row_index": row_idx,
                    "record": {k: str(v) if v is not None else None for k, v in row.items()},
                    "errors": [
                        {
                            "field": ".".join(str(x) for x in err["loc"]),
                            "type": err["type"],
                            "msg": err["msg"],
                        }
                        for err in e.errors()
                    ],
                })
        
        result.valid_records = len(valid_records)
        
        # Write valid records
        if valid_records:
            valid_df = pl.DataFrame(valid_records)
            valid_df.write_csv(self.output_dir / f"{source_name}.csv")
            # Cache valid primary keys for FK lookups by downstream sources
            self._cache_valid_keys(schema_name, primary_key, valid_df)
        
        # Write quarantine
        if quarantined_records:
            self._save_quarantine(source_name, quarantined_records)
        
        return result
    
    def _cache_valid_keys(
        self,
        schema_name: str,
        primary_key: Optional[str],
        df: pl.DataFrame,
    ) -> None:
        """Cache valid primary keys for referential integrity checks."""
        if primary_key and primary_key in df.columns:
            keys = set(str(v) for v in df[primary_key].to_list() if v is not None)
            self._valid_keys[schema_name] = keys
            self.logger.debug(f"Cached {len(keys)} valid keys for {schema_name}")
    
    def _apply_cleaning(
        self,
        df: pl.DataFrame,
        fields: Dict[str, Any],
    ) -> tuple[pl.DataFrame, Dict[str, int]]:
        """Apply Polars-based cleaning transformations."""
        stats = {}
        
        for field_name, field_def in fields.items():
            if field_name not in df.columns:
                continue
            
            clean_rule = field_def.get("clean")
            if not clean_rule:
                continue
            
            try:
                if clean_rule == "lowercase":
                    if df[field_name].dtype in (pl.String, pl.Utf8):
                        df = df.with_columns(
                            pl.col(field_name).str.to_lowercase().alias(field_name)
                        )
                        stats[field_name] = stats.get(field_name, 0) + 1
                    
                elif clean_rule == "uppercase":
                    if df[field_name].dtype in (pl.String, pl.Utf8):
                        df = df.with_columns(
                            pl.col(field_name).str.to_uppercase().alias(field_name)
                        )
                        stats[field_name] = stats.get(field_name, 0) + 1
                    
                elif clean_rule == "phone_normalize":
                    if df[field_name].dtype in (pl.String, pl.Utf8):
                        df = df.with_columns(
                            pl.col(field_name)
                            .str.replace_all(r"[\(\)\-\s\.\+]", "")
                            .alias(field_name)
                        )
                        stats[field_name] = stats.get(field_name, 0) + 1
                    
                elif clean_rule == "boolean_normalize":
                    if df[field_name].dtype in (pl.String, pl.Utf8):
                        df = df.with_columns(
                            pl.when(pl.col(field_name).str.to_lowercase().is_in(
                                ["true", "yes", "1", "y"]
                            ))
                            .then(pl.lit(True))
                            .when(pl.col(field_name).str.to_lowercase().is_in(
                                ["false", "no", "0", "n"]
                            ))
                            .then(pl.lit(False))
                            .otherwise(pl.lit(None))
                            .alias(field_name)
                        )
                        stats[field_name] = stats.get(field_name, 0) + 1
                    
                elif clean_rule == "date_iso":
                    if df[field_name].dtype in (pl.String, pl.Utf8):
                        def _normalize_date(val):
                            if val is None or val == "":
                                return val
                            try:
                                parsed = date_parser.parse(str(val), dayfirst=False, fuzzy=False)
                                return parsed.strftime("%Y-%m-%d")
                            except (ValueError, TypeError, OverflowError):
                                return val  # Leave unparseable dates for Pydantic to catch

                        df = df.with_columns(
                            pl.col(field_name)
                            .map_elements(_normalize_date, return_dtype=pl.String)
                            .alias(field_name)
                        )
                        stats[field_name] = stats.get(field_name, 0) + 1
                    
            except Exception as e:
                self.logger.warning(f"Cleaning {field_name} with {clean_rule} failed: {e}")
        
        return df, stats
    
    def _save_quarantine(
        self,
        source_name: str,
        records: List[Dict[str, Any]],
    ) -> None:
        """Save quarantined records to JSON."""
        output_path = self.quarantine_dir / f"{source_name}_quarantine.json"
        with open(output_path, 'w') as f:
            json.dump(records, f, indent=2, default=str)
        self.logger.debug(f"Saved {len(records)} quarantined → {output_path.name}")
    
    def _parse_invoice_line_items(self) -> None:
        """
        Parse line_items_json from Bronze invoices into a separate Silver table.
        
        Only includes line items for invoices that passed Silver validation.
        Each line item is validated through InvoiceLineItemSchema.
        Invalid line items are quarantined.
        """
        from .schemas import InvoiceLineItemSchema
        
        # Read from Bronze (source of line_items_json column) but filter to
        # only invoices that passed Silver validation
        bronze_path = self.bronze_dir / "invoices.csv"
        silver_path = self.output_dir / "invoices.csv"
        
        if not bronze_path.exists() or not silver_path.exists():
            return
        
        bronze_df = pl.read_csv(bronze_path, infer_schema_length=None)
        silver_df = pl.read_csv(silver_path, infer_schema_length=None)
        
        if "line_items_json" not in bronze_df.columns:
            self.logger.debug("No line_items_json column in invoices")
            return
        
        # Only process line items for validated (Silver) invoice IDs
        valid_invoice_ids = set(silver_df["invoice_id"].to_list())
        
        all_line_items = []
        quarantined = []
        
        for row in bronze_df.iter_rows(named=True):
            invoice_id = row.get("invoice_id")
            raw_json = row.get("line_items_json")
            source_file = row.get("_source_file")
            loaded_at = row.get("_loaded_at")
            
            if not raw_json or not invoice_id:
                continue
            
            # Skip line items for invoices that didn't pass Silver validation
            if invoice_id not in valid_invoice_ids:
                continue
            
            try:
                items = json.loads(raw_json)
                for idx, item in enumerate(items):
                    item["invoice_id"] = invoice_id
                    if source_file:
                        item["_source_file"] = source_file
                    if loaded_at:
                        item["_loaded_at"] = loaded_at
                    
                    # Validate through Pydantic schema
                    try:
                        validated = InvoiceLineItemSchema(**item)
                        all_line_items.append(validated.model_dump())
                    except ValidationError as e:
                        quarantined.append({
                            "row_index": idx,
                            "record": item,
                            "errors": [
                                {"field": err["loc"][-1] if err["loc"] else "unknown",
                                 "type": err["type"],
                                 "msg": err["msg"]}
                                for err in e.errors()
                            ],
                        })
            except (json.JSONDecodeError, TypeError):
                continue
        
        if all_line_items:
            line_items_df = pl.DataFrame(all_line_items)
            output_path = self.output_dir / "invoice_line_items.csv"
            line_items_df.write_csv(output_path)
            self.logger.info(
                f"✓ invoice_line_items: {len(all_line_items)} valid line items "
                f"({len(quarantined)} quarantined)"
            )
        
        if quarantined:
            self._save_quarantine("invoice_line_items", quarantined)


