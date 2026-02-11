"""
Data Quality Report Generator.

Generates a markdown quality report from pipeline results.
"""

from pathlib import Path
from typing import Dict, Any
from datetime import datetime
import json

from loguru import logger


def generate_quality_report(
    bronze_results: Dict[str, Any],
    silver_results: Dict[str, Any],
    gold_results: Dict[str, Any],
    output_path: Path,
) -> Path:
    """
    Generate a markdown data quality report.
    
    Args:
        bronze_results: Results from Bronze layer
        silver_results: Results from Silver layer (ProcessingResult objects)
        gold_results: Results from Gold layer
        output_path: Path to write the report
        
    Returns:
        Path to the generated report
    """
    log = logger.bind(component="QualityReport")
    
    lines = []
    lines.append("# Data Quality Report")
    lines.append(f"\n**Generated:** {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    lines.append(f"\n**Pipeline:** Medallion Architecture (Polars → DuckDB)")
    lines.append("")
    
    # --- Bronze Summary ---
    lines.append("---")
    lines.append("\n## Bronze Layer (Ingestion)")
    lines.append("")
    lines.append("| Source | Format | Records | Status |")
    lines.append("|--------|--------|---------|--------|")
    
    total_bronze = 0
    for source, result in bronze_results.items():
        if isinstance(result, dict):
            rows = result.get("row_count", 0)
            fmt = result.get("format", "?")
            success = "✓" if result.get("success") else "✗"
            total_bronze += rows
            lines.append(f"| {source} | {fmt} | {rows:,} | {success} |")
    
    lines.append(f"| **Total** | | **{total_bronze:,}** | |")
    lines.append("")
    
    # --- Silver Summary ---
    lines.append("---")
    lines.append("\n## Silver Layer (Cleaning & Validation)")
    lines.append("")
    lines.append("| Source | Total | Valid | Quarantined | Deduped | Orphaned | Pass Rate |")
    lines.append("|--------|-------|-------|-------------|---------|----------|-----------|")
    
    total_valid = 0
    total_quarantined = 0
    total_deduped = 0
    total_orphaned = 0
    
    for source, result in silver_results.items():
        total = result.total_records
        valid = result.valid_records
        quarantined = result.quarantined_records
        deduped = result.duplicates_removed
        orphaned = result.orphaned_records
        rate = f"{result.pass_rate:.1%}"
        
        total_valid += valid
        total_quarantined += quarantined
        total_deduped += deduped
        total_orphaned += orphaned
        
        lines.append(
            f"| {source} | {total:,} | {valid:,} | {quarantined} | "
            f"{deduped} | {orphaned} | {rate} |"
        )
    
    overall_rate = total_valid / total_bronze * 100 if total_bronze > 0 else 0
    lines.append(
        f"| **Total** | **{total_bronze:,}** | **{total_valid:,}** | "
        f"**{total_quarantined}** | **{total_deduped}** | **{total_orphaned}** | "
        f"**{overall_rate:.1f}%** |"
    )
    lines.append("")
    
    # --- Error Breakdown ---
    lines.append("### Validation Error Breakdown")
    lines.append("")
    
    all_errors: Dict[str, int] = {}
    for result in silver_results.values():
        for err_type, count in result.error_counts.items():
            all_errors[err_type] = all_errors.get(err_type, 0) + count
    
    if all_errors:
        lines.append("| Error Type | Count |")
        lines.append("|------------|-------|")
        for err_type, count in sorted(all_errors.items(), key=lambda x: -x[1]):
            lines.append(f"| {err_type} | {count} |")
    else:
        lines.append("No validation errors found.")
    lines.append("")
    
    # --- Cleaning Rules Applied ---
    lines.append("### Cleaning Rules Applied")
    lines.append("")
    lines.append("| Source | Fields Cleaned |")
    lines.append("|--------|---------------|")
    for source, result in silver_results.items():
        if result.fields_cleaned:
            fields = ", ".join(result.fields_cleaned.keys())
            lines.append(f"| {source} | {fields} |")
    lines.append("")
    
    # --- Deduplication Details ---
    if total_deduped > 0:
        lines.append("### Deduplication")
        lines.append("")
        lines.append("| Source | Duplicates Removed |")
        lines.append("|--------|-------------------|")
        for source, result in silver_results.items():
            if result.duplicates_removed > 0:
                lines.append(f"| {source} | {result.duplicates_removed} |")
        lines.append("")
    
    # --- Referential Integrity ---
    if total_orphaned > 0:
        lines.append("### Referential Integrity Violations")
        lines.append("")
        lines.append("| Source | Orphaned Records | Details |")
        lines.append("|--------|-----------------|---------|")
        for source, result in silver_results.items():
            if result.orphaned_records > 0:
                lines.append(
                    f"| {source} | {result.orphaned_records} | "
                    f"Invalid foreign key references |"
                )
        lines.append("")
    
    # --- Gold Summary ---
    lines.append("---")
    lines.append("\n## Gold Layer (Feature Engineering)")
    lines.append("")
    lines.append("| Feature Table | Rows | Features |")
    lines.append("|---------------|------|----------|")
    
    for name, result in gold_results.items():
        rows = result.row_count
        features = len(result.columns)
        lines.append(f"| {name} | {rows:,} | {features} |")
    
    total_features = sum(len(r.columns) for r in gold_results.values())
    lines.append(f"| **Total** | | **{total_features}** |")
    lines.append("")
    
    # --- Known Issues Detected ---
    lines.append("---")
    lines.append("\n## Data Quality Issues Detected")
    lines.append("")
    lines.append("Based on the README's known issues list:")
    lines.append("")
    
    issues = [
        ("Duplicate SKUs", "products", "Deduplication on product_id"),
        ("Negative prices", "products", "Pydantic: price >= 0"),
        ("Invalid vendor references", "products", "FK: vendor_id → vendors"),
        ("Duplicate/invalid emails", "customers", "Pydantic validation"),
        ("Inconsistent phone formats", "customers", "phone_normalize cleaning"),
        ("Future dates", "customers", "Quarantined via validation"),
        ("Negative quantities", "transactions", "Pydantic validation"),
        ("Invalid customer/product refs", "transactions", "FK validation"),
        ("Duplicate transactions", "transactions", "Deduplication on transaction_id"),
        ("Duplicate invoice numbers", "invoices", "Deduplication on invoice_id"),
        ("Invalid vendor refs in invoices", "invoices", "FK: vendor_id → vendors"),
        ("Invalid product refs in reviews", "reviews", "FK: product_id → products"),
        ("Invalid customer refs in tickets", "support_tickets", "FK: customer_id → customers"),
        ("Satisfaction scores out of range", "support_tickets", "Pydantic: 1-5 range"),
        ("Missing ticket refs in calls", "call_transcripts", "FK: ticket_id → tickets"),
    ]
    
    lines.append("| Issue | Source | How Handled |")
    lines.append("|-------|--------|-------------|")
    for issue, source, handling in issues:
        lines.append(f"| {issue} | {source} | {handling} |")
    lines.append("")
    
    # --- Quarantine Files ---
    quarantine_dir = output_path.parent / "quarantine"
    if quarantine_dir.exists():
        lines.append("---")
        lines.append("\n## Quarantine Files")
        lines.append("")
        for qf in sorted(quarantine_dir.glob("*.json")):
            try:
                with open(qf) as f:
                    records = json.load(f)
                lines.append(f"- `{qf.name}`: {len(records)} records")
            except Exception:
                lines.append(f"- `{qf.name}`: (unable to read)")
    lines.append("")
    
    # Write report
    report_text = "\n".join(lines)
    output_path.parent.mkdir(parents=True, exist_ok=True)
    with open(output_path, 'w') as f:
        f.write(report_text)
    
    log.info(f"Quality report → {output_path}")
    return output_path
