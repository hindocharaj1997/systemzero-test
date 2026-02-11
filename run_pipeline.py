"""
Medallion Pipeline Runner.

Orchestrates the Bronze → Silver → Gold → Graph pipeline.

Technologies:
- Bronze: Polars (file I/O)
- Silver: Polars + Pydantic (cleaning + validation + dedup + FK checks)
- Gold: DuckDB SQL (analytical queries)
- Graph: SurrealDB (graph modeling + loading)
"""

import sys
import asyncio
import argparse
from pathlib import Path
from datetime import datetime

import yaml
from loguru import logger

from src.bronze import BronzeIngester
from src.silver import SilverProcessor
from src.gold import GoldProcessor
from src.utils.quality_report import generate_quality_report


def setup_logging(log_dir: Path, verbose: bool = False) -> None:
    """Configure logging."""
    log_dir.mkdir(parents=True, exist_ok=True)
    
    logger.remove()
    
    level = "DEBUG" if verbose else "INFO"
    logger.add(
        sys.stdout,
        format="<green>{time:HH:mm:ss}</green> | <level>{level: <8}</level> | <cyan>{extra[component]}</cyan> | {message}",
        level=level,
        filter=lambda record: record["extra"].get("component"),
    )
    
    log_file = log_dir / f"pipeline_{datetime.now().strftime('%Y%m%d_%H%M%S')}.log"
    logger.add(
        log_file,
        format="{time:YYYY-MM-DD HH:mm:ss} | {level: <8} | {extra[component]} | {message}",
        level="DEBUG",
    )


def load_configs(config_dir: Path) -> dict:
    """Load all YAML configurations."""
    configs = {}
    
    config_files = {
        "pipeline": "pipeline_config.yaml",
        "sources": "sources.yaml",
        "schemas": "schemas.yaml",
        "cleaning_rules": "cleaning_rules.yaml",
    }
    
    for name, filename in config_files.items():
        path = config_dir / filename
        if path.exists():
            with open(path) as f:
                configs[name] = yaml.safe_load(f)
        else:
            logger.warning(f"Config not found: {path}")
            configs[name] = {}
    
    return configs


def run_pipeline(
    layers: list[str] = None,
    verbose: bool = False,
    fresh: bool = False,
) -> dict:
    """
    Run the medallion pipeline.
    
    Args:
        layers: Specific layers to run (bronze, silver, gold, graph), or None for all
        verbose: Enable verbose logging
        fresh: Delete existing outputs and start fresh
        
    Returns:
        Dictionary with pipeline results
    """
    project_dir = Path(__file__).parent
    config_dir = project_dir / "config"
    output_dir = project_dir / "outputs"
    logs_dir = output_dir / "logs"
    
    setup_logging(logs_dir, verbose)
    log = logger.bind(component="Pipeline")
    
    configs = load_configs(config_dir)
    pipeline_config = configs["pipeline"]
    
    # Clean outputs if fresh
    if fresh:
        import shutil
        for subdir in ["bronze", "silver", "gold"]:
            path = output_dir / "processed" / subdir
            if path.exists():
                shutil.rmtree(path)
                log.info(f"Cleaned: {path}")
        quarantine_path = output_dir / "quarantine"
        if quarantine_path.exists():
            shutil.rmtree(quarantine_path)
            log.info(f"Cleaned: {quarantine_path}")
    
    if layers is None:
        layers = ["bronze", "silver", "gold", "graph"]
    
    results = {
        "started_at": datetime.now().isoformat(),
        "layers": {},
    }
    
    # Keep raw result objects for quality report
    bronze_results_raw = {}
    silver_results_raw = {}
    graph_results_raw = {}
    gold_results_raw = {}
    
    log.info("=" * 70)
    log.info("MEDALLION PIPELINE")
    log.info("=" * 70)
    log.info(f"Layers: {', '.join(layers)}")
    log.info(f"Tech: Polars (Bronze/Silver) → DuckDB SQL (Gold) → SurrealDB (Graph)")
    
    try:
        # Bronze Layer (Polars)
        if "bronze" in layers:
            ingester = BronzeIngester(
                sources_config=configs["sources"],
                input_dir=Path(pipeline_config["paths"]["input_dir"]),
                output_dir=Path(pipeline_config["paths"]["output_dir"]),
            )
            bronze_results_raw = ingester.ingest_all()
            results["layers"]["bronze"] = bronze_results_raw
        
        # Silver Layer (Polars + Pydantic + Dedup + FK)
        if "silver" in layers:
            processor = SilverProcessor(
                sources_config=configs["sources"],
                schemas_config=configs["schemas"],
                cleaning_rules=configs["cleaning_rules"],
                bronze_dir=Path(pipeline_config["paths"]["output_dir"]) / "bronze",
                output_dir=Path(pipeline_config["paths"]["output_dir"]),
            )
            silver_results_raw = processor.process_all()
            results["layers"]["silver"] = {
                name: {
                    "valid": r.valid_records,
                    "quarantined": r.quarantined_records,
                    "duplicates_removed": r.duplicates_removed,
                    "orphaned_records": r.orphaned_records,
                    "pass_rate": r.pass_rate,
                }
                for name, r in silver_results_raw.items()
            }
        
        # Gold Layer (DuckDB SQL)
        if "gold" in layers:
            db_path = Path(pipeline_config.get("duckdb", {}).get(
                "database_path", "outputs/pipeline.duckdb"
            ))
            gold_processor = GoldProcessor(
                silver_dir=Path(pipeline_config["paths"]["output_dir"]) / "silver",
                output_dir=Path(pipeline_config["paths"]["output_dir"]),
                db_path=db_path,
            )
            gold_results_raw = gold_processor.process_all()
            gold_processor.close()
            
            results["layers"]["gold"] = {
                name: {
                    "rows": r.row_count,
                    "features": len(r.columns),
                }
                for name, r in gold_results_raw.items()
            }
        
        # Graph Layer (SurrealDB)
        if "graph" in layers:
            try:
                from src.graph import GraphLoader

                surrealdb_config = pipeline_config.get("surrealdb", {})
                silver_dir = Path(pipeline_config["paths"]["output_dir"]) / "silver"
                graph_loader = GraphLoader(
                    silver_dir=silver_dir,
                    config=surrealdb_config,
                )
                graph_results_raw = asyncio.run(graph_loader.load_all())
                asyncio.run(graph_loader.close())

                results["layers"]["graph"] = {
                    name: {
                        "type": r.table_type,
                        "loaded": r.records_loaded,
                        "errors": r.errors,
                    }
                    for name, r in graph_results_raw.items()
                }
            except ImportError:
                log.warning(
                    "SurrealDB SDK not installed. "
                    "Install with: pip install surrealdb>=0.3.0"
                )
            except Exception as e:
                log.error(f"Graph layer failed: {e}")
                results["layers"]["graph"] = {"error": str(e)}

        results["status"] = "success"
        
        # Generate quality report
        if bronze_results_raw and silver_results_raw and gold_results_raw:
            report_path = output_dir / "quality_report.md"
            generate_quality_report(
                bronze_results=bronze_results_raw,
                silver_results=silver_results_raw,
                gold_results=gold_results_raw,
                output_path=report_path,
            )
        
    except Exception as e:
        log.error(f"Pipeline failed: {e}")
        results["status"] = "failed"
        results["error"] = str(e)
        raise
    
    results["completed_at"] = datetime.now().isoformat()
    
    log.info("=" * 70)
    log.info("PIPELINE COMPLETE")
    log.info("=" * 70)
    
    return results


def main():
    """CLI entry point."""
    parser = argparse.ArgumentParser(description="Run Medallion Pipeline")
    parser.add_argument(
        "--layers",
        nargs="+",
        choices=["bronze", "silver", "gold", "graph"],
        help="Specific layers to run",
    )
    parser.add_argument(
        "--fresh",
        action="store_true",
        help="Delete existing outputs and start fresh",
    )
    parser.add_argument(
        "--verbose", "-v",
        action="store_true",
        help="Enable verbose logging",
    )
    
    args = parser.parse_args()
    
    results = run_pipeline(
        layers=args.layers,
        verbose=args.verbose,
        fresh=args.fresh,
    )
    
    # Print summary
    if results["status"] == "success":
        print("\n✓ Pipeline completed successfully")
        
        if "bronze" in results.get("layers", {}):
            bronze = results["layers"]["bronze"]
            total = sum(r.get("row_count", 0) for r in bronze.values() if isinstance(r, dict))
            print(f"  Bronze (Polars): {len(bronze)} sources → {total} rows")
        
        if "silver" in results.get("layers", {}):
            silver = results["layers"]["silver"]
            total_valid = sum(r.get("valid", 0) for r in silver.values())
            total_deduped = sum(r.get("duplicates_removed", 0) for r in silver.values())
            total_orphaned = sum(r.get("orphaned_records", 0) for r in silver.values())
            print(f"  Silver (Polars + Pydantic): {total_valid} valid, {total_deduped} deduped, {total_orphaned} orphaned")
        
        if "gold" in results.get("layers", {}):
            gold = results["layers"]["gold"]
            total_features = sum(r.get("features", 0) for r in gold.values())
            print(f"  Gold (DuckDB SQL): {len(gold)} tables, {total_features} features")

        if "graph" in results.get("layers", {}):
            graph = results["layers"]["graph"]
            if "error" not in graph:
                total_nodes = sum(
                    r.get("loaded", 0) for r in graph.values()
                    if isinstance(r, dict) and r.get("type") == "node"
                )
                total_edges = sum(
                    r.get("loaded", 0) for r in graph.values()
                    if isinstance(r, dict) and r.get("type") == "edge"
                )
                print(f"  Graph (SurrealDB): {total_nodes} nodes, {total_edges} edges")
            else:
                print(f"  Graph (SurrealDB): skipped – {graph['error']}")

        print(f"  Quality report → outputs/quality_report.md")
    else:
        print(f"\n✗ Pipeline failed: {results.get('error', 'Unknown error')}")
        sys.exit(1)


if __name__ == "__main__":
    main()
