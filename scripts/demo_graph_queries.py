
import asyncio
import yaml
from pprint import pprint
from pathlib import Path

import sys
from pathlib import Path

# Add project root to Python path so we can import 'src'
# Logic: script is in scripts/demo_graph_queries.py -> parent is scripts -> parent is project root
project_root = Path(__file__).resolve().parent.parent
if str(project_root) not in sys.path:
    sys.path.append(str(project_root))

from src.graph.loader import GraphLoader
from src.graph.queries import GRAPH_QUERIES

async def run_queries():
    # Load config from project root
    config_path = project_root / "config" / "pipeline_config.yaml"
    with open(config_path) as f:
        config = yaml.safe_load(f)
    
    surreal_config = config["surrealdb"]
    
    # Use standard silver output directory
    # Architecture doc says: outputs/processed/silver
    # Implementation uses: outputs/silver usually?
    # Let's check where the pipeline writes. run_pipeline.py uses:
    # silver_dir = Path(pipeline_config["paths"]["output_dir"]) / "silver"
    # Let's replicate this logic.
    output_dir_config = config.get("paths", {}).get("output_dir", "outputs/processed")
    silver_dir = project_root / output_dir_config / "silver"
    
    print(f"Loading Graph from: {silver_dir}")
    loader = GraphLoader(silver_dir, surreal_config)
    await loader.connect()
    db = loader.db
    
    print("\n=== Running Sample Graph Queries ===\n")

    for name, info in GRAPH_QUERIES.items():
        print(f"\n--- Query: {name} ---")
        print(f"Description: {info['description']}")
        
        query = info["query"]
        try:
            results = await db.query(query)
            # SurrealDB Python SDK query() (for some versions) returns the data list directly
            # or a list of responses if multiple queries.
            # Based on debug, it returns [row1, row2, ...] matching the single query.
            
            if isinstance(results, list):
                # Check if it's a list of records (dicts)
                print(f"Rows returned: {len(results)}")
                if results:
                    print("Sample Row:")
                    pprint(results[0])
            elif isinstance(results, dict) and "result" in results:
                 # It might be the wrapped format in some cases?
                 data = results["result"]
                 print(f"Rows returned: {len(data)}")
                 if data:
                    pprint(data[0])
            else:
                 print("Unexpected result format")
                 pprint(results)

        except Exception as e:
            print(f"Query failed: {e}")



    await loader.close()

if __name__ == "__main__":
    asyncio.run(run_queries())
