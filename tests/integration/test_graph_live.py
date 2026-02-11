
import pytest
import asyncio
from pathlib import Path
from src.graph.loader import GraphLoader
from surrealdb import AsyncSurreal

# Skip if surrealdb not installed or no live DB
try:
    import surrealdb
except ImportError:
    pytest.skip("surrealdb not installed", allow_module_level=True)

@pytest.mark.asyncio
async def test_live_graph_loading(tmp_path):
    """
    Test loading data into a live SurrealDB instance.
    Requires SurrealDB running at localhost:8000 (default in docker-compose).
    """
    # Create minimal silver data
    silver_dir = tmp_path / "silver"
    silver_dir.mkdir()
    
    # Create customers.csv
    (silver_dir / "customers.csv").write_text(
        "customer_id,full_name,email,registration_date,is_active\n"
        "CUS-001,Alice,alice@example.com,2024-01-01,True\n"
        "CUS-002,Bob,bob@example.com,2024-01-02,False\n"
    )
    
    # Create products.csv (for similar_to edge)
    (silver_dir / "products.csv").write_text(
        "product_id,product_name,category,price,vendor_id\n"
        "PRD-001,Widget A,Widgets,10.0,VND-001\n"
        "PRD-002,Widget B,Widgets,15.0,VND-001\n"
        "PRD-003,Gadget A,Gadgets,20.0,VND-001\n"
    )
    
    # Create vendors.csv
    (silver_dir / "vendors.csv").write_text(
        "vendor_id,vendor_name,region\n"
        "VND-001,Acme Corp,North\n"
    )
    
    # Create empty files for others to avoid file not found warnings
    for f in ["invoices.csv", "transactions.csv", "invoice_line_items.csv", "reviews.csv", "support_tickets.csv", "call_transcripts.csv"]:
        (silver_dir / f).write_text(
            "col1,col2\n"
        )

    config = {
        "url": "ws://localhost:8000/rpc",
        "namespace": "test_ns",
        "database": "test_db",
        "username": "root",
        "password": "root",
    }
    
    loader = GraphLoader(silver_dir=silver_dir, config=config)
    
    try:
        # Try connecting first. If fails, skip test (dev environment might not have DB running)
        await loader.connect()
    except Exception as e:
        pytest.skip(f"Could not connect to SurrealDB: {e}")
        return

    try:
        # Load all data
        results = await loader.load_all()
        
        # Verify nodes loaded
        assert results["customer"].records_loaded == 2
        assert results["product"].records_loaded == 3
        assert results["vendor"].records_loaded == 1
        
        # Verify derived nodes
        assert results["category"].records_loaded == 2  # Widgets, Gadgets
        
        # Verify similar_to edges (Widget A <-> Widget B)
        # We expect at least some edges. 
        # PRD-001 and PRD-002 are in "Widgets", so they should be connected.
        assert results["similar_to"].records_loaded >= 1
        
        # Verify validation query
        # Fetch directly from DB to confirm data is there
        # Check if CUS-001 exists
        customer = await loader.db.select("customer:CUS001") # sanitized ID
        # sanitized CUS-001 -> CUS001
        
        # Note: The loader sanitizes IDs by removing hyphens? No, just backticks/control chars.
        # "VND-001" -> "VND-001". 
        # CAREFUL: "CUS-001" contains hyphen. 
        # If we use `customer:CUS-001` in SELECT, we might need backticks if not using the record link format.
        # Python SDK select("table:id") usually works.
        
        # Query to count nodes - use select() for simpler API usage
        customers = await loader.db.select("customer")
        assert len(customers) >= 2
        
        # Verify CUS-001 is present
        found = False
        for c in customers:
            # The ID might be returned as "customer:CUS-001" or just "CUS-001" depending on client
            # Or record link object
            str_c = str(c)
            if "CUS-001" in str_c or "CUS001" in str_c:
                found = True
                break
        assert found 
        
    finally:
        await loader.close()
