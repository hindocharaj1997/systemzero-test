"""
E-Commerce Data Pipeline - Medallion Architecture.

Technologies:
- Bronze: Polars (file I/O)
- Silver: Polars + Pydantic (cleaning + validation)
- Gold: DuckDB SQL (analytical queries)
- Graph: SurrealDB (graph modeling + loading)
"""

from .bronze import BronzeIngester
from .silver import SilverProcessor
from .gold import GoldProcessor

try:
    from .graph import GraphLoader
except ImportError:
    GraphLoader = None  # SurrealDB not installed

__all__ = [
    "BronzeIngester",
    "SilverProcessor",
    "GoldProcessor",
    "GraphLoader",
]
