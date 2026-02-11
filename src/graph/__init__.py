"""
Graph Layer - SurrealDB Graph Modeling & Loading.

Loads Silver data into SurrealDB as a graph for GraphRAG applications.
"""

from .loader import GraphLoader
from .queries import GRAPH_QUERIES

__all__ = [
    "GraphLoader",
    "GRAPH_QUERIES",
]
