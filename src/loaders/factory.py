"""Factory for creating loaders."""

from typing import Any, Dict

from .base import BaseLoader
from .csv_loader import CSVLoader
from .s3_loader import S3Loader
from .neo4j_loader import Neo4jLoader


def get_loader(loader_type: str) -> type[BaseLoader]:
    """Factory function to get loader class by type."""
    loaders = {
        "csv": CSVLoader,
        "s3": S3Loader,
        "neo4j": Neo4jLoader,
    }
    
    if loader_type not in loaders:
        available = ", ".join(loaders.keys())
        raise ValueError(f"Unknown loader type: {loader_type}. Available: {available}")
    
    return loaders[loader_type]


def create_loader(config: Dict[str, Any]) -> BaseLoader:
    """Create and return configured loader instance."""
    loader_type = config.get("type")
    if not loader_type:
        raise ValueError("Loader configuration must include 'type' field")
    
    loader_class = get_loader(loader_type)
    return loader_class(config)