"""Factory for creating transformers."""

from typing import Any, Dict

from .base import BaseTransformer
from .data_cleaner import DataCleaner
from .aggregator import DataAggregator
from .sql_transformer import SQLTransformer


def get_transformer(transformer_type: str) -> type[BaseTransformer]:
    """Factory function to get transformer class by type."""
    transformers = {
        "cleaner": DataCleaner,
        "aggregator": DataAggregator,
        "sql": SQLTransformer,
    }
    
    if transformer_type not in transformers:
        available = ", ".join(transformers.keys())
        raise ValueError(f"Unknown transformer type: {transformer_type}. Available: {available}")
    
    return transformers[transformer_type]


def create_transformer(config: Dict[str, Any]) -> BaseTransformer:
    """Create and return configured transformer instance."""
    transformer_type = config.get("type")
    if not transformer_type:
        raise ValueError("Transformer configuration must include 'type' field")
    
    transformer_class = get_transformer(transformer_type)
    return transformer_class(config)