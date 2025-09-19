"""Factory for creating extractors."""

from typing import Any, Dict

from .base import BaseExtractor
from .csv_extractor import CSVExtractor
from .s3_extractor import S3Extractor
from .neo4j_extractor import Neo4jExtractor


def get_extractor(extractor_type: str) -> type[BaseExtractor]:
    """Factory function to get extractor class by type."""
    extractors = {
        "csv": CSVExtractor,
        "s3": S3Extractor,
        "neo4j": Neo4jExtractor,
    }
    
    if extractor_type not in extractors:
        available = ", ".join(extractors.keys())
        raise ValueError(f"Unknown extractor type: {extractor_type}. Available: {available}")
    
    return extractors[extractor_type]


def create_extractor(config: Dict[str, Any]) -> BaseExtractor:
    """Create and return configured extractor instance."""
    extractor_type = config.get("type")
    if not extractor_type:
        raise ValueError("Extractor configuration must include 'type' field")
    
    extractor_class = get_extractor(extractor_type)
    return extractor_class(config)