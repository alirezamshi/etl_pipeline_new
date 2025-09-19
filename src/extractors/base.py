"""Base extractor class."""

from abc import ABC, abstractmethod
from typing import Any, Dict, Union

import pandas as pd


class BaseExtractor(ABC):
    """Base class for all data extractors."""
    
    def __init__(self, config: Dict[str, Any]):
        """Initialize extractor with configuration."""
        self.config = config
        self.validate_config()
    
    @abstractmethod
    def validate_config(self) -> None:
        """Validate extractor configuration."""
        pass
    
    @abstractmethod
    def run(self) -> Union[pd.DataFrame, list, dict]:
        """Extract data and return it."""
        pass
    
    def get_connection_params(self) -> Dict[str, Any]:
        """Get connection parameters from config."""
        return {
            key: value for key, value in self.config.items()
            if key in ["uri", "user", "password", "access_key", "secret_key", "bucket", "key"]
        }