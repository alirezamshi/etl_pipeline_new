"""Base transformer class."""

from abc import ABC, abstractmethod
from typing import Any, Dict, Union

import pandas as pd


class BaseTransformer(ABC):
    """Base class for all data transformers."""
    
    def __init__(self, config: Dict[str, Any]):
        """Initialize transformer with configuration."""
        self.config = config
        self.validate_config()
    
    @abstractmethod
    def validate_config(self) -> None:
        """Validate transformer configuration."""
        pass
    
    @abstractmethod
    def transform(self, data: Union[pd.DataFrame, list, dict]) -> Union[pd.DataFrame, list, dict]:
        """Transform the input data."""
        pass
    
    def run(self, data: Union[pd.DataFrame, list, dict]) -> Union[pd.DataFrame, list, dict]:
        """Execute the transformation."""
        return self.transform(data)