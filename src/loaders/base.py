"""Base loader class."""

from abc import ABC, abstractmethod
from typing import Any, Dict, Union

import pandas as pd


class BaseLoader(ABC):
    """Base class for all data loaders."""
    
    def __init__(self, config: Dict[str, Any]):
        """Initialize loader with configuration."""
        self.config = config
        self.validate_config()
    
    @abstractmethod
    def validate_config(self) -> None:
        """Validate loader configuration."""
        pass
    
    @abstractmethod
    def load(self, data: Union[pd.DataFrame, list, dict], overwrite: bool = False) -> None:
        """Load data to destination."""
        pass
    
    def run(self, data: Union[pd.DataFrame, list, dict], overwrite: bool = False) -> None:
        """Execute the loading process."""
        return self.load(data, overwrite)