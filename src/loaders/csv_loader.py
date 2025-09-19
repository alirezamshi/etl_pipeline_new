"""CSV file loader."""

from pathlib import Path
from typing import Any, Dict, Union

import pandas as pd
import structlog

from .base import BaseLoader

logger = structlog.get_logger()


class CSVLoader(BaseLoader):
    """Load data to CSV files."""
    
    def validate_config(self) -> None:
        """Validate CSV loader configuration."""
        if not self.config.get("path"):
            raise ValueError("CSV loader requires 'path' in configuration")
    
    def load(self, data: Union[pd.DataFrame, list, dict], overwrite: bool = False) -> None:
        """Load data to CSV file."""
        file_path = Path(self.config["path"])
        
        # Handle relative paths - assume output_data directory for output files
        if not file_path.is_absolute():
            output_path = Path("output_data") / file_path
            file_path = output_path
        
        # Create directory if it doesn't exist
        file_path.parent.mkdir(parents=True, exist_ok=True)
        
        # Check if file exists and handle overwrite
        if file_path.exists() and not overwrite:
            if self.config.get("append", False):
                logger.info("Appending to existing CSV file", path=str(file_path))
            else:
                raise FileExistsError(f"CSV file already exists: {file_path}. Use overwrite=True or append=True")
        
        try:
            # Convert data to DataFrame if needed
            if isinstance(data, list):
                df = pd.DataFrame(data)
            elif isinstance(data, dict):
                df = pd.DataFrame([data])
            elif isinstance(data, pd.DataFrame):
                df = data
            else:
                raise ValueError(f"Unsupported data type for CSV loader: {type(data)}")
            
            # Get CSV writing parameters
            csv_params = {
                "index": self.config.get("include_index", False),
                "encoding": self.config.get("encoding", "utf-8"),
                "sep": self.config.get("separator", ","),
                "header": self.config.get("include_header", True),
            }
            
            # Handle append mode
            if self.config.get("append", False) and file_path.exists():
                csv_params["mode"] = "a"
                csv_params["header"] = False  # Don't write header when appending
            
            df.to_csv(file_path, **csv_params)
            
            logger.info("Data loaded to CSV", 
                       path=str(file_path), 
                       rows=len(df), 
                       columns=len(df.columns),
                       mode="append" if csv_params.get("mode") == "a" else "write")
        
        except Exception as e:
            logger.error("Failed to load CSV data", path=str(file_path), error=str(e))
            raise RuntimeError(f"CSV loading failed: {e}") from e