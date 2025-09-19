"""CSV file extractor."""

import os
from pathlib import Path
from typing import Any, Dict, Union

import pandas as pd
import structlog

from .base import BaseExtractor

logger = structlog.get_logger()


class CSVExtractor(BaseExtractor):
    """Extract data from CSV files."""
    
    def validate_config(self) -> None:
        """Validate CSV extractor configuration."""
        if not self.config.get("path"):
            raise ValueError("CSV extractor requires 'path' in configuration")
    
    def run(self) -> pd.DataFrame:
        """Extract data from CSV file."""
        file_path = Path(self.config["path"])
        
        # Handle relative paths - assume input_data directory for data files
        if not file_path.is_absolute():
            if not file_path.exists():
                input_path = Path("input_data") / file_path
                if input_path.exists():
                    file_path = input_path
        
        if not file_path.exists():
            raise FileNotFoundError(f"CSV file not found: {file_path}")
        
        try:
            # Get CSV reading parameters
            csv_params = {
                "encoding": self.config.get("encoding", "utf-8"),
                "sep": self.config.get("separator", ","),
                "header": self.config.get("header", 0),
                "dtype": self.config.get("dtype"),
                "parse_dates": self.config.get("parse_dates"),
                "chunksize": self.config.get("chunksize"),
            }
            
            # Remove None values
            csv_params = {k: v for k, v in csv_params.items() if v is not None}
            
            logger.info("Reading CSV file", path=str(file_path), params=csv_params)
            
            if csv_params.get("chunksize"):
                # Return iterator for large files
                return pd.read_csv(file_path, **csv_params)
            else:
                df = pd.read_csv(file_path, **csv_params)
                logger.info("CSV data extracted", rows=len(df), columns=len(df.columns))
                return df
        
        except Exception as e:
            logger.error("Failed to extract CSV data", path=str(file_path), error=str(e))
            raise RuntimeError(f"CSV extraction failed: {e}") from e