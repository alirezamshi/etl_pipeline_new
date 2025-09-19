"""Utilities for handling intermediate data persistence."""

import os
import shutil
from pathlib import Path
from typing import Any, Union

import pandas as pd
import structlog

logger = structlog.get_logger()


def save_intermediate(
    data: Union[pd.DataFrame, list, dict], 
    path: str,
    format_type: str = "parquet"
) -> None:
    """Save intermediate data to specified path."""
    if not path:
        return
    
    file_path = Path(path)
    file_path.parent.mkdir(parents=True, exist_ok=True)
    
    try:
        if isinstance(data, pd.DataFrame):
            if format_type == "parquet":
                data.to_parquet(file_path, index=False)
            elif format_type == "csv":
                data.to_csv(file_path, index=False)
            else:
                raise ValueError(f"Unsupported format for DataFrame: {format_type}")
        
        elif isinstance(data, (list, dict)):
            import json
            with open(file_path, "w") as f:
                json.dump(data, f, indent=2, default=str)
        
        else:
            # Fallback: pickle for complex objects
            import pickle
            with open(file_path, "wb") as f:
                pickle.dump(data, f)
        
        logger.info("Intermediate data saved", path=str(file_path), type=type(data).__name__)
    
    except Exception as e:
        logger.error("Failed to save intermediate data", path=str(file_path), error=str(e))
        raise


def load_intermediate(path: str, format_type: str = "parquet") -> Any:
    """Load intermediate data from specified path."""
    file_path = Path(path)
    
    if not file_path.exists():
        raise FileNotFoundError(f"Intermediate file not found: {path}")
    
    try:
        if format_type == "parquet":
            return pd.read_parquet(file_path)
        elif format_type == "csv":
            return pd.read_csv(file_path)
        elif format_type == "json":
            import json
            with open(file_path, "r") as f:
                return json.load(f)
        else:
            # Fallback: pickle
            import pickle
            with open(file_path, "rb") as f:
                return pickle.load(f)
    
    except Exception as e:
        logger.error("Failed to load intermediate data", path=str(file_path), error=str(e))
        raise


def cleanup_intermediates(intermediate_config: dict) -> None:
    """Clean up intermediate files after pipeline completion."""
    if not intermediate_config.get("cleanup", False):
        return
    
    paths_to_clean = []
    
    # Collect all intermediate paths
    for key, value in intermediate_config.items():
        if key != "cleanup" and isinstance(value, str) and value:
            paths_to_clean.append(value)
    
    # Clean up files and directories
    for path_str in paths_to_clean:
        path = Path(path_str)
        try:
            if path.is_file():
                path.unlink()
                logger.info("Cleaned up intermediate file", path=str(path))
            elif path.is_dir():
                shutil.rmtree(path)
                logger.info("Cleaned up intermediate directory", path=str(path))
        except Exception as e:
            logger.warning("Failed to cleanup intermediate", path=str(path), error=str(e))
    
    # Clean up common intermediate directories if empty
    for dir_name in ["intermediate_data", "temp_data"]:
        dir_path = Path(dir_name)
        if dir_path.exists() and dir_path.is_dir():
            try:
                if not any(dir_path.iterdir()):  # Directory is empty
                    dir_path.rmdir()
                    logger.info("Cleaned up empty intermediate directory", path=str(dir_path))
            except Exception as e:
                logger.debug("Could not remove intermediate directory", path=str(dir_path), error=str(e))