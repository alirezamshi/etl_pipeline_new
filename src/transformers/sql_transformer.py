"""SQL-based data transformations."""

from pathlib import Path
from typing import Any, Dict, Union

import pandas as pd
import structlog
from sqlalchemy import create_engine, text

from .base import BaseTransformer

logger = structlog.get_logger()


class SQLTransformer(BaseTransformer):
    """Transform data using SQL queries (dbt-style)."""
    
    def validate_config(self) -> None:
        """Validate SQL transformer configuration."""
        if not self.config.get("sql_query") and not self.config.get("sql_file"):
            raise ValueError("SQLTransformer requires either 'sql_query' or 'sql_file' in configuration")
    
    def _load_sql_from_file(self, sql_file: str) -> str:
        """Load SQL query from file."""
        sql_path = Path(sql_file)
        
        # Check in multiple locations
        possible_paths = [
            sql_path,
            Path("sql") / sql_path,
            Path("src/sql") / sql_path,
            Path("transformers/sql") / sql_path,
        ]
        
        for path in possible_paths:
            if path.exists():
                with open(path, "r") as f:
                    return f.read()
        
        raise FileNotFoundError(f"SQL file not found: {sql_file}")
    
    def transform(self, data: pd.DataFrame) -> pd.DataFrame:
        """Apply SQL transformation to data."""
        if not isinstance(data, pd.DataFrame):
            raise ValueError("SQLTransformer requires pandas DataFrame input")
        
        # Get SQL query
        if self.config.get("sql_file"):
            sql_query = self._load_sql_from_file(self.config["sql_file"])
        else:
            sql_query = self.config["sql_query"]
        
        logger.info("Executing SQL transformation", query_length=len(sql_query))
        
        try:
            # Create in-memory SQLite database
            engine = create_engine("sqlite:///:memory:")
            
            # Load data into temporary table
            table_name = self.config.get("table_name", "source_data")
            data.to_sql(table_name, engine, index=False, if_exists="replace")
            
            # Execute SQL query
            result_df = pd.read_sql_query(sql_query, engine)
            
            logger.info("SQL transformation completed", 
                       input_rows=len(data),
                       output_rows=len(result_df),
                       output_columns=len(result_df.columns))
            
            return result_df
        
        except Exception as e:
            logger.error("SQL transformation failed", error=str(e), query=sql_query[:200])
            raise RuntimeError(f"SQL transformation failed: {e}") from e


def apply_sql_transform(df: pd.DataFrame, sql_query: str, table_name: str = "data") -> pd.DataFrame:
    """Standalone function to apply SQL transformation."""
    engine = create_engine("sqlite:///:memory:")
    df.to_sql(table_name, engine, index=False, if_exists="replace")
    return pd.read_sql_query(sql_query, engine)