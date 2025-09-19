"""Data aggregation transformations."""

from typing import Any, Dict, List, Union

import pandas as pd
import structlog

from .base import BaseTransformer

logger = structlog.get_logger()


class DataAggregator(BaseTransformer):
    """Aggregate data using various grouping and aggregation functions."""
    
    def validate_config(self) -> None:
        """Validate aggregator configuration."""
        if not self.config.get("group_by"):
            raise ValueError("DataAggregator requires 'group_by' in configuration")
        
        if not self.config.get("aggregations"):
            raise ValueError("DataAggregator requires 'aggregations' in configuration")
    
    def transform(self, data: pd.DataFrame) -> pd.DataFrame:
        """Apply aggregation transformations."""
        if not isinstance(data, pd.DataFrame):
            raise ValueError("DataAggregator requires pandas DataFrame input")
        
        df = data.copy()
        group_by = self.config["group_by"]
        aggregations = self.config["aggregations"]
        
        logger.info("Starting data aggregation", 
                   group_by=group_by, 
                   aggregations=list(aggregations.keys()),
                   original_rows=len(df))
        
        try:
            # Perform groupby aggregation
            if isinstance(group_by, str):
                group_by = [group_by]
            
            # Validate group_by columns exist
            missing_cols = [col for col in group_by if col not in df.columns]
            if missing_cols:
                raise ValueError(f"Group by columns not found: {missing_cols}")
            
            # Apply aggregations
            agg_result = df.groupby(group_by).agg(aggregations)
            
            # Flatten column names if multi-level
            if isinstance(agg_result.columns, pd.MultiIndex):
                agg_result.columns = ['_'.join(col).strip() for col in agg_result.columns.values]
            
            # Reset index to make group_by columns regular columns
            agg_result = agg_result.reset_index()
            
            # Apply post-aggregation transformations
            if self.config.get("sort_by"):
                sort_cols = self.config["sort_by"]
                ascending = self.config.get("sort_ascending", True)
                agg_result = agg_result.sort_values(sort_cols, ascending=ascending)
            
            # Apply filters after aggregation
            post_filter = self.config.get("post_filter")
            if post_filter:
                agg_result = agg_result.query(post_filter)
            
            # Limit results if specified
            limit = self.config.get("limit")
            if limit:
                agg_result = agg_result.head(limit)
            
            logger.info("Data aggregation completed", 
                       final_rows=len(agg_result),
                       final_columns=len(agg_result.columns))
            
            return agg_result
        
        except Exception as e:
            logger.error("Aggregation failed", error=str(e))
            raise RuntimeError(f"Data aggregation failed: {e}") from e


def create_summary_stats(df: pd.DataFrame, group_cols: List[str] = None) -> pd.DataFrame:
    """Create summary statistics for numeric columns."""
    if group_cols:
        return df.groupby(group_cols).describe()
    else:
        return df.describe()