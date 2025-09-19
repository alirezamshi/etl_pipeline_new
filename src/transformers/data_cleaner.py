"""Data cleaning transformations."""

from typing import Any, Dict, List, Union

import pandas as pd
import structlog

from .base import BaseTransformer

logger = structlog.get_logger()


class DataCleaner(BaseTransformer):
    """Clean and standardize data."""
    
    def validate_config(self) -> None:
        """Validate data cleaner configuration."""
        # Configuration is optional for data cleaner
        pass
    
    def transform(self, data: pd.DataFrame) -> pd.DataFrame:
        """Apply data cleaning transformations."""
        if not isinstance(data, pd.DataFrame):
            raise ValueError("DataCleaner requires pandas DataFrame input")
        
        df = data.copy()
        original_rows = len(df)
        
        logger.info("Starting data cleaning", original_rows=original_rows)
        
        # Remove duplicates
        if self.config.get("remove_duplicates", True):
            dedup_cols = self.config.get("dedup_cols", [])
            if dedup_cols:
                df = df.drop_duplicates(subset=dedup_cols)
            else:
                df = df.drop_duplicates()
            logger.info("Removed duplicates", rows_removed=original_rows - len(df))
        
        # Handle missing values
        missing_strategy = self.config.get("missing_strategy", "drop")
        if missing_strategy == "drop":
            df = df.dropna()
        elif missing_strategy == "fill":
            fill_values = self.config.get("fill_values", {})
            if fill_values:
                df = df.fillna(fill_values)
            else:
                # Fill numeric columns with mean, categorical with mode
                for col in df.columns:
                    if df[col].dtype in ["int64", "float64"]:
                        df[col] = df[col].fillna(df[col].mean())
                    else:
                        df[col] = df[col].fillna(df[col].mode().iloc[0] if not df[col].mode().empty else "Unknown")
        
        # Apply filters
        filter_expr = self.config.get("filter_expr")
        if filter_expr:
            try:
                df = df.query(filter_expr)
                logger.info("Applied filter", expression=filter_expr, remaining_rows=len(df))
            except Exception as e:
                logger.warning("Filter expression failed", expression=filter_expr, error=str(e))
        
        # Standardize column names
        if self.config.get("standardize_columns", True):
            df.columns = df.columns.str.lower().str.replace(" ", "_").str.replace("[^a-zA-Z0-9_]", "", regex=True)
        
        # Data type conversions
        dtype_conversions = self.config.get("dtype_conversions", {})
        for col, dtype in dtype_conversions.items():
            if col in df.columns:
                try:
                    df[col] = df[col].astype(dtype)
                except Exception as e:
                    logger.warning("Type conversion failed", column=col, dtype=dtype, error=str(e))
        
        # Remove outliers (using IQR method)
        if self.config.get("remove_outliers", False):
            outlier_cols = self.config.get("outlier_cols", [])
            if not outlier_cols:
                outlier_cols = df.select_dtypes(include=["int64", "float64"]).columns.tolist()
            
            for col in outlier_cols:
                if col in df.columns:
                    Q1 = df[col].quantile(0.25)
                    Q3 = df[col].quantile(0.75)
                    IQR = Q3 - Q1
                    lower_bound = Q1 - 1.5 * IQR
                    upper_bound = Q3 + 1.5 * IQR
                    
                    before_count = len(df)
                    df = df[(df[col] >= lower_bound) & (df[col] <= upper_bound)]
                    outliers_removed = before_count - len(df)
                    
                    if outliers_removed > 0:
                        logger.info("Removed outliers", column=col, outliers_removed=outliers_removed)
        
        final_rows = len(df)
        logger.info("Data cleaning completed", 
                   original_rows=original_rows, 
                   final_rows=final_rows, 
                   rows_removed=original_rows - final_rows)
        
        return df


def dedup_and_filter(df: pd.DataFrame, rules: Dict[str, Any]) -> pd.DataFrame:
    """Standalone function for deduplication and filtering."""
    result_df = df.copy()
    
    # Deduplication
    dedup_cols = rules.get("dedup_cols", [])
    if dedup_cols:
        result_df = result_df.drop_duplicates(subset=dedup_cols)
    else:
        result_df = result_df.drop_duplicates()
    
    # Filtering
    filter_expr = rules.get("filter_expr")
    if filter_expr:
        result_df = result_df.query(filter_expr)
    
    return result_df