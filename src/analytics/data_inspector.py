"""Data inspection and profiling utilities."""

from typing import Any, Dict, List, Union

import pandas as pd
import structlog

logger = structlog.get_logger()


class DataInspector:
    """Inspect and profile data for quality and characteristics."""
    
    def __init__(self, config: Dict[str, Any] = None):
        """Initialize data inspector."""
        self.config = config or {}
    
    def inspect(self, data: pd.DataFrame) -> Dict[str, Any]:
        """Perform comprehensive data inspection."""
        if not isinstance(data, pd.DataFrame):
            raise ValueError("DataInspector requires pandas DataFrame input")
        
        logger.info("Starting data inspection", rows=len(data), columns=len(data.columns))
        
        inspection_result = {
            "basic_info": self._get_basic_info(data),
            "summary_stats": self._get_summary_stats(data),
            "null_analysis": self._get_null_analysis(data),
            "data_types": self._get_data_types(data),
            "sample_data": self._get_sample_data(data),
        }
        
        # Optional detailed analysis
        if self.config.get("include_correlations", False):
            inspection_result["correlations"] = self._get_correlations(data)
        
        if self.config.get("include_duplicates", True):
            inspection_result["duplicates"] = self._get_duplicate_analysis(data)
        
        if self.config.get("include_outliers", False):
            inspection_result["outliers"] = self._get_outlier_analysis(data)
        
        logger.info("Data inspection completed")
        return inspection_result
    
    def _get_basic_info(self, df: pd.DataFrame) -> Dict[str, Any]:
        """Get basic information about the dataset."""
        return {
            "rows": len(df),
            "columns": len(df.columns),
            "memory_usage_mb": df.memory_usage(deep=True).sum() / 1024 / 1024,
            "column_names": df.columns.tolist(),
        }
    
    def _get_summary_stats(self, df: pd.DataFrame) -> Dict[str, Any]:
        """Get summary statistics for numeric columns."""
        numeric_cols = df.select_dtypes(include=["int64", "float64"]).columns
        if len(numeric_cols) > 0:
            stats = df[numeric_cols].describe()
            return {col: {stat: float(val) for stat, val in stats[col].items()} for col in stats.columns}
        return {}
    
    def _get_null_analysis(self, df: pd.DataFrame) -> Dict[str, Any]:
        """Analyze null values in the dataset."""
        null_counts = df.isnull().sum()
        null_percentages = (null_counts / len(df) * 100).round(2)
        
        return {
            "null_counts": {col: int(count) for col, count in null_counts.items()},
            "null_percentages": {col: float(pct) for col, pct in null_percentages.items()},
            "columns_with_nulls": null_counts[null_counts > 0].index.tolist(),
            "total_null_values": int(null_counts.sum()),
        }
    
    def _get_data_types(self, df: pd.DataFrame) -> Dict[str, str]:
        """Get data types for all columns."""
        return {col: str(dtype) for col, dtype in df.dtypes.items()}
    
    def _get_sample_data(self, df: pd.DataFrame) -> Dict[str, Any]:
        """Get sample data from the dataset."""
        sample_size = min(self.config.get("sample_size", 5), len(df))
        return {
            "head": df.head(sample_size).to_dict(orient="records"),
            "tail": df.tail(sample_size).to_dict(orient="records"),
        }
    
    def _get_correlations(self, df: pd.DataFrame) -> Dict[str, Any]:
        """Calculate correlations between numeric columns."""
        numeric_cols = df.select_dtypes(include=["int64", "float64"]).columns
        if len(numeric_cols) > 1:
            corr_matrix = df[numeric_cols].corr()
            return corr_matrix.to_dict()
        return {}
    
    def _get_duplicate_analysis(self, df: pd.DataFrame) -> Dict[str, Any]:
        """Analyze duplicate rows in the dataset."""
        duplicate_rows = df.duplicated().sum()
        return {
            "duplicate_rows": int(duplicate_rows),
            "duplicate_percentage": round(duplicate_rows / len(df) * 100, 2),
            "unique_rows": len(df) - duplicate_rows,
        }
    
    def _get_outlier_analysis(self, df: pd.DataFrame) -> Dict[str, Any]:
        """Detect outliers using IQR method."""
        numeric_cols = df.select_dtypes(include=["int64", "float64"]).columns
        outlier_info = {}
        
        for col in numeric_cols:
            Q1 = df[col].quantile(0.25)
            Q3 = df[col].quantile(0.75)
            IQR = Q3 - Q1
            lower_bound = Q1 - 1.5 * IQR
            upper_bound = Q3 + 1.5 * IQR
            
            outliers = df[(df[col] < lower_bound) | (df[col] > upper_bound)]
            outlier_info[col] = {
                "outlier_count": len(outliers),
                "outlier_percentage": round(len(outliers) / len(df) * 100, 2),
                "lower_bound": lower_bound,
                "upper_bound": upper_bound,
            }
        
        return outlier_info


def inspect_data(df: pd.DataFrame, config: Dict[str, Any] = None) -> Dict[str, Any]:
    """Standalone function to inspect data."""
    inspector = DataInspector(config)
    return inspector.inspect(df)