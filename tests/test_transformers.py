"""Tests for data transformers."""

import pandas as pd
import pytest
from unittest.mock import Mock, patch

from src.transformers import DataCleaner, DataAggregator, SQLTransformer
from src.transformers.factory import get_transformer, create_transformer


class TestDataCleaner:
    """Test data cleaner functionality."""
    
    def test_data_cleaner_basic(self):
        """Test basic data cleaning."""
        df = pd.DataFrame({
            "id": [1, 2, 2, 3, 4],
            "name": ["Alice", "Bob", "Bob", None, "Charlie"],
            "value": [10, 20, 20, 30, 40]
        })
        
        config = {
            "remove_duplicates": True,
            "missing_strategy": "drop"
        }
        
        cleaner = DataCleaner(config)
        result = cleaner.transform(df)
        
        # Should remove duplicates and null values
        assert len(result) == 3  # Original 5 - 1 duplicate - 1 null = 3
        assert result["name"].isnull().sum() == 0
    
    def test_data_cleaner_fill_strategy(self):
        """Test data cleaning with fill strategy."""
        df = pd.DataFrame({
            "id": [1, 2, 3],
            "name": ["Alice", None, "Charlie"],
            "category": [None, "B", "C"]
        })
        
        config = {
            "missing_strategy": "fill",
            "fill_values": {"name": "Unknown", "category": "Default"}
        }
        
        cleaner = DataCleaner(config)
        result = cleaner.transform(df)
        
        assert result.loc[1, "name"] == "Unknown"
        assert result.loc[0, "category"] == "Default"
    
    def test_data_cleaner_filter(self):
        """Test data cleaning with filter expression."""
        df = pd.DataFrame({
            "id": [1, 2, 3, 4],
            "value": [10, 20, 30, 40]
        })
        
        config = {
            "filter_expr": "value > 20"
        }
        
        cleaner = DataCleaner(config)
        result = cleaner.transform(df)
        
        assert len(result) == 2
        assert all(result["value"] > 20)
    
    def test_data_cleaner_invalid_input(self):
        """Test data cleaner with invalid input."""
        config = {}
        cleaner = DataCleaner(config)
        
        with pytest.raises(ValueError, match="requires pandas DataFrame"):
            cleaner.transform([1, 2, 3])


class TestDataAggregator:
    """Test data aggregator functionality."""
    
    def test_aggregator_validation(self):
        """Test aggregator configuration validation."""
        with pytest.raises(ValueError, match="requires 'group_by'"):
            DataAggregator({})
        
        with pytest.raises(ValueError, match="requires 'aggregations'"):
            DataAggregator({"group_by": ["col1"]})
    
    def test_aggregator_basic(self):
        """Test basic data aggregation."""
        df = pd.DataFrame({
            "category": ["A", "A", "B", "B"],
            "value": [10, 20, 30, 40],
            "count": [1, 1, 1, 1]
        })
        
        config = {
            "group_by": ["category"],
            "aggregations": {
                "value": ["sum", "mean"],
                "count": "sum"
            }
        }
        
        aggregator = DataAggregator(config)
        result = aggregator.transform(df)
        
        assert len(result) == 2  # Two categories
        assert "value_sum" in result.columns
        assert "value_mean" in result.columns
        assert result[result["category"] == "A"]["value_sum"].iloc[0] == 30
        assert result[result["category"] == "B"]["value_sum"].iloc[0] == 70
    
    def test_aggregator_with_sort(self):
        """Test aggregator with sorting."""
        df = pd.DataFrame({
            "category": ["A", "A", "B", "B"],
            "value": [10, 20, 30, 40]
        })
        
        config = {
            "group_by": ["category"],
            "aggregations": {"value": "sum"},
            "sort_by": ["value_sum"],
            "sort_ascending": False
        }
        
        aggregator = DataAggregator(config)
        result = aggregator.transform(df)
        
        # B should come first (70 > 30)
        assert result.iloc[0]["category"] == "B"
        assert result.iloc[1]["category"] == "A"
    
    def test_aggregator_invalid_input(self):
        """Test aggregator with invalid input."""
        config = {
            "group_by": ["category"],
            "aggregations": {"value": "sum"}
        }
        aggregator = DataAggregator(config)
        
        with pytest.raises(ValueError, match="requires pandas DataFrame"):
            aggregator.transform([1, 2, 3])


class TestSQLTransformer:
    """Test SQL transformer functionality."""
    
    def test_sql_transformer_validation(self):
        """Test SQL transformer configuration validation."""
        with pytest.raises(ValueError, match="requires either 'sql_query' or 'sql_file'"):
            SQLTransformer({})
    
    def test_sql_transformer_basic(self):
        """Test basic SQL transformation."""
        df = pd.DataFrame({
            "id": [1, 2, 3, 4],
            "value": [10, 20, 30, 40],
            "category": ["A", "A", "B", "B"]
        })
        
        config = {
            "sql_query": "SELECT category, SUM(value) as total_value FROM source_data GROUP BY category",
            "table_name": "source_data"
        }
        
        transformer = SQLTransformer(config)
        result = transformer.transform(df)
        
        assert len(result) == 2
        assert "total_value" in result.columns
        assert set(result["category"]) == {"A", "B"}
    
    def test_sql_transformer_invalid_input(self):
        """Test SQL transformer with invalid input."""
        config = {"sql_query": "SELECT * FROM source_data"}
        transformer = SQLTransformer(config)
        
        with pytest.raises(ValueError, match="requires pandas DataFrame"):
            transformer.transform([1, 2, 3])


class TestTransformerFactory:
    """Test transformer factory functionality."""
    
    def test_get_transformer_valid_types(self):
        """Test getting valid transformer types."""
        assert get_transformer("cleaner") == DataCleaner
        assert get_transformer("aggregator") == DataAggregator
        assert get_transformer("sql") == SQLTransformer
    
    def test_get_transformer_invalid_type(self):
        """Test getting invalid transformer type."""
        with pytest.raises(ValueError, match="Unknown transformer type"):
            get_transformer("invalid")
    
    def test_create_transformer(self):
        """Test creating transformer instance."""
        config = {"type": "cleaner"}
        transformer = create_transformer(config)
        assert isinstance(transformer, DataCleaner)
    
    def test_create_transformer_missing_type(self):
        """Test creating transformer without type."""
        config = {"remove_duplicates": True}
        with pytest.raises(ValueError, match="must include 'type'"):
            create_transformer(config)