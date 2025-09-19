"""Tests for data extractors."""

import pandas as pd
import pytest
from unittest.mock import Mock, patch, mock_open

from src.extractors import CSVExtractor, S3Extractor, Neo4jExtractor
from src.extractors.factory import get_extractor, create_extractor


class TestCSVExtractor:
    """Test CSV extractor functionality."""
    
    def test_csv_extractor_validation(self):
        """Test CSV extractor configuration validation."""
        with pytest.raises(ValueError, match="requires 'path'"):
            CSVExtractor({})
    
    @patch("pandas.read_csv")
    @patch("pathlib.Path.exists")
    def test_csv_extractor_success(self, mock_exists, mock_read_csv):
        """Test successful CSV extraction."""
        mock_exists.return_value = True
        mock_df = pd.DataFrame({"col1": [1, 2, 3], "col2": ["a", "b", "c"]})
        mock_read_csv.return_value = mock_df
        
        config = {"path": "test.csv"}
        extractor = CSVExtractor(config)
        result = extractor.run()
        
        assert isinstance(result, pd.DataFrame)
        assert len(result) == 3
        mock_read_csv.assert_called_once()
    
    @patch("pathlib.Path.exists")
    def test_csv_extractor_file_not_found(self, mock_exists):
        """Test CSV extractor with missing file."""
        mock_exists.return_value = False
        
        config = {"path": "nonexistent.csv"}
        extractor = CSVExtractor(config)
        
        with pytest.raises(FileNotFoundError):
            extractor.run()


class TestS3Extractor:
    """Test S3 extractor functionality."""
    
    def test_s3_extractor_validation(self):
        """Test S3 extractor configuration validation."""
        with pytest.raises(ValueError, match="requires 'bucket'"):
            S3Extractor({"key": "test.csv"})
        
        with pytest.raises(ValueError, match="requires 'key'"):
            S3Extractor({"bucket": "test-bucket"})
    
    @patch("boto3.client")
    @patch("pandas.read_csv")
    def test_s3_extractor_success(self, mock_read_csv, mock_boto_client):
        """Test successful S3 extraction."""
        mock_client = Mock()
        mock_boto_client.return_value = mock_client
        mock_client.get_object.return_value = {"Body": Mock()}
        
        mock_df = pd.DataFrame({"col1": [1, 2, 3], "col2": ["a", "b", "c"]})
        mock_read_csv.return_value = mock_df
        
        config = {
            "bucket": "test-bucket",
            "key": "test.csv",
            "access_key": "test-key",
            "secret_key": "test-secret"
        }
        extractor = S3Extractor(config)
        result = extractor.run()
        
        assert isinstance(result, pd.DataFrame)
        assert len(result) == 3
        mock_client.get_object.assert_called_once_with(Bucket="test-bucket", Key="test.csv")


class TestNeo4jExtractor:
    """Test Neo4j extractor functionality."""
    
    def test_neo4j_extractor_validation(self):
        """Test Neo4j extractor configuration validation."""
        with pytest.raises(ValueError, match="requires 'uri'"):
            Neo4jExtractor({"user": "test", "password": "test", "query": "MATCH (n) RETURN n"})
        
        with pytest.raises(ValueError, match="requires 'query'"):
            Neo4jExtractor({"uri": "bolt://localhost", "user": "test", "password": "test"})


class TestExtractorFactory:
    """Test extractor factory functionality."""
    
    def test_get_extractor_valid_types(self):
        """Test getting valid extractor types."""
        assert get_extractor("csv") == CSVExtractor
        assert get_extractor("s3") == S3Extractor
        assert get_extractor("neo4j") == Neo4jExtractor
    
    def test_get_extractor_invalid_type(self):
        """Test getting invalid extractor type."""
        with pytest.raises(ValueError, match="Unknown extractor type"):
            get_extractor("invalid")
    
    def test_create_extractor(self):
        """Test creating extractor instance."""
        config = {"type": "csv", "path": "test.csv"}
        extractor = create_extractor(config)
        assert isinstance(extractor, CSVExtractor)
    
    def test_create_extractor_missing_type(self):
        """Test creating extractor without type."""
        config = {"path": "test.csv"}
        with pytest.raises(ValueError, match="must include 'type'"):
            create_extractor(config)