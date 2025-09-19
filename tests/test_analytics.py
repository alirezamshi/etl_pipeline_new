"""Tests for analytics modules."""

import pandas as pd
import pytest

from src.analytics import DataInspector, DataQualityChecker, DataReporter


class TestDataInspector:
    """Test data inspector functionality."""
    
    def test_basic_inspection(self):
        """Test basic data inspection."""
        df = pd.DataFrame({
            "id": [1, 2, 3, 4, 5],
            "name": ["Alice", "Bob", None, "David", "Eve"],
            "value": [10.5, 20.0, 30.5, 40.0, 50.5],
            "category": ["A", "B", "A", "B", "A"]
        })
        
        inspector = DataInspector()
        result = inspector.inspect(df)
        
        assert "basic_info" in result
        assert "summary_stats" in result
        assert "null_analysis" in result
        assert "data_types" in result
        assert "sample_data" in result
        
        # Check basic info
        assert result["basic_info"]["rows"] == 5
        assert result["basic_info"]["columns"] == 4
        
        # Check null analysis
        assert result["null_analysis"]["null_counts"]["name"] == 1
        assert result["null_analysis"]["total_null_values"] == 1
    
    def test_inspection_with_options(self):
        """Test data inspection with additional options."""
        df = pd.DataFrame({
            "x": [1, 2, 3, 4, 5],
            "y": [2, 4, 6, 8, 10],
            "z": [1, 1, 2, 2, 3]
        })
        
        config = {
            "include_correlations": True,
            "include_duplicates": True,
            "include_outliers": True
        }
        
        inspector = DataInspector(config)
        result = inspector.inspect(df)
        
        assert "correlations" in result
        assert "duplicates" in result
        assert "outliers" in result
    
    def test_inspection_invalid_input(self):
        """Test data inspector with invalid input."""
        inspector = DataInspector()
        
        with pytest.raises(ValueError, match="requires pandas DataFrame"):
            inspector.inspect([1, 2, 3])


class TestDataQualityChecker:
    """Test data quality checker functionality."""
    
    def test_basic_quality_check(self):
        """Test basic data quality checking."""
        df = pd.DataFrame({
            "id": [1, 2, 3, 4, 5],
            "name": ["Alice", "Bob", "Charlie", "David", "Eve"],
            "value": [10, 20, 30, 40, 50]
        })
        
        checker = DataQualityChecker()
        result = checker.check_quality(df)
        
        assert "overall_score" in result
        assert "checks_passed" in result
        assert "checks_failed" in result
        assert "issues" in result
        assert "detailed_results" in result
        
        # Should pass all checks for clean data
        assert result["overall_score"] > 90
    
    def test_quality_check_with_issues(self):
        """Test quality checking with data issues."""
        df = pd.DataFrame({
            "id": [1, 2, 2, 4, 5],  # Duplicate
            "name": ["Alice", None, "Charlie", "David", "Eve"],  # Missing value
            "value": [10, 20, 30, 40, 50]
        })
        
        config = {
            "quality_rules": {
                "completeness": {
                    "enabled": True,
                    "max_null_percentage": 5.0,
                    "required_columns": ["name"]
                },
                "uniqueness": {
                    "enabled": True,
                    "max_duplicate_percentage": 1.0,
                    "unique_columns": ["id"]
                }
            }
        }
        
        checker = DataQualityChecker(config)
        result = checker.check_quality(df)
        
        assert result["overall_score"] < 100
        assert len(result["issues"]) > 0
        assert any("duplicate" in issue.lower() for issue in result["issues"])
        assert any("null" in issue.lower() for issue in result["issues"])
    
    def test_quality_check_invalid_input(self):
        """Test quality checker with invalid input."""
        checker = DataQualityChecker()
        
        with pytest.raises(ValueError, match="requires pandas DataFrame"):
            checker.check_quality([1, 2, 3])


class TestDataReporter:
    """Test data reporter functionality."""
    
    def test_basic_report_generation(self):
        """Test basic report generation."""
        df = pd.DataFrame({
            "id": [1, 2, 3, 4, 5],
            "name": ["Alice", "Bob", "Charlie", "David", "Eve"],
            "value": [10, 20, 30, 40, 50],
            "category": ["A", "B", "A", "B", "A"]
        })
        
        reporter = DataReporter()
        report = reporter.generate_report(df)
        
        assert "metadata" in report
        assert "dataset_overview" in report
        assert "recommendations" in report
        assert "executive_summary" in report
        
        # Check dataset overview
        overview = report["dataset_overview"]
        assert overview["total_rows"] == 5
        assert overview["total_columns"] == 4
    
    def test_report_with_analysis_results(self):
        """Test report generation with analysis results."""
        df = pd.DataFrame({
            "id": [1, 2, 3],
            "value": [10, 20, 30]
        })
        
        # Mock inspection and quality results
        inspection_results = {
            "basic_info": {"rows": 3, "columns": 2},
            "null_analysis": {"total_null_values": 0}
        }
        
        quality_results = {
            "overall_score": 95.0,
            "issues": [],
            "checks_passed": 5,
            "checks_failed": 0
        }
        
        reporter = DataReporter()
        report = reporter.generate_report(df, inspection_results, quality_results)
        
        assert "data_inspection" in report
        assert "quality_assessment" in report
        assert report["quality_assessment"]["overall_score"] == 95.0
    
    def test_recommendations_generation(self):
        """Test recommendations generation."""
        # Large dataset to trigger memory recommendation
        df = pd.DataFrame({
            "id": range(10000),
            "category": ["A"] * 5000 + ["B"] * 5000,  # Low cardinality
            "value": range(10000)
        })
        
        reporter = DataReporter()
        report = reporter.generate_report(df)
        
        recommendations = report["recommendations"]
        assert len(recommendations) > 0
        
        # Should have memory and optimization recommendations
        categories = [rec["category"] for rec in recommendations]
        assert "Performance" in categories or "Optimization" in categories