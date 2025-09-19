"""Data reporting and visualization utilities."""

import json
from pathlib import Path
from typing import Any, Dict, List

import pandas as pd
import structlog

logger = structlog.get_logger()


class DataReporter:
    """Generate reports and summaries from data analysis."""
    
    def __init__(self, config: Dict[str, Any] = None):
        """Initialize data reporter."""
        self.config = config or {}
    
    def generate_report(
        self, 
        data: pd.DataFrame, 
        inspection_results: Dict[str, Any] = None,
        quality_results: Dict[str, Any] = None
    ) -> Dict[str, Any]:
        """Generate comprehensive data report."""
        logger.info("Generating data report", rows=len(data), columns=len(data.columns))
        
        report = {
            "metadata": {
                "report_timestamp": pd.Timestamp.now().isoformat(),
                "dataset_name": self.config.get("dataset_name", "Unknown"),
                "report_version": "1.0",
            },
            "dataset_overview": self._generate_overview(data),
        }
        
        if inspection_results:
            report["data_inspection"] = inspection_results
        
        if quality_results:
            report["quality_assessment"] = quality_results
        
        # Generate recommendations
        report["recommendations"] = self._generate_recommendations(data, quality_results)
        
        # Generate summary
        report["executive_summary"] = self._generate_executive_summary(report)
        
        logger.info("Data report generated successfully")
        return report
    
    def save_report(self, report: Dict[str, Any], output_path: str = None) -> str:
        """Save report to file."""
        if not output_path:
            output_path = self.config.get("output_path", "output_data/data_report.json")
        
        report_path = Path(output_path)
        report_path.parent.mkdir(parents=True, exist_ok=True)
        
        # Convert pandas/numpy types to JSON serializable types
        def convert_types(obj):
            if hasattr(obj, 'dtype'):
                if 'int' in str(obj.dtype):
                    return int(obj)
                elif 'float' in str(obj.dtype):
                    return float(obj)
                elif 'bool' in str(obj.dtype):
                    return bool(obj)
                else:
                    return str(obj)
            elif isinstance(obj, dict):
                return {k: convert_types(v) for k, v in obj.items()}
            elif isinstance(obj, list):
                return [convert_types(item) for item in obj]
            else:
                return str(obj)
        
        with open(report_path, "w") as f:
            json.dump(report, f, indent=2, default=convert_types)
        
        logger.info("Report saved", path=str(report_path))
        return str(report_path)
    
    def _generate_overview(self, df: pd.DataFrame) -> Dict[str, Any]:
        """Generate dataset overview."""
        return {
            "total_rows": int(len(df)),
            "total_columns": int(len(df.columns)),
            "memory_usage_mb": float(round(df.memory_usage(deep=True).sum() / 1024 / 1024, 2)),
            "column_types": {str(k): int(v) for k, v in df.dtypes.value_counts().items()},
            "numeric_columns": df.select_dtypes(include=["int64", "float64"]).columns.tolist(),
            "categorical_columns": df.select_dtypes(include=["object", "category"]).columns.tolist(),
            "datetime_columns": df.select_dtypes(include=["datetime64"]).columns.tolist(),
        }
    
    def _generate_recommendations(
        self, 
        df: pd.DataFrame, 
        quality_results: Dict[str, Any] = None
    ) -> List[Dict[str, str]]:
        """Generate data improvement recommendations."""
        recommendations = []
        
        # Memory optimization recommendations
        memory_mb = df.memory_usage(deep=True).sum() / 1024 / 1024
        if memory_mb > 100:
            recommendations.append({
                "category": "Performance",
                "priority": "Medium",
                "recommendation": f"Dataset is large ({memory_mb:.1f}MB). Consider using chunked processing or data sampling for better performance.",
            })
        
        # Data type optimization
        for col in df.columns:
            if df[col].dtype == "object":
                unique_ratio = df[col].nunique() / len(df)
                if unique_ratio < 0.5:
                    recommendations.append({
                        "category": "Optimization",
                        "priority": "Low",
                        "recommendation": f"Column '{col}' has low cardinality ({unique_ratio:.2%}). Consider converting to categorical type.",
                    })
        
        # Quality-based recommendations
        if quality_results:
            overall_score = quality_results.get("overall_score", 100)
            if overall_score < 80:
                recommendations.append({
                    "category": "Quality",
                    "priority": "High",
                    "recommendation": f"Data quality score is low ({overall_score:.1f}%). Review and address quality issues before processing.",
                })
            
            # Specific quality issues
            for issue in quality_results.get("issues", []):
                if "null" in issue.lower():
                    recommendations.append({
                        "category": "Completeness",
                        "priority": "Medium",
                        "recommendation": f"Address missing data: {issue}",
                    })
                elif "duplicate" in issue.lower():
                    recommendations.append({
                        "category": "Uniqueness",
                        "priority": "Medium",
                        "recommendation": f"Remove or investigate duplicates: {issue}",
                    })
        
        # Null value recommendations
        null_cols = df.columns[df.isnull().any()].tolist()
        if null_cols:
            recommendations.append({
                "category": "Completeness",
                "priority": "Medium",
                "recommendation": f"Columns with null values: {', '.join(null_cols)}. Consider imputation or removal strategies.",
            })
        
        return recommendations
    
    def _generate_executive_summary(self, report: Dict[str, Any]) -> Dict[str, Any]:
        """Generate executive summary of the report."""
        overview = report.get("dataset_overview", {})
        quality = report.get("quality_assessment", {})
        
        summary = {
            "dataset_size": f"{overview.get('total_rows', 0):,} rows × {overview.get('total_columns', 0)} columns",
            "memory_footprint": f"{overview.get('memory_usage_mb', 0):.1f} MB",
        }
        
        if quality:
            summary["quality_score"] = f"{quality.get('overall_score', 0):.1f}%"
            summary["quality_status"] = self._get_quality_status(quality.get('overall_score', 0))
            summary["critical_issues"] = len([
                issue for issue in quality.get('issues', [])
                if any(keyword in issue.lower() for keyword in ['required', 'critical', 'violation'])
            ])
        
        recommendations = report.get("recommendations", [])
        summary["high_priority_recommendations"] = len([
            rec for rec in recommendations if rec.get("priority") == "High"
        ])
        
        return summary
    
    def _get_quality_status(self, score: float) -> str:
        """Get quality status based on score."""
        if score >= 90:
            return "Excellent"
        elif score >= 80:
            return "Good"
        elif score >= 70:
            return "Fair"
        elif score >= 60:
            return "Poor"
        else:
            return "Critical"