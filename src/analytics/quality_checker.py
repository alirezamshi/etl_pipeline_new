"""Data quality checking utilities."""

from typing import Any, Dict, List

import pandas as pd
import structlog

logger = structlog.get_logger()


class DataQualityChecker:
    """Check data quality against defined rules and thresholds."""
    
    def __init__(self, config: Dict[str, Any] = None):
        """Initialize data quality checker."""
        self.config = config or {}
        self.quality_rules = self.config.get("quality_rules", {})
    
    def check_quality(self, data: pd.DataFrame) -> Dict[str, Any]:
        """Run comprehensive data quality checks."""
        if not isinstance(data, pd.DataFrame):
            raise ValueError("DataQualityChecker requires pandas DataFrame input")
        
        logger.info("Starting data quality checks", rows=len(data), columns=len(data.columns))
        
        quality_results = {
            "overall_score": 0.0,
            "checks_passed": 0,
            "checks_failed": 0,
            "issues": [],
            "detailed_results": {},
        }
        
        # Run individual quality checks
        checks = [
            ("completeness", self._check_completeness),
            ("uniqueness", self._check_uniqueness),
            ("validity", self._check_validity),
            ("consistency", self._check_consistency),
            ("accuracy", self._check_accuracy),
        ]
        
        total_checks = 0
        passed_checks = 0
        
        for check_name, check_function in checks:
            if self.quality_rules.get(check_name, {}).get("enabled", True):
                result = check_function(data)
                quality_results["detailed_results"][check_name] = result
                
                total_checks += 1
                if result["passed"]:
                    passed_checks += 1
                else:
                    quality_results["issues"].extend(result.get("issues", []))
        
        # Calculate overall score
        if total_checks > 0:
            quality_results["overall_score"] = round(passed_checks / total_checks * 100, 2)
        
        quality_results["checks_passed"] = passed_checks
        quality_results["checks_failed"] = total_checks - passed_checks
        
        logger.info("Data quality checks completed", 
                   overall_score=quality_results["overall_score"],
                   checks_passed=passed_checks,
                   checks_failed=total_checks - passed_checks)
        
        return quality_results
    
    def _check_completeness(self, df: pd.DataFrame) -> Dict[str, Any]:
        """Check data completeness (null values)."""
        completeness_rules = self.quality_rules.get("completeness", {})
        max_null_percentage = completeness_rules.get("max_null_percentage", 5.0)
        required_columns = completeness_rules.get("required_columns", [])
        
        null_percentages = (df.isnull().sum() / len(df) * 100).round(2)
        issues = []
        
        # Check overall null percentage
        overall_null_pct = (df.isnull().sum().sum() / (len(df) * len(df.columns)) * 100)
        if overall_null_pct > max_null_percentage:
            issues.append(f"Overall null percentage ({overall_null_pct:.2f}%) exceeds threshold ({max_null_percentage}%)")
        
        # Check required columns
        for col in required_columns:
            if col in df.columns and null_percentages[col] > 0:
                issues.append(f"Required column '{col}' has null values ({null_percentages[col]:.2f}%)")
        
        # Check individual column thresholds
        for col, pct in null_percentages.items():
            if pct > max_null_percentage:
                issues.append(f"Column '{col}' null percentage ({pct:.2f}%) exceeds threshold ({max_null_percentage}%)")
        
        return {
            "passed": len(issues) == 0,
            "score": max(0, 100 - overall_null_pct),
            "issues": issues,
            "null_percentages": null_percentages.to_dict(),
        }
    
    def _check_uniqueness(self, df: pd.DataFrame) -> Dict[str, Any]:
        """Check data uniqueness (duplicates)."""
        uniqueness_rules = self.quality_rules.get("uniqueness", {})
        max_duplicate_percentage = uniqueness_rules.get("max_duplicate_percentage", 1.0)
        unique_columns = uniqueness_rules.get("unique_columns", [])
        
        duplicate_count = df.duplicated().sum()
        duplicate_percentage = (duplicate_count / len(df) * 100)
        issues = []
        
        # Check overall duplicate percentage
        if duplicate_percentage > max_duplicate_percentage:
            issues.append(f"Duplicate percentage ({duplicate_percentage:.2f}%) exceeds threshold ({max_duplicate_percentage}%)")
        
        # Check unique columns
        for col in unique_columns:
            if col in df.columns:
                col_duplicates = df[col].duplicated().sum()
                if col_duplicates > 0:
                    col_dup_pct = (col_duplicates / len(df) * 100)
                    issues.append(f"Column '{col}' should be unique but has {col_duplicates} duplicates ({col_dup_pct:.2f}%)")
        
        return {
            "passed": len(issues) == 0,
            "score": max(0, 100 - duplicate_percentage),
            "issues": issues,
            "duplicate_count": int(duplicate_count),
            "duplicate_percentage": round(duplicate_percentage, 2),
        }
    
    def _check_validity(self, df: pd.DataFrame) -> Dict[str, Any]:
        """Check data validity (format, range, etc.)."""
        validity_rules = self.quality_rules.get("validity", {})
        column_rules = validity_rules.get("column_rules", {})
        issues = []
        invalid_count = 0
        total_values = len(df) * len(df.columns)
        
        for col, rules in column_rules.items():
            if col not in df.columns:
                continue
            
            # Check data type
            expected_type = rules.get("type")
            if expected_type and not df[col].dtype.name.startswith(expected_type):
                issues.append(f"Column '{col}' has incorrect type: {df[col].dtype} (expected: {expected_type})")
            
            # Check value range
            min_val = rules.get("min_value")
            max_val = rules.get("max_value")
            if min_val is not None or max_val is not None:
                numeric_col = pd.to_numeric(df[col], errors="coerce")
                if min_val is not None:
                    invalid_min = (numeric_col < min_val).sum()
                    if invalid_min > 0:
                        issues.append(f"Column '{col}' has {invalid_min} values below minimum ({min_val})")
                        invalid_count += invalid_min
                
                if max_val is not None:
                    invalid_max = (numeric_col > max_val).sum()
                    if invalid_max > 0:
                        issues.append(f"Column '{col}' has {invalid_max} values above maximum ({max_val})")
                        invalid_count += invalid_max
            
            # Check allowed values
            allowed_values = rules.get("allowed_values")
            if allowed_values:
                invalid_values = ~df[col].isin(allowed_values)
                invalid_val_count = invalid_values.sum()
                if invalid_val_count > 0:
                    issues.append(f"Column '{col}' has {invalid_val_count} invalid values")
                    invalid_count += invalid_val_count
        
        invalid_percentage = (invalid_count / total_values * 100) if total_values > 0 else 0
        
        return {
            "passed": len(issues) == 0,
            "score": max(0, 100 - invalid_percentage),
            "issues": issues,
            "invalid_count": invalid_count,
            "invalid_percentage": round(invalid_percentage, 2),
        }
    
    def _check_consistency(self, df: pd.DataFrame) -> Dict[str, Any]:
        """Check data consistency across columns."""
        consistency_rules = self.quality_rules.get("consistency", {})
        cross_column_rules = consistency_rules.get("cross_column_rules", [])
        issues = []
        
        for rule in cross_column_rules:
            rule_name = rule.get("name", "Unnamed rule")
            columns = rule.get("columns", [])
            condition = rule.get("condition")
            
            if len(columns) < 2 or not condition:
                continue
            
            try:
                # Evaluate consistency condition
                inconsistent = df.query(f"not ({condition})")
                if len(inconsistent) > 0:
                    issues.append(f"Consistency rule '{rule_name}' failed for {len(inconsistent)} rows")
            except Exception as e:
                issues.append(f"Consistency rule '{rule_name}' evaluation failed: {str(e)}")
        
        return {
            "passed": len(issues) == 0,
            "score": 100 if len(issues) == 0 else 50,  # Binary score for consistency
            "issues": issues,
        }
    
    def _check_accuracy(self, df: pd.DataFrame) -> Dict[str, Any]:
        """Check data accuracy against reference data or business rules."""
        accuracy_rules = self.quality_rules.get("accuracy", {})
        business_rules = accuracy_rules.get("business_rules", [])
        issues = []
        
        for rule in business_rules:
            rule_name = rule.get("name", "Unnamed rule")
            condition = rule.get("condition")
            
            if not condition:
                continue
            
            try:
                # Evaluate business rule
                violations = df.query(f"not ({condition})")
                if len(violations) > 0:
                    issues.append(f"Business rule '{rule_name}' violated by {len(violations)} rows")
            except Exception as e:
                issues.append(f"Business rule '{rule_name}' evaluation failed: {str(e)}")
        
        return {
            "passed": len(issues) == 0,
            "score": 100 if len(issues) == 0 else 50,  # Binary score for accuracy
            "issues": issues,
        }