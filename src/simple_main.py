"""Simple ETL pipeline orchestrator without Prefect."""

import argparse
import sys
from pathlib import Path
from typing import Any, Dict

import structlog

from src.analytics import DataInspector, DataQualityChecker, DataReporter
from src.extractors.factory import create_extractor
from src.loaders.factory import create_loader
from src.transformers.factory import create_transformer
from src.utils import (
    check_idempotency_hash,
    cleanup_intermediates,
    load_config,
    save_intermediate,
    setup_logging,
)

logger = structlog.get_logger()


def extract_task(source_config: Dict[str, Any], intermediate_config: Dict[str, Any]):
    """Extract data from source."""
    logger.info("Starting data extraction", source_type=source_config.get("type"))
    
    extractor = create_extractor(source_config)
    data = extractor.run()
    
    # Log extraction metrics
    if hasattr(data, "__len__"):
        logger.info("Data extraction completed", records=len(data))
    else:
        logger.info("Data extraction completed", type=type(data).__name__)
    
    # Save intermediate data if configured
    if intermediate_config.get("extract"):
        save_intermediate(data, intermediate_config["extract"])
    
    return data


def transform_task(data: Any, transform_config: Dict[str, Any], intermediate_config: Dict[str, Any]):
    """Transform data according to configuration."""
    logger.info("Starting data transformation", transform_type=transform_config.get("type", "cleaner"))
    
    transformer = create_transformer(transform_config)
    transformed_data = transformer.run(data)
    
    # Log transformation metrics
    if hasattr(transformed_data, "__len__"):
        logger.info("Data transformation completed", records=len(transformed_data))
    
    # Save intermediate data if configured
    if intermediate_config.get("transform"):
        save_intermediate(transformed_data, intermediate_config["transform"])
    
    return transformed_data


def load_task(data: Any, load_config: Dict[str, Any]):
    """Load data to destination."""
    logger.info("Starting data loading", load_type=load_config.get("type"))
    
    loader = create_loader(load_config)
    loader.run(data, overwrite=load_config.get("overwrite", False))
    
    logger.info("Data loading completed")


def analyze_task(data: Any, analytics_config: Dict[str, Any] = None):
    """Analyze data quality and generate insights."""
    if not analytics_config or not analytics_config.get("enabled", True):
        return None
    
    logger.info("Starting data analysis")
    
    # Data inspection
    inspector = DataInspector(analytics_config.get("inspection", {}))
    inspection_results = inspector.inspect(data)
    
    # Quality checking
    quality_checker = DataQualityChecker(analytics_config.get("quality", {}))
    quality_results = quality_checker.check_quality(data)
    
    # Generate report
    reporter = DataReporter(analytics_config.get("reporting", {}))
    report = reporter.generate_report(data, inspection_results, quality_results)
    
    # Save report if configured
    if analytics_config.get("save_report", True):
        report_path = reporter.save_report(report)
        logger.info("Analysis report saved", path=report_path)
    
    logger.info("Data analysis completed", 
               quality_score=quality_results.get("overall_score", 0),
               issues_found=len(quality_results.get("issues", [])))
    
    return {
        "inspection": inspection_results,
        "quality": quality_results,
        "report": report,
    }


def etl_pipeline(config_path: str):
    """Main ETL pipeline flow."""
    logger.info("Starting ETL pipeline", config_path=config_path)
    
    # Load configuration
    try:
        config = load_config(config_path)
    except Exception as e:
        logger.error("Failed to load configuration", error=str(e))
        raise
    
    # Check idempotency
    if check_idempotency_hash(config):
        logger.info("Pipeline skipped: Configuration unchanged")
        return "Skipped - No changes detected"
    
    # Extract data
    data = extract_task(config["source"], config.get("intermediates", {}))
    
    # Transform data
    if config.get("transform"):
        data = transform_task(data, config["transform"], config.get("intermediates", {}))
    
    # Analyze data (optional)
    analysis_results = None
    if config.get("analytics"):
        analysis_results = analyze_task(data, config["analytics"])
    
    # Load data
    load_task(data, config["load"])
    
    # Cleanup intermediate files
    if config.get("intermediates", {}).get("cleanup", True):
        cleanup_intermediates(config.get("intermediates", {}))
    
    logger.info("ETL pipeline completed successfully")
    
    return {
        "status": "completed",
        "analysis": analysis_results,
    }


def main():
    """Main entry point for the ETL pipeline."""
    parser = argparse.ArgumentParser(description="Run ETL Pipeline")
    parser.add_argument(
        "--config", 
        "-c", 
        required=True, 
        help="Path to configuration file"
    )
    parser.add_argument(
        "--log-level", 
        default="INFO", 
        choices=["DEBUG", "INFO", "WARNING", "ERROR"],
        help="Logging level"
    )
    parser.add_argument(
        "--json-logs", 
        action="store_true", 
        help="Use JSON logging format"
    )
    parser.add_argument(
        "--reset-idempotency", 
        action="store_true", 
        help="Reset idempotency check"
    )
    
    args = parser.parse_args()
    
    # Setup logging
    setup_logging(level=args.log_level, json_logs=args.json_logs)
    
    # Reset idempotency if requested
    if args.reset_idempotency:
        from src.utils.idempotency import reset_idempotency
        reset_idempotency()
        logger.info("Idempotency check reset")
    
    # Validate config file exists
    config_path = Path(args.config)
    if not config_path.exists():
        logger.error("Configuration file not found", path=str(config_path))
        sys.exit(1)
    
    try:
        # Run the pipeline
        result = etl_pipeline(str(config_path))
        logger.info("Pipeline execution completed", result=result)
        
    except Exception as e:
        logger.error("Pipeline execution failed", error=str(e))
        sys.exit(1)


if __name__ == "__main__":
    main()