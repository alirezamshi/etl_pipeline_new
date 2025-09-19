"""Logging configuration and utilities."""

import logging
import sys
from typing import Any, Dict

import structlog


def setup_logging(level: str = "INFO", json_logs: bool = True) -> None:
    """Configure structured logging for the pipeline."""
    log_level = getattr(logging, level.upper())
    
    # Configure standard library logging
    logging.basicConfig(
        format="%(message)s",
        stream=sys.stdout,
        level=log_level,
    )
    
    # Configure structlog
    processors = [
        structlog.stdlib.filter_by_level,
        structlog.stdlib.add_logger_name,
        structlog.stdlib.add_log_level,
        structlog.stdlib.PositionalArgumentsFormatter(),
        structlog.processors.TimeStamper(fmt="iso"),
        structlog.processors.StackInfoRenderer(),
        structlog.processors.format_exc_info,
        structlog.processors.UnicodeDecoder(),
    ]
    
    if json_logs:
        processors.append(structlog.processors.JSONRenderer())
    else:
        processors.append(structlog.dev.ConsoleRenderer())
    
    structlog.configure(
        processors=processors,
        wrapper_class=structlog.stdlib.BoundLogger,
        logger_factory=structlog.stdlib.LoggerFactory(),
        cache_logger_on_first_use=True,
    )


def log_pipeline_metrics(
    step: str,
    metrics: Dict[str, Any],
    logger: structlog.BoundLogger,
) -> None:
    """Log pipeline step metrics in a structured format."""
    logger.info(
        "Pipeline step completed",
        step=step,
        **metrics,
    )