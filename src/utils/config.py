"""Configuration management utilities."""

import os
from pathlib import Path
from typing import Any, Dict

import yaml
from pydantic import BaseModel, Field


class SourceConfig(BaseModel):
    """Configuration for data sources."""
    type: str
    path: str = ""
    bucket: str = ""
    key: str = ""
    uri: str = ""
    user: str = ""
    password: str = ""
    access_key: str = ""
    secret_key: str = ""
    query: str = ""
    table: str = ""


class TransformConfig(BaseModel):
    """Configuration for data transformations."""
    rules: Dict[str, Any] = Field(default_factory=dict)
    sql_file: str = ""
    dedup_cols: list[str] = Field(default_factory=list)
    filter_expr: str = ""


class LoadConfig(BaseModel):
    """Configuration for data loading."""
    type: str
    path: str = ""
    bucket: str = ""
    key: str = ""
    uri: str = ""
    user: str = ""
    password: str = ""
    access_key: str = ""
    secret_key: str = ""
    table: str = ""
    overwrite: bool = False
    batch_size: int = 1000


class IntermediateConfig(BaseModel):
    """Configuration for intermediate data storage."""
    extract: str = ""
    transform: str = ""
    cleanup: bool = True
    persist_steps: list[str] = Field(default_factory=list)


class PipelineConfig(BaseModel):
    """Main pipeline configuration."""
    source: SourceConfig
    transform: TransformConfig
    load: LoadConfig
    intermediates: IntermediateConfig = Field(default_factory=IntermediateConfig)
    environment: str = "dev"


def load_config(config_path: str) -> Dict[str, Any]:
    """Load configuration from YAML file with environment variable substitution."""
    config_file = Path(config_path)
    if not config_file.exists():
        raise FileNotFoundError(f"Configuration file not found: {config_path}")
    
    with open(config_file, "r") as f:
        config_content = f.read()
    
    # Replace environment variables
    config_content = os.path.expandvars(config_content)
    
    config_data = yaml.safe_load(config_content)
    
    # Validate configuration
    try:
        PipelineConfig(**config_data)
    except Exception as e:
        raise ValueError(f"Invalid configuration: {e}")
    
    return config_data


def get_secret(key: str, default: str = "") -> str:
    """Get secret from environment variables."""
    return os.getenv(key, default)