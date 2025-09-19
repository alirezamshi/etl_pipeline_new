"""Idempotency utilities for pipeline runs."""

import hashlib
import json
from pathlib import Path
from typing import Any, Dict

import structlog

logger = structlog.get_logger()


def generate_config_hash(config: Dict[str, Any]) -> str:
    """Generate a hash from pipeline configuration for idempotency checks."""
    # Create a normalized version of config for hashing
    config_str = json.dumps(config, sort_keys=True, default=str)
    return hashlib.sha256(config_str.encode()).hexdigest()[:16]


def check_idempotency_hash(config: Dict[str, Any], hash_file: str = ".pipeline_hash") -> bool:
    """Check if pipeline has already been run with the same configuration."""
    current_hash = generate_config_hash(config)
    hash_path = Path(hash_file)
    
    if hash_path.exists():
        try:
            with open(hash_path, "r") as f:
                stored_hash = f.read().strip()
            
            if stored_hash == current_hash:
                logger.info("Pipeline configuration unchanged, skipping run", hash=current_hash)
                return True
        except Exception as e:
            logger.warning("Could not read hash file", error=str(e))
    
    # Store current hash
    try:
        with open(hash_path, "w") as f:
            f.write(current_hash)
        logger.debug("Stored new configuration hash", hash=current_hash)
    except Exception as e:
        logger.warning("Could not write hash file", error=str(e))
    
    return False


def reset_idempotency(hash_file: str = ".pipeline_hash") -> None:
    """Reset idempotency check by removing hash file."""
    hash_path = Path(hash_file)
    if hash_path.exists():
        hash_path.unlink()
        logger.info("Reset pipeline idempotency check")