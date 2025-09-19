"""S3 data extractor."""

from typing import Any, Dict, Union

import boto3
import pandas as pd
import structlog
from botocore.exceptions import ClientError, NoCredentialsError

from .base import BaseExtractor

logger = structlog.get_logger()


class CustomExtractionError(Exception):
    """Custom exception for extraction errors."""
    pass


class S3Extractor(BaseExtractor):
    """Extract data from S3 buckets."""
    
    def validate_config(self) -> None:
        """Validate S3 extractor configuration."""
        required_fields = ["bucket", "key"]
        for field in required_fields:
            if not self.config.get(field):
                raise ValueError(f"S3 extractor requires '{field}' in configuration")
    
    def _get_s3_client(self):
        """Create S3 client with credentials."""
        client_params = {}
        
        if self.config.get("access_key") and self.config.get("secret_key"):
            client_params.update({
                "aws_access_key_id": self.config["access_key"],
                "aws_secret_access_key": self.config["secret_key"],
            })
        
        if self.config.get("region"):
            client_params["region_name"] = self.config["region"]
        
        return boto3.client("s3", **client_params)
    
    def run(self) -> Union[pd.DataFrame, pd.io.parsers.readers.TextFileReader]:
        """Extract data from S3."""
        bucket = self.config["bucket"]
        key = self.config["key"]
        
        try:
            client = self._get_s3_client()
            
            logger.info("Fetching data from S3", bucket=bucket, key=key)
            
            # Get object from S3
            response = client.get_object(Bucket=bucket, Key=key)
            
            # Determine file type and read accordingly
            if key.endswith(".csv"):
                csv_params = {
                    "encoding": self.config.get("encoding", "utf-8"),
                    "sep": self.config.get("separator", ","),
                    "header": self.config.get("header", 0),
                    "chunksize": self.config.get("chunksize"),
                }
                csv_params = {k: v for k, v in csv_params.items() if v is not None}
                
                if csv_params.get("chunksize"):
                    return pd.read_csv(response["Body"], **csv_params)
                else:
                    df = pd.read_csv(response["Body"], **csv_params)
                    logger.info("S3 CSV data extracted", rows=len(df), columns=len(df.columns))
                    return df
            
            elif key.endswith(".parquet"):
                df = pd.read_parquet(response["Body"])
                logger.info("S3 Parquet data extracted", rows=len(df), columns=len(df.columns))
                return df
            
            elif key.endswith(".json"):
                df = pd.read_json(response["Body"])
                logger.info("S3 JSON data extracted", rows=len(df), columns=len(df.columns))
                return df
            
            else:
                # Default to CSV
                df = pd.read_csv(response["Body"])
                logger.info("S3 data extracted (default CSV)", rows=len(df), columns=len(df.columns))
                return df
        
        except NoCredentialsError as e:
            logger.error("S3 credentials not found")
            raise CustomExtractionError("S3 credentials not configured") from e
        
        except ClientError as e:
            error_code = e.response["Error"]["Code"]
            logger.error("S3 client error", error_code=error_code, bucket=bucket, key=key)
            raise CustomExtractionError(f"S3 fetch failed: {error_code}") from e
        
        except Exception as e:
            logger.error("S3 extraction failed", bucket=bucket, key=key, error=str(e))
            raise CustomExtractionError(f"S3 extraction failed: {e}") from e