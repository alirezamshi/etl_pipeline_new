"""S3 data loader."""

import io
from typing import Any, Dict, Union

import boto3
import pandas as pd
import structlog
from botocore.exceptions import ClientError, NoCredentialsError

from .base import BaseLoader

logger = structlog.get_logger()


class S3Loader(BaseLoader):
    """Load data to S3 buckets."""
    
    def validate_config(self) -> None:
        """Validate S3 loader configuration."""
        required_fields = ["bucket", "key"]
        for field in required_fields:
            if not self.config.get(field):
                raise ValueError(f"S3 loader requires '{field}' in configuration")
    
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
    
    def _check_object_exists(self, client, bucket: str, key: str) -> bool:
        """Check if S3 object exists."""
        try:
            client.head_object(Bucket=bucket, Key=key)
            return True
        except ClientError as e:
            if e.response["Error"]["Code"] == "404":
                return False
            raise
    
    def load(self, data: Union[pd.DataFrame, list, dict], overwrite: bool = False) -> None:
        """Load data to S3."""
        bucket = self.config["bucket"]
        key = self.config["key"]
        
        try:
            client = self._get_s3_client()
            
            # Check if object exists
            if not overwrite and self._check_object_exists(client, bucket, key):
                raise FileExistsError(f"S3 object already exists: s3://{bucket}/{key}. Use overwrite=True")
            
            # Convert data to appropriate format
            if isinstance(data, pd.DataFrame):
                df = data
            elif isinstance(data, list):
                df = pd.DataFrame(data)
            elif isinstance(data, dict):
                df = pd.DataFrame([data])
            else:
                raise ValueError(f"Unsupported data type for S3 loader: {type(data)}")
            
            # Determine format and upload
            if key.endswith(".csv"):
                buffer = io.StringIO()
                df.to_csv(buffer, index=False)
                content = buffer.getvalue()
                content_type = "text/csv"
            
            elif key.endswith(".parquet"):
                buffer = io.BytesIO()
                df.to_parquet(buffer, index=False)
                content = buffer.getvalue()
                content_type = "application/octet-stream"
            
            elif key.endswith(".json"):
                buffer = io.StringIO()
                df.to_json(buffer, orient="records", indent=2)
                content = buffer.getvalue()
                content_type = "application/json"
            
            else:
                # Default to CSV
                buffer = io.StringIO()
                df.to_csv(buffer, index=False)
                content = buffer.getvalue()
                content_type = "text/csv"
            
            # Upload to S3
            extra_args = {"ContentType": content_type}
            if self.config.get("server_side_encryption"):
                extra_args["ServerSideEncryption"] = self.config["server_side_encryption"]
            
            if isinstance(content, str):
                client.put_object(
                    Bucket=bucket,
                    Key=key,
                    Body=content.encode("utf-8"),
                    **extra_args
                )
            else:
                client.put_object(
                    Bucket=bucket,
                    Key=key,
                    Body=content,
                    **extra_args
                )
            
            logger.info("Data loaded to S3", 
                       bucket=bucket, 
                       key=key, 
                       rows=len(df), 
                       columns=len(df.columns),
                       content_type=content_type)
        
        except NoCredentialsError as e:
            logger.error("S3 credentials not found")
            raise RuntimeError("S3 credentials not configured") from e
        
        except ClientError as e:
            error_code = e.response["Error"]["Code"]
            logger.error("S3 client error", error_code=error_code, bucket=bucket, key=key)
            raise RuntimeError(f"S3 upload failed: {error_code}") from e
        
        except Exception as e:
            logger.error("S3 loading failed", bucket=bucket, key=key, error=str(e))
            raise RuntimeError(f"S3 loading failed: {e}") from e