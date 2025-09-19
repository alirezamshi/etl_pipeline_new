"""Neo4j graph database extractor."""

from typing import Any, Dict, List, Union

import pandas as pd
import structlog
from neo4j import GraphDatabase
from neo4j.exceptions import ServiceUnavailable, AuthError

from .base import BaseExtractor

logger = structlog.get_logger()


class Neo4jExtractor(BaseExtractor):
    """Extract data from Neo4j graph database."""
    
    def validate_config(self) -> None:
        """Validate Neo4j extractor configuration."""
        required_fields = ["uri", "user", "password"]
        for field in required_fields:
            if not self.config.get(field):
                raise ValueError(f"Neo4j extractor requires '{field}' in configuration")
        
        if not self.config.get("query"):
            raise ValueError("Neo4j extractor requires 'query' in configuration")
    
    def run(self) -> Union[pd.DataFrame, List[Dict[str, Any]]]:
        """Extract data from Neo4j using Cypher query."""
        uri = self.config["uri"]
        user = self.config["user"]
        password = self.config["password"]
        query = self.config["query"]
        
        driver = None
        try:
            driver = GraphDatabase.driver(uri, auth=(user, password))
            
            logger.info("Executing Neo4j query", uri=uri, user=user)
            
            with driver.session() as session:
                result = session.run(query, self.config.get("parameters", {}))
                
                # Convert to list of dictionaries
                records = []
                for record in result:
                    records.append(dict(record))
                
                logger.info("Neo4j data extracted", records=len(records))
                
                # Return as DataFrame if requested, otherwise as list
                if self.config.get("return_dataframe", True):
                    if records:
                        df = pd.DataFrame(records)
                        logger.info("Converted to DataFrame", rows=len(df), columns=len(df.columns))
                        return df
                    else:
                        return pd.DataFrame()
                else:
                    return records
        
        except AuthError as e:
            logger.error("Neo4j authentication failed", uri=uri, user=user)
            raise RuntimeError(f"Neo4j authentication failed: {e}") from e
        
        except ServiceUnavailable as e:
            logger.error("Neo4j service unavailable", uri=uri)
            raise RuntimeError(f"Neo4j service unavailable: {e}") from e
        
        except Exception as e:
            logger.error("Neo4j extraction failed", uri=uri, error=str(e))
            raise RuntimeError(f"Neo4j extraction failed: {e}") from e
        
        finally:
            if driver:
                driver.close()