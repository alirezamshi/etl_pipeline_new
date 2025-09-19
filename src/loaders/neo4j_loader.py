"""Neo4j graph database loader."""

from typing import Any, Dict, List, Union

import pandas as pd
import structlog
from neo4j import GraphDatabase
from neo4j.exceptions import ServiceUnavailable, AuthError

from .base import BaseLoader

logger = structlog.get_logger()


def chunk_data(data: List[Dict[str, Any]], chunk_size: int) -> List[List[Dict[str, Any]]]:
    """Split data into chunks for batch processing."""
    for i in range(0, len(data), chunk_size):
        yield data[i:i + chunk_size]


class Neo4jLoader(BaseLoader):
    """Load data to Neo4j graph database."""
    
    def validate_config(self) -> None:
        """Validate Neo4j loader configuration."""
        required_fields = ["uri", "user", "password"]
        for field in required_fields:
            if not self.config.get(field):
                raise ValueError(f"Neo4j loader requires '{field}' in configuration")
    
    def _upsert_nodes_batch(self, tx, batch: List[Dict[str, Any]], overwrite: bool):
        """Upsert a batch of nodes."""
        node_label = self.config.get("node_label", "Node")
        id_field = self.config.get("id_field", "id")
        
        if overwrite:
            query = f"""
            UNWIND $records AS rec
            MERGE (n:{node_label} {{{id_field}: rec.{id_field}}})
            SET n = rec
            """
        else:
            query = f"""
            UNWIND $records AS rec
            MERGE (n:{node_label} {{{id_field}: rec.{id_field}}})
            ON CREATE SET n = rec
            ON MATCH SET n += rec
            """
        
        tx.run(query, records=batch)
    
    def _create_relationships_batch(self, tx, batch: List[Dict[str, Any]]):
        """Create a batch of relationships."""
        rel_type = self.config.get("relationship_type", "RELATED_TO")
        from_field = self.config.get("from_field", "from_id")
        to_field = self.config.get("to_field", "to_id")
        from_label = self.config.get("from_label", "Node")
        to_label = self.config.get("to_label", "Node")
        id_field = self.config.get("id_field", "id")
        
        query = f"""
        UNWIND $records AS rec
        MATCH (from:{from_label} {{{id_field}: rec.{from_field}}})
        MATCH (to:{to_label} {{{id_field}: rec.{to_field}}})
        MERGE (from)-[r:{rel_type}]->(to)
        SET r += rec
        """
        
        tx.run(query, records=batch)
    
    def load(self, data: Union[pd.DataFrame, list, dict], overwrite: bool = False) -> None:
        """Load data to Neo4j."""
        uri = self.config["uri"]
        user = self.config["user"]
        password = self.config["password"]
        batch_size = self.config.get("batch_size", 1000)
        
        driver = None
        try:
            driver = GraphDatabase.driver(uri, auth=(user, password))
            
            # Convert data to list of dictionaries
            if isinstance(data, pd.DataFrame):
                records = data.to_dict(orient="records")
            elif isinstance(data, list):
                records = data
            elif isinstance(data, dict):
                records = [data]
            else:
                raise ValueError(f"Unsupported data type for Neo4j loader: {type(data)}")
            
            logger.info("Loading data to Neo4j", 
                       uri=uri, 
                       user=user, 
                       records=len(records),
                       batch_size=batch_size)
            
            # Determine loading strategy
            load_type = self.config.get("load_type", "nodes")
            
            with driver.session() as session:
                if load_type == "nodes":
                    # Load as nodes
                    for batch in chunk_data(records, batch_size):
                        session.execute_write(self._upsert_nodes_batch, batch, overwrite)
                        logger.debug("Loaded node batch", batch_size=len(batch))
                
                elif load_type == "relationships":
                    # Load as relationships
                    for batch in chunk_data(records, batch_size):
                        session.execute_write(self._create_relationships_batch, batch)
                        logger.debug("Loaded relationship batch", batch_size=len(batch))
                
                elif load_type == "custom":
                    # Use custom Cypher query
                    custom_query = self.config.get("custom_query")
                    if not custom_query:
                        raise ValueError("Custom load type requires 'custom_query' in configuration")
                    
                    for batch in chunk_data(records, batch_size):
                        session.run(custom_query, records=batch)
                        logger.debug("Executed custom query batch", batch_size=len(batch))
                
                else:
                    raise ValueError(f"Unknown load_type: {load_type}. Use 'nodes', 'relationships', or 'custom'")
            
            logger.info("Data loaded to Neo4j successfully", records=len(records))
        
        except AuthError as e:
            logger.error("Neo4j authentication failed", uri=uri, user=user)
            raise RuntimeError(f"Neo4j authentication failed: {e}") from e
        
        except ServiceUnavailable as e:
            logger.error("Neo4j service unavailable", uri=uri)
            raise RuntimeError(f"Neo4j service unavailable: {e}") from e
        
        except Exception as e:
            logger.error("Neo4j loading failed", uri=uri, error=str(e))
            raise RuntimeError(f"Neo4j loading failed: {e}") from e
        
        finally:
            if driver:
                driver.close()