"""
Delta Table Vacuum Utility
========================

This notebook implements automated vacuum operations for Delta tables to manage
storage and performance optimization. It handles the cleanup of old file versions
and optimizes table storage.

Features:
- Automated vacuum operations across all tables in a schema
- Configurable retention period
- Safety checks disabled for maintenance operations
- Batch processing of multiple tables
- Workflow and manual execution support

Dependencies:
    - delta.tables: For Delta Lake operations
    - dbutils: For Databricks utilities
    - spark: For SQL operations

Workflow Configuration:
    This notebook can be run as part of a Databricks workflow with the following parameters:
    - schema_name: Schema/database containing tables to vacuum
    - retention_days: (Optional) Number of days of history to retain
    - process_all_tables: (Optional) Whether to process all tables or just the first one

Author: Shubham Dalvi
Last Modified: 2024
"""

# COMMAND ----------
from delta.tables import DeltaTable
from typing import List, Dict, Optional
import logging
import json

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# COMMAND ----------
def get_workflow_parameters() -> Dict[str, str]:
    """
    Get parameters from workflow context if available.
    
    Returns:
        Dict[str, str]: Dictionary of workflow parameters
    """
    try:
        # Check if running in a workflow
        workflow_params = json.loads(dbutils.widgets.get("job_parameters"))
        return {
            "schema_name": workflow_params.get("schema_name", ""),
            "retention_days": workflow_params.get("retention_days", "4"),
            "process_all_tables": workflow_params.get("process_all_tables", "false")
        }
    except:
        return {}

def setup_widgets() -> Dict[str, any]:
    """
    Configure and initialize notebook widgets.
    Supports both workflow and manual execution.
    
    Returns:
        Dict[str, any]: Configuration parameters
    """
    # Setup widgets for manual execution
    dbutils.widgets.text("schema_name", "")
    dbutils.widgets.text("retention_days", "4")
    dbutils.widgets.dropdown("process_all_tables", "false", ["true", "false"])
    
    # Try to get workflow parameters first
    workflow_params = get_workflow_parameters()
    if workflow_params:
        return {
            "schema_name": workflow_params["schema_name"],
            "retention_days": int(workflow_params["retention_days"]),
            "process_all_tables": workflow_params["process_all_tables"].lower() == "true"
        }
    
    # Fall back to widget values for manual execution
    return {
        "schema_name": dbutils.widgets.get("schema_name"),
        "retention_days": int(dbutils.widgets.get("retention_days")),
        "process_all_tables": dbutils.widgets.get("process_all_tables").lower() == "true"
    }

def configure_vacuum_settings() -> None:
    """
    Configure Delta table vacuum settings.
    Disables retention duration check for maintenance operations.
    """
    logger.info("Configuring vacuum settings...")
    spark.conf.set("spark.databricks.delta.retentionDurationCheck.enabled", False)

def get_tables_in_schema(schema_name: str) -> List[str]:
    """
    Get list of all tables in the specified schema.
    
    Args:
        schema_name (str): Name of the schema to query
        
    Returns:
        List[str]: List of table names in the schema
    """
    logger.info(f"Getting tables from schema: {schema_name}")
    spark.sql(f"USE {schema_name}")
    df = spark.sql("SHOW TABLES").select("tableName").collect()
    return [i.tableName for i in df]

def vacuum_table(table_name: str, retention_days: int = 4) -> None:
    """
    Perform vacuum operation on a single table.
    
    Args:
        table_name (str): Name of the table to vacuum
        retention_days (int): Number of days of history to retain
    """
    logger.info(f"Vacuuming table: {table_name}")
    try:
        delta_table = DeltaTable.forName(spark, table_name)
        delta_table.vacuum(retention_days)
        logger.info(f"Successfully vacuumed table: {table_name}")
    except Exception as e:
        logger.error(f"Error vacuuming table {table_name}: {str(e)}")
        raise

def process_schema_tables(schema_name: str, retention_days: int = 4, process_all: bool = False) -> None:
    """
    Process all tables in a schema for vacuum operations.
    
    Args:
        schema_name (str): Name of the schema to process
        retention_days (int): Number of days of history to retain
        process_all (bool): Whether to process all tables or just the first one
    """
    if not schema_name:
        raise ValueError("Schema name is required")
        
    tables = get_tables_in_schema(schema_name)
    logger.info(f"Found {len(tables)} tables to process")
    
    for table in tables:
        vacuum_table(table, retention_days)
        if not process_all:
            logger.info("Processing single table only. Set process_all_tables=true to process all tables.")
            break

# COMMAND ----------
def main():
    """
    Main execution function that orchestrates the vacuum process.
    Supports both workflow and manual execution modes.
    """
    try:
        # Setup and configuration
        config = setup_widgets()
        configure_vacuum_settings()
        
        # Process tables
        process_schema_tables(
            schema_name=config["schema_name"],
            retention_days=config["retention_days"],
            process_all=config["process_all_tables"]
        )
        
        logger.info("Vacuum operations completed successfully")
    except Exception as e:
        logger.error(f"Error during vacuum operations: {str(e)}")
        raise

# COMMAND ----------
# Execute main function
if __name__ == "__main__":
    main()

# Usage Examples:
# 1. Workflow Execution:
#    Configure job with parameters:
#    {
#        "schema_name": "formula1_processed",
#        "retention_days": "7",
#        "process_all_tables": "true"
#    }
#
# 2. Manual Execution:
#    - Set schema_name widget to "formula1_processed"
#    - Set retention_days widget to "7" (optional)
#    - Set process_all_tables widget to "true" (optional)