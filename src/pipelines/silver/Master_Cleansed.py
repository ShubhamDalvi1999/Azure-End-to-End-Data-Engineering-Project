"""
Formula 1 Data Cleansing Master Script
====================================

This script serves as the main orchestrator for the data cleansing process in the Formula 1
data pipeline. It handles the transformation of data from bronze (raw) to silver (cleansed)
layer following the medallion architecture pattern.

Key Features:
1. Dynamic table processing based on input parameter
2. Automated merge condition generation
3. Delta Lake merge operations with full history
4. Error handling and logging

Process Flow:
1. Receives table name as parameter
2. Validates table existence in both layers
3. Performs schema validation
4. Executes merge operation with automatic key detection
5. Maintains audit trail

Dependencies:
    - Common_Functions.py: Utility functions for data processing
    - Delta Lake: For merge operations
    - Databricks Runtime: For widget and utility functions

Author: Your Organization
Last Modified: 2024
"""

# Databricks notebook source
import logging
from pyspark.sql.utils import AnalysisException
from delta.exceptions import DeltaProtocolError

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# COMMAND ----------

# Setup input parameter
dbutils.widgets.text("tablename", "")
tablename = dbutils.widgets.get("tablename")

if not tablename:
    raise ValueError("Table name parameter is required")

# COMMAND ----------

# MAGIC %run /DeltaLake/Utilities/Common_Functions

# COMMAND ----------

def validate_table_exists(table_name: str, database: str) -> bool:
    """
    Validate if a table exists in the specified database.
    
    Args:
        table_name (str): Name of the table to check
        database (str): Database name (bronze/silver/gold)
        
    Returns:
        bool: True if table exists, False otherwise
    """
    try:
        spark.sql(f"DESCRIBE {database}.{table_name}")
        return True
    except AnalysisException:
        return False

# COMMAND ----------

def process_table_cleansing(table_name: str):
    """
    Process a single table's data cleansing operation.
    
    Handles the complete cleansing process including:
    1. Table validation
    2. Data retrieval
    3. Merge operation
    4. Error handling
    
    Args:
        table_name (str): Name of the table to process
        
    Raises:
        ValueError: If table validation fails
        Exception: For other processing errors
    """
    try:
        logger.info(f"Starting cleansing process for table: {table_name}")
        
        # Validate table existence
        if not validate_table_exists(table_name, "bronze"):
            raise ValueError(f"Source table bronze.{table_name} does not exist")
        if not validate_table_exists(table_name, "silver"):
            raise ValueError(f"Target table silver.{table_name} does not exist")
            
        # Execute merge operation
        logger.info(f"Executing merge operation for {table_name}")
        merge_delta_tables(
            source_path=f"/mnt/bronze_layer_gen2/{table_name}",
            target_table=f"silver.{table_name}"
        )
        
        logger.info(f"Successfully completed cleansing for table: {table_name}")
        
    except ValueError as ve:
        logger.error(f"Validation error for {table_name}: {str(ve)}")
        raise
    except DeltaProtocolError as dpe:
        logger.error(f"Delta protocol error for {table_name}: {str(dpe)}")
        raise
    except Exception as e:
        logger.error(f"Unexpected error processing {table_name}: {str(e)}")
        raise

# COMMAND ----------

# Main execution
try:
    process_table_cleansing(tablename)
except Exception as e:
    logger.error(f"Failed to process table {tablename}")
    raise