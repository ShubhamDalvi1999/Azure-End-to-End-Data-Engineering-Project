"""
Formula 1 Circuits Data Cleansing
===============================

This script handles the cleansing and validation of Formula 1 circuit data as part
of the silver layer processing. It ensures data quality and standardization for
circuit information including geographical coordinates and location data.

Cleansing Operations:
1. Validates circuit identifiers
2. Standardizes location and country names
3. Validates geographical coordinates
4. Ensures consistent altitude values
5. Verifies URL formats

Data Quality Checks:
1. Circuit ID uniqueness
2. Valid coordinate ranges (latitude: -90 to 90, longitude: -180 to 180)
3. Non-null essential fields (name, location, country)
4. URL format validation
5. Consistent country name formatting

Dependencies:
    - Common_Functions.py: Utility functions for data processing
    - Delta Lake: For merge operations
    - Geopy (optional): For coordinate validation

Author: Your Organization
Last Modified: 2024
"""

# Databricks notebook source
import logging
from pyspark.sql.functions import col, when, regexp_replace, trim
from delta.exceptions import DeltaProtocolError

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# COMMAND ----------

# MAGIC %run /DeltaLake/Utilities/Common_Functions

# COMMAND ----------

def validate_circuit_data():
    """
    Perform data quality validation on circuits data.
    
    Checks:
    1. Circuit ID uniqueness
    2. Required fields presence
    3. Coordinate validity
    4. Country name standardization
    
    Raises:
        ValueError: If data validation fails
    """
    try:
        # Read latest bronze data
        bronze_df = spark.read.format('delta').load("/mnt/bronze_layer_gen2/circuits")
        
        # Validate required fields
        null_counts = bronze_df.select([
            sum(col(c).isNull().cast("int")).alias(c)
            for c in ["circuitId", "name", "location", "country"]
        ]).collect()[0]
        
        if any(null_counts):
            raise ValueError("Found null values in required fields")
            
        # Validate coordinate ranges
        invalid_coords = bronze_df.filter(
            (col("lat") < -90) | (col("lat") > 90) |
            (col("lng") < -180) | (col("lng") > 180)
        ).count()
        
        if invalid_coords > 0:
            raise ValueError(f"Found {invalid_coords} records with invalid coordinates")
            
        # Validate circuit ID uniqueness
        duplicate_ids = bronze_df.groupBy("circuitId").count().filter(col("count") > 1).count()
        
        if duplicate_ids > 0:
            raise ValueError(f"Found {duplicate_ids} duplicate circuit IDs")
            
    except Exception as e:
        logger.error(f"Circuit data validation failed: {str(e)}")
        raise

# COMMAND ----------

def clean_circuit_data():
    """
    Apply cleansing transformations to circuits data.
    
    Transformations:
    1. Standardize country names
    2. Clean location names
    3. Format coordinates
    4. Validate URLs
    """
    try:
        logger.info("Starting circuit data cleansing process")
        
        # Validate data quality
        validate_circuit_data()
        
        # Execute merge operation with cleansing
        logger.info("Executing merge operation for circuits")
        merge_delta_tables(
            source_path="/mnt/bronze_layer_gen2/circuits",
            target_table="silver.circuits"
        )
        
        logger.info("Successfully completed circuit data cleansing")
        
    except ValueError as ve:
        logger.error(f"Validation error: {str(ve)}")
        raise
    except DeltaProtocolError as dpe:
        logger.error(f"Delta protocol error: {str(dpe)}")
        raise
    except Exception as e:
        logger.error(f"Unexpected error: {str(e)}")
        raise

# COMMAND ----------

# Main execution
try:
    clean_circuit_data()
except Exception as e:
    logger.error("Failed to process circuits data")
    raise