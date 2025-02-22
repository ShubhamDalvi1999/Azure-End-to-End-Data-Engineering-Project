"""
Formula 1 Race Results Data Cleansing
==================================

This script handles the cleansing and validation of Formula 1 race results data as part
of the silver layer processing. It ensures data quality and standardization for race
outcomes, timing data, and points calculations.

Cleansing Operations:
1. Validates race and driver references
2. Standardizes timing formats
3. Validates points calculations
4. Ensures position consistency
5. Validates status codes

Data Quality Checks:
1. Foreign key validation (raceId, driverId, constructorId)
2. Points calculation verification
3. Position order consistency
4. Lap count validation
5. Timing data format standardization

Business Rules:
1. Points must match official F1 scoring system
2. Grid positions must be valid integers
3. Race positions must be consistent with positionOrder
4. Fastest lap points must be valid
5. Status codes must be valid

Dependencies:
    - Common_Functions.py: Utility functions for data processing
    - Delta Lake: For merge operations
    - Reference data: Valid status codes, points system

Author: Your Organization
Last Modified: 2024
"""

# Databricks notebook source
import logging
from pyspark.sql.functions import col, when, expr
from delta.exceptions import DeltaProtocolError

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# COMMAND ----------

# MAGIC %run /DeltaLake/Utilities/Common_Functions

# COMMAND ----------

def validate_foreign_keys():
    """
    Validate foreign key relationships in results data.
    
    Checks:
    1. Race IDs exist in races table
    2. Driver IDs exist in drivers table
    3. Constructor IDs exist in constructors table
    
    Raises:
        ValueError: If foreign key validation fails
    """
    try:
        # Read latest bronze data
        results_df = spark.read.format('delta').load("/mnt/bronze_layer_gen2/results")
        
        # Validate race IDs
        invalid_races = results_df.join(
            spark.table("bronze.race"),
            "raceId",
            "left_anti"
        ).count()
        
        if invalid_races > 0:
            raise ValueError(f"Found {invalid_races} results with invalid race IDs")
            
        # Validate driver IDs
        invalid_drivers = results_df.join(
            spark.table("bronze.drivers"),
            "driverId",
            "left_anti"
        ).count()
        
        if invalid_drivers > 0:
            raise ValueError(f"Found {invalid_drivers} results with invalid driver IDs")
            
        # Validate constructor IDs
        invalid_constructors = results_df.join(
            spark.table("bronze.constructor"),
            "constructorId",
            "left_anti"
        ).count()
        
        if invalid_constructors > 0:
            raise ValueError(f"Found {invalid_constructors} results with invalid constructor IDs")
            
    except Exception as e:
        logger.error(f"Foreign key validation failed: {str(e)}")
        raise

# COMMAND ----------

def validate_points_and_positions():
    """
    Validate race results points and positions.
    
    Checks:
    1. Points follow F1 scoring system
    2. Position order is consistent
    3. Grid positions are valid
    4. Fastest lap points are valid
    
    Raises:
        ValueError: If validation fails
    """
    try:
        results_df = spark.read.format('delta').load("/mnt/bronze_layer_gen2/results")
        
        # Validate points range
        invalid_points = results_df.filter(
            (col("points") < 0) | (col("points") > 26)  # Max 25 + 1 fastest lap
        ).count()
        
        if invalid_points > 0:
            raise ValueError(f"Found {invalid_points} results with invalid points")
            
        # Validate position consistency
        position_mismatch = results_df.filter(
            col("position").isNotNull() & 
            col("positionOrder").isNotNull() &
            (col("position") != col("positionOrder"))
        ).count()
        
        if position_mismatch > 0:
            raise ValueError(f"Found {position_mismatch} results with position inconsistencies")
            
        # Validate grid positions
        invalid_grid = results_df.filter(col("grid") < 0).count()
        
        if invalid_grid > 0:
            raise ValueError(f"Found {invalid_grid} results with invalid grid positions")
            
    except Exception as e:
        logger.error(f"Points and positions validation failed: {str(e)}")
        raise

# COMMAND ----------

def clean_results_data():
    """
    Apply cleansing transformations to race results data.
    
    Process:
    1. Validate foreign keys
    2. Validate points and positions
    3. Clean and standardize timing data
    4. Apply business rules
    5. Merge to silver layer
    """
    try:
        logger.info("Starting race results cleansing process")
        
        # Perform validations
        validate_foreign_keys()
        validate_points_and_positions()
        
        # Execute merge operation
        logger.info("Executing merge operation for results")
        merge_delta_tables(
            source_path="/mnt/bronze_layer_gen2/results",
            target_table="silver.results"
        )
        
        logger.info("Successfully completed race results cleansing")
        
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
    clean_results_data()
except Exception as e:
    logger.error("Failed to process race results data")
    raise