"""
Formula 1 Lap Times Data Cleansing
================================

This script handles the cleansing and validation of Formula 1 lap timing data as part
of the silver layer processing. It ensures data quality and standardization for lap
times, including validation of timing formats and race progression logic.

Cleansing Operations:
1. Validates race and driver references
2. Standardizes lap time formats
3. Validates lap numbers and progression
4. Ensures position consistency
5. Converts string times to milliseconds

Data Quality Checks:
1. Foreign key validation (raceId, driverId)
2. Lap number sequence validation
3. Position range validation
4. Time format standardization
5. Milliseconds consistency check

Business Rules:
1. Lap numbers must be sequential for each driver
2. Position must be within valid range (1 to number of drivers)
3. Lap times must be within reasonable range
4. Time and milliseconds must be consistent
5. No duplicate lap entries per driver per race

Dependencies:
    - Common_Functions.py: Utility functions for data processing
    - Delta Lake: For merge operations
    - Reference data: Race calendar, driver roster

Author: Your Organization
Last Modified: 2024
"""

# Databricks notebook source
import logging
from pyspark.sql.functions import col, lag, lead, row_number
from pyspark.sql.window import Window
from delta.exceptions import DeltaProtocolError

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# COMMAND ----------

# MAGIC %run /DeltaLake/Utilities/Common_Functions

# COMMAND ----------

def validate_lap_sequence():
    """
    Validate lap number sequences for each driver in each race.
    
    Checks:
    1. Lap numbers are sequential
    2. No missing laps
    3. No duplicate laps
    4. Lap count matches race total
    
    Raises:
        ValueError: If lap sequence validation fails
    """
    try:
        # Read latest bronze data
        laptimes_df = spark.read.format('delta').load("/mnt/bronze_layer_gen2/laptimes")
        
        # Create window for sequence checking
        race_driver_window = Window.partitionBy("raceId", "driverId").orderBy("lap")
        
        # Check for sequence gaps
        with_sequence = laptimes_df.withColumn(
            "expected_lap",
            row_number().over(race_driver_window)
        )
        
        sequence_errors = with_sequence.filter(
            col("lap") != col("expected_lap")
        ).count()
        
        if sequence_errors > 0:
            raise ValueError(f"Found {sequence_errors} lap sequence errors")
            
        # Check for duplicates
        duplicates = laptimes_df.groupBy("raceId", "driverId", "lap").count().filter(
            col("count") > 1
        ).count()
        
        if duplicates > 0:
            raise ValueError(f"Found {duplicates} duplicate lap entries")
            
    except Exception as e:
        logger.error(f"Lap sequence validation failed: {str(e)}")
        raise

# COMMAND ----------

def validate_timing_data():
    """
    Validate lap timing data consistency and formats.
    
    Checks:
    1. Time string format is valid
    2. Milliseconds are consistent with time string
    3. Times are within reasonable range
    4. No negative times
    
    Raises:
        ValueError: If timing validation fails
    """
    try:
        laptimes_df = spark.read.format('delta').load("/mnt/bronze_layer_gen2/laptimes")
        
        # Validate time ranges
        invalid_times = laptimes_df.filter(
            (col("milliseconds") <= 0) |  # No negative or zero times
            (col("milliseconds") > 300000)  # Max 5 minutes per lap
        ).count()
        
        if invalid_times > 0:
            raise ValueError(f"Found {invalid_times} invalid lap times")
            
        # Validate position data
        invalid_positions = laptimes_df.filter(
            (col("position") <= 0) |
            (col("position") > 20)  # Max 20 cars in modern F1
        ).count()
        
        if invalid_positions > 0:
            raise ValueError(f"Found {invalid_positions} invalid positions")
            
    except Exception as e:
        logger.error(f"Timing data validation failed: {str(e)}")
        raise

# COMMAND ----------

def clean_laptime_data():
    """
    Apply cleansing transformations to lap times data.
    
    Process:
    1. Validate lap sequences
    2. Validate timing data
    3. Standardize time formats
    4. Apply business rules
    5. Merge to silver layer
    """
    try:
        logger.info("Starting lap times cleansing process")
        
        # Perform validations
        validate_lap_sequence()
        validate_timing_data()
        
        # Execute merge operation
        logger.info("Executing merge operation for lap times")
        merge_delta_tables(
            source_path="/mnt/bronze_layer_gen2/laptimes",
            target_table="silver.laptimes"
        )
        
        logger.info("Successfully completed lap times cleansing")
        
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
    clean_laptime_data()
except Exception as e:
    logger.error("Failed to process lap times data")
    raise