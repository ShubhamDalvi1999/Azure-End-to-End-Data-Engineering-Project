# Databricks notebook source
# MAGIC %md
# MAGIC # Formula 1 Data Pipeline Orchestration
# MAGIC 
# MAGIC This notebook orchestrates the entire Formula 1 data processing pipeline using the medallion architecture:
# MAGIC - Bronze Layer: Raw data ingestion
# MAGIC - Silver Layer: Cleaned and transformed data
# MAGIC - Gold Layer: Business-ready aggregated data
# MAGIC 
# MAGIC The pipeline is designed to be idempotent and can be executed through Databricks Workflows.

# COMMAND ----------
# MAGIC %md
# MAGIC ## Workflow Parameters and Widgets

# COMMAND ----------

# MAGIC %python
# MAGIC # Create widgets for parameter passing
# MAGIC dbutils.widgets.text("processing_date", "", "Processing Date (YYYY-MM-DD)")
# MAGIC dbutils.widgets.dropdown("environment", "dev", ["dev", "staging", "prod"], "Environment")
# MAGIC dbutils.widgets.dropdown("layer_to_process", "all", ["all", "bronze", "silver", "gold"], "Layer to Process")

# COMMAND ----------
# MAGIC %md
# MAGIC ## Setup and Configuration

# COMMAND ----------

import sys
from datetime import datetime
from typing import List, Dict, Any
from pyspark.sql import SparkSession

# Add src to Python path
sys.path.append("/Workspace/Repos/azure-databricks-formula1/src")

from utils.logger import Logger
from config.pipeline_config import PipelineConfig
from pipelines.bronze import BronzeLayer
from pipelines.silver import SilverLayer
from pipelines.gold import GoldLayer

# Initialize logger and spark session
logger = Logger()
spark = SparkSession.builder.getOrCreate()

# COMMAND ----------
# MAGIC %md
# MAGIC ## Pipeline Configuration

# COMMAND ----------

# Get parameters from widgets
processing_date = dbutils.widgets.get("processing_date")
environment = dbutils.widgets.get("environment")
layer_to_process = dbutils.widgets.get("layer_to_process")

# If processing_date is not provided, use current date
if not processing_date:
    processing_date = datetime.now().strftime("%Y-%m-%d")

# Get pipeline configuration with environment
pipeline_config = PipelineConfig(environment=environment)

# Log pipeline parameters
logger.info(f"Pipeline Parameters:")
logger.info(f"Processing Date: {processing_date}")
logger.info(f"Environment: {environment}")
logger.info(f"Layer to Process: {layer_to_process}")

# COMMAND ----------
# MAGIC %md
# MAGIC ## Pipeline Execution Functions

# COMMAND ----------

def process_bronze_layer(date: str, config: PipelineConfig) -> bool:
    """
    Process bronze layer data ingestion
    Args:
        date: Processing date
        config: Pipeline configuration
    Returns:
        bool: Success status
    """
    try:
        bronze_layer = BronzeLayer(config)
        bronze_layer.process(date)
        return True
    except Exception as e:
        error_msg = f"Error processing bronze layer: {str(e)}"
        logger.error(error_msg)
        raise Exception(error_msg)

def process_silver_layer(date: str, config: PipelineConfig) -> bool:
    """
    Process silver layer transformations
    Args:
        date: Processing date
        config: Pipeline configuration
    Returns:
        bool: Success status
    """
    try:
        silver_layer = SilverLayer(config)
        silver_layer.process(date)
        return True
    except Exception as e:
        error_msg = f"Error processing silver layer: {str(e)}"
        logger.error(error_msg)
        raise Exception(error_msg)

def process_gold_layer(date: str, config: PipelineConfig) -> bool:
    """
    Process gold layer aggregations
    Args:
        date: Processing date
        config: Pipeline configuration
    Returns:
        bool: Success status
    """
    try:
        gold_layer = GoldLayer(config)
        gold_layer.process(date)
        return True
    except Exception as e:
        error_msg = f"Error processing gold layer: {str(e)}"
        logger.error(error_msg)
        raise Exception(error_msg)

# COMMAND ----------
# MAGIC %md
# MAGIC ## Main Pipeline Execution

# COMMAND ----------

def main(processing_date: str, config: PipelineConfig, layer: str = "all"):
    """
    Main pipeline orchestration function
    Args:
        processing_date: Date to process data for
        config: Pipeline configuration
        layer: Layer to process (all, bronze, silver, or gold)
    """
    logger.info(f"Starting Formula 1 data pipeline for date: {processing_date}")
    
    try:
        # Process bronze layer
        if layer in ["all", "bronze"]:
            logger.info("Starting bronze layer processing...")
            process_bronze_layer(processing_date, config)
            logger.info("Bronze layer processing completed successfully.")
        
        # Process silver layer
        if layer in ["all", "silver"]:
            logger.info("Starting silver layer processing...")
            process_silver_layer(processing_date, config)
            logger.info("Silver layer processing completed successfully.")
        
        # Process gold layer
        if layer in ["all", "gold"]:
            logger.info("Starting gold layer processing...")
            process_gold_layer(processing_date, config)
            logger.info("Gold layer processing completed successfully.")
        
        logger.info(f"Formula 1 data pipeline completed successfully for date: {processing_date}")
        
        # Set exit state for workflow
        dbutils.notebook.exit("SUCCESS")
        
    except Exception as e:
        error_msg = f"Pipeline failed: {str(e)}"
        logger.error(error_msg)
        # Set failed exit state for workflow
        dbutils.notebook.exit("FAILED: " + error_msg)

# COMMAND ----------
# MAGIC %md
# MAGIC ## Pipeline Execution

# COMMAND ----------

# Execute the pipeline
if __name__ == "__main__":
    main(processing_date, pipeline_config, layer_to_process) 