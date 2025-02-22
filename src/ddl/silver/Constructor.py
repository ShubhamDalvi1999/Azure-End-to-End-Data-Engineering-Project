# Databricks notebook source

"""
This script creates the silver.constructor table which stores cleansed Formula 1 constructor data.
It provides a quality-checked version of constructor/team information from the bronze layer,
ensuring consistency in team names and nationality data.

Storage:
- Input: /mnt/bronze_layer_gen2/constructor/
- Output: /mnt/silver_layer_gen2/constructor/
- Format: Delta Lake (non-partitioned)

Columns:
- constructorId: Unique identifier for each constructor
- constructorRef: Standardized constructor reference name
- name: Validated official team name
- nationality: Standardized nationality
- url: Verified Wikipedia URL

Audit Columns:
- input_file_name: Bronze file tracking
- date_part: Process date
- load_timestamp: Processing timestamp

Transformations:
- Standardizes constructor names across historical records
- Validates and normalizes nationality values
- Ensures consistent reference names for team identification
- Verifies URLs are properly formatted

Used By:
- gold.fact_results: Links race results to constructors
- gold.constructor_standings: Used for team championship analysis
- Various performance analysis views

Source: bronze.constructor
Dependencies: Requires successful load of bronze.constructor
"""

from pyspark.sql import SparkSession

# Create constructor table in silver layer
spark.sql("""
CREATE TABLE IF NOT EXISTS silver.constructor
(
constructorId integer
,constructorRef string
,name string
,nationality string
,url string
,input_file_name string
,date_part date
,load_timestamp timestamp
)
using DELTA
location '/mnt/silver_layer_gen2/constructor'
""")