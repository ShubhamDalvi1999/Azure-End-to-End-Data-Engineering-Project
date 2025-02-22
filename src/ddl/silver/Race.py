# Databricks notebook source

"""
This script creates the silver.race table which stores cleansed Formula 1 race event data.
It provides quality-checked race information with standardized dates and times,
ensuring consistency in race event details across the dataset.

Storage:
- Input: /mnt/bronze_layer_gen2/race/
- Output: /mnt/silver_layer_gen2/race/
- Format: Delta Lake (non-partitioned)

Columns:
- raceId: Validated unique identifier for each race
- year/round: Verified championship chronology
- circuitId: Validated circuit reference
- name: Standardized race name
- date/time: Normalized datetime values
- url: Verified Wikipedia URL

Audit Columns:
- input_file_name: Bronze file tracking
- date_part: Process date
- load_timestamp: Processing timestamp

Transformations:
- Validates race chronology and round numbers
- Standardizes race names across seasons
- Normalizes date and time formats
- Ensures valid circuit references
- Verifies URLs are properly formatted

Used By:
- gold.dim_race: Feeds into race dimension table
- gold.fact_results: Core race event reference
- gold.race_analysis: Race timing and scheduling analysis
- Various championship progression views

Source: bronze.race
Dependencies: 
- Requires successful load of bronze.race
- References silver.circuits for circuit validation
"""

from pyspark.sql import SparkSession

# Create race table in silver layer
spark.sql("""
CREATE TABLE IF NOT EXISTS silver.race
(
raceId integer
,year integer
,round integer
,circuitId integer
,name string
,date date
,time string
,url string
,input_file_name string
,date_part date
,load_timestamp timestamp
)
using DELTA
location '/mnt/silver_layer_gen2/race'
""")