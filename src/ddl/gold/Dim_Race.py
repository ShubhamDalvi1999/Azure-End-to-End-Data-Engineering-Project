# Databricks notebook source

"""
This script creates the gold.dim_race dimension table which provides a clean,
denormalized view of Formula 1 race event information for reporting purposes.
It contains essential race attributes used in performance analysis.

Storage:
- Input Tables: 
  * /mnt/silver_layer_gen2/race/
- Output: /mnt/gold_layer_gen2/dim_race/
- Format: Delta Lake (optimized for analytics)

Columns:
- raceId: Primary key, unique identifier for each race
- year: Year of the race
- round: Round number in the championship
- circuitId: Reference to circuit dimension
- name: Official race name
- date: Race date
- time: Race start time

Used by:
- fact_results: For temporal analysis and race event context

Dependencies:
- Requires successful processing of silver.race
- Should be processed before fact_results table
"""

from pyspark.sql import SparkSession

# Create dim_race table in gold layer
spark.sql("""
CREATE TABLE IF NOT EXISTS gold.dim_race
(
raceId integer
,year integer
,round integer
,circuitId integer
,name string
,date date
,time string
)
using DELTA
location '/mnt/gold_layer_gen2/dim_race'
""")