# Databricks notebook source

"""
This script creates the silver.laptimes table which stores cleansed Formula 1 lap timing data.
It provides quality-checked lap time information with validated timestamps and durations.
The data is crucial for race pace analysis and performance comparisons.

Columns:
- raceId/driverId: Validated references to race and driver
- lap: Verified lap number
- position: Validated track position
- time: Standardized lap time format
- milliseconds: Validated milliseconds for accurate calculations

Source: bronze.laptimes
Used in: Race pace analysis and performance metrics
"""

from pyspark.sql import SparkSession

# Create laptimes table in silver layer
spark.sql("""
CREATE TABLE IF NOT EXISTS silver.laptimes
(
raceId integer
,driverId integer
,lap integer
,position integer
,time string
,milliseconds integer
,input_file_name string
,date_part date
,load_timestamp timestamp
)
using DELTA
location '/mnt/silver_layer_gen2/laptimes'
""")