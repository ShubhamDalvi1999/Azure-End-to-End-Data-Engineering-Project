# Databricks notebook source

"""
This script creates the bronze.laptimes table which stores raw Formula 1 lap timing data.
The table captures detailed timing information for each lap completed by drivers during races.
Data is partitioned by date_part for efficient data loading and historical tracking.

Columns:
- raceId/driverId: References to race and driver
- lap: Lap number in the race
- position: Driver's position on this lap
- time: Lap time in string format
- milliseconds: Lap time in milliseconds for calculations
"""

from pyspark.sql import SparkSession

# Create laptimes table in bronze layer
spark.sql("""
CREATE TABLE IF NOT EXISTS bronze.laptimes
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
partitioned By(date_part)
location '/mnt/bronze_layer_gen2/laptimes'
""")