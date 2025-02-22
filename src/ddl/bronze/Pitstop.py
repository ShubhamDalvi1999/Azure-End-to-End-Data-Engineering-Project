# Databricks notebook source

"""
This script creates the bronze.pitstop table which stores raw Formula 1 pit stop data.
The table captures timing and duration information for all pit stops during races.
Data is partitioned by date_part for efficient data loading and historical tracking.

Columns:
- raceId/driverId: References to race and driver
- stop: Pit stop number for the driver in this race
- lap: Lap number when pit stop occurred
- time: Timestamp of the pit stop
- duration: Duration of the pit stop
- milliseconds: Pit stop duration in milliseconds
"""

from pyspark.sql import SparkSession

# Create pitstop table in bronze layer
spark.sql("""
CREATE TABLE IF NOT EXISTS bronze.pitstop
(
raceId integer
,driverId integer
,stop integer
,lap integer
,time timestamp
,duration string
,milliseconds integer
,input_file_name string
,date_part date
,load_timestamp timestamp
)
using DELTA
partitioned By(date_part)
location '/mnt/bronze_layer_gen2/pitstop'
""")