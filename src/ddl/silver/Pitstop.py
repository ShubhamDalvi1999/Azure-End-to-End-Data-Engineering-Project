# Databricks notebook source

"""
This script creates the silver.pitstop table which stores cleansed Formula 1 pit stop data.
It provides quality-checked pit stop timing information with validated durations and
standardized timestamps for race strategy analysis.

Columns:
- raceId/driverId: Validated references to race and driver
- stop: Verified pit stop sequence number
- lap: Validated lap number when stop occurred
- time: Standardized timestamp of pit stop
- duration: Validated pit stop duration
- milliseconds: Verified duration in milliseconds

Source: bronze.pitstop
Used in: Race strategy analysis and pit stop performance studies
Links to: silver.race, silver.drivers for comprehensive pit strategy analysis
"""

from pyspark.sql import SparkSession

# Create pitstop table in silver layer
spark.sql("""
CREATE TABLE IF NOT EXISTS silver.pitstop
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
location '/mnt/silver_layer_gen2/pitstop'
""")