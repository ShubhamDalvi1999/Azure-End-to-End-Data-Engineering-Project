# Databricks notebook source

"""
This script creates the bronze.results table which stores raw Formula 1 race results.
The table captures detailed performance data for each driver in every race, including
finishing position, points earned, and lap times. Data is partitioned by date_part
for efficient data loading and historical tracking.

Columns:
- resultId: Unique identifier for each race result
- raceId/driverId/constructorId: References to race, driver, and constructor
- position/positionOrder: Final race position
- points: Championship points earned
- laps: Number of laps completed
- time/milliseconds: Race completion time
- fastestLap/fastestLapTime/fastestLapSpeed: Best lap performance
- statusId: Race completion status
"""

from pyspark.sql import SparkSession

# Create results table in bronze layer
spark.sql("""
CREATE TABLE IF NOT EXISTS bronze.results
(
resultId integer
,raceId integer
,driverId integer
,constructorId integer
,number integer
,grid integer
,position integer
,positionText string
,positionOrder integer
,points double
,laps integer
,time string
,milliseconds integer
,fastestLap integer
,rank integer
,fastestLapTime string
,fastestLapSpeed double
,statusId integer
,input_file_name string
,date_part date
,load_timestamp timestamp
)
using DELTA
partitioned By(date_part)
location '/mnt/bronze_layer_gen2/results'
""") 