# Databricks notebook source

"""
This script creates the bronze.qualifying table which stores raw Formula 1 qualifying session data.
The table captures qualifying performance data including timing for Q1, Q2, and Q3 sessions.
Data is partitioned by date_part for efficient data loading and historical tracking.

Storage:
- Input: /mnt/source_layer_gen2/qualifying/*.csv
- Output: /mnt/bronze_layer_gen2/qualifying/
- Format: Delta Lake with date_part partitioning

Columns:
- qualifyId: Unique identifier for qualifying result
- raceId/driverId/constructorId: References to race, driver, and constructor
- number: Driver's car number
- position: Final qualifying position
- q1/q2/q3: Timing for each qualifying session

Audit Columns:
- input_file_name: Source file tracking
- date_part: Partition date
- load_timestamp: Processing timestamp
"""

from pyspark.sql import SparkSession

# Create qualifying table in bronze layer
spark.sql("""
CREATE TABLE IF NOT EXISTS bronze.qualifying
(
qualifyId integer
,raceId integer
,driverId integer
,constructorId integer
,number integer
,position integer
,q1 string
,q2 string
,q3 string
,input_file_name string
,date_part date
,load_timestamp timestamp
)
using DELTA
partitioned By(date_part)
location '/mnt/bronze_layer_gen2/qualifying'
""")