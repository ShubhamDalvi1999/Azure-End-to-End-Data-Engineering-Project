# Databricks notebook source

"""
This script creates the bronze.race table which stores raw Formula 1 race event information.
The table captures details about each race including basic event information and scheduling.
Data is partitioned by date_part for efficient data loading and historical tracking.

Storage:
- Input: /mnt/source_layer_gen2/race/*.csv
- Output: /mnt/bronze_layer_gen2/race/
- Format: Delta Lake with date_part partitioning

Columns:
- raceId: Unique identifier for each race
- year: Year of the race
- round: Round number in the championship
- circuitId: Reference to the circuit where race was held
- name: Official race name
- date: Race date
- time: Race start time
- url: Wikipedia URL for the race

Audit Columns:
- input_file_name: Source file tracking
- date_part: Partition date
- load_timestamp: Processing timestamp
"""

from pyspark.sql import SparkSession

# Create race table in bronze layer
spark.sql("""
CREATE TABLE IF NOT EXISTS bronze.race
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
partitioned By(date_part)
location '/mnt/bronze_layer_gen2/race'
""")