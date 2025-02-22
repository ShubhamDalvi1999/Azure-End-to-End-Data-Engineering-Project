# Databricks notebook source

"""
This script creates the silver.results table which stores cleansed Formula 1 race results.
It provides quality-checked and validated race outcome data, ensuring consistency in
timing data, points calculations, and position information.

Storage:
- Input: /mnt/bronze_layer_gen2/results/
- Output: /mnt/silver_layer_gen2/results/
- Format: Delta Lake (non-partitioned)

Columns:
- resultId: Validated unique identifier for each result
- raceId/driverId/constructorId: Verified entity references
- grid/position/positionOrder: Validated race position data
- points: Verified championship points
- laps/time/milliseconds: Standardized race duration metrics
- fastestLap/fastestLapTime/fastestLapSpeed: Validated performance metrics
- statusId: Verified race completion status

Audit Columns:
- input_file_name: Bronze file tracking
- date_part: Process date
- load_timestamp: Processing timestamp

Transformations:
- Validates all entity references (race, driver, constructor)
- Ensures consistent position and grid data
- Standardizes time formats and validates durations
- Verifies points calculations based on F1 rules
- Normalizes fastest lap data
- Validates race completion status codes

Used By:
- gold.fact_results: Primary source for race outcomes
- gold.driver_standings: Driver championship calculations
- gold.constructor_standings: Constructor championship points
- Various performance analysis views

Source: bronze.results
Dependencies:
- Requires successful load of bronze.results
- References silver.race for race validation
- References silver.drivers for driver validation
- References silver.constructor for constructor validation
"""

from pyspark.sql import SparkSession

# Create results table in silver layer
spark.sql("""
CREATE TABLE IF NOT EXISTS silver.results
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
location '/mnt/silver_layer_gen2/results'
""")