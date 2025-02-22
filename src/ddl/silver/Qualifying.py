# Databricks notebook source

"""
This script creates the silver.qualifying table which stores cleansed Formula 1 qualifying data.
It provides quality-checked qualifying session information with validated timing data
for all three qualifying sessions (Q1, Q2, Q3).

Columns:
- qualifyId: Validated unique identifier
- raceId/driverId/constructorId: Verified references
- number: Validated car number
- position: Verified qualifying position
- q1/q2/q3: Standardized session timing data

Source: bronze.qualifying
Used in: Qualifying performance analysis and grid position studies
"""

from pyspark.sql import SparkSession

# Create qualifying table in silver layer
spark.sql("""
CREATE TABLE IF NOT EXISTS silver.qualifying
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
location '/mnt/silver_layer_gen2/qualifying'
""")