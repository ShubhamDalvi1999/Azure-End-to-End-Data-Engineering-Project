# Databricks notebook source
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