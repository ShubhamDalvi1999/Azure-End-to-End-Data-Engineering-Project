# Databricks notebook source

"""
This script creates the bronze.drivers table which stores raw Formula 1 driver information.
The table contains personal and professional details about each driver in the championship.
Data is partitioned by date_part for efficient data loading and historical tracking.

Columns:
- driverId: Unique identifier for each driver
- driverRef: Driver reference name
- number: Driver's racing number
- code: Driver's short code (e.g., 'HAM' for Hamilton)
- forename/surname: Driver's full name
- dob: Date of birth
- nationality: Driver's nationality
- url: Wikipedia URL for the driver
"""

from pyspark.sql import SparkSession

# Create drivers table in bronze layer
spark.sql("""
CREATE TABLE IF NOT EXISTS bronze.drivers
(
driverId integer
,driverRef string
,number integer
,code string
,forename string
,surname string
,dob date
,nationality string
,url string
,input_file_name string
,date_part date
,load_timestamp timestamp
)
using DELTA
partitioned By(date_part)
location '/mnt/bronze_layer_gen2/drivers'
""")

# COMMAND ----------

# MAGIC %sql
# MAGIC select  distinct dob from delta.`/mnt/bronze_geekcoders_gen2/drivers`

# COMMAND ----------

# MAGIC %sql
# MAGIC drop table bronze.drivers

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE TABLE IF NOT EXISTS bronze.drivers
# MAGIC (
# MAGIC driverId integer
# MAGIC ,driverRef string
# MAGIC ,number string
# MAGIC ,code string
# MAGIC ,forename string
# MAGIC ,surname string
# MAGIC ,dob date
# MAGIC ,nationality string
# MAGIC ,url string
# MAGIC ,input_file_name string
# MAGIC ,date_part date
# MAGIC ,load_timestamp timestamp
# MAGIC )
# MAGIC using DELTA
# MAGIC partitioned By(date_part)
# MAGIC location '/mnt/bronze_geekcoders_gen2/drivers'