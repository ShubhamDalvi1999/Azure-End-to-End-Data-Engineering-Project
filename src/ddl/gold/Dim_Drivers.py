# Databricks notebook source

"""
This script creates the gold.dim_drivers dimension table which provides a clean,
denormalized view of driver information for reporting purposes. It includes
essential driver attributes and a computed full_name field for easy reporting.

Storage:
- Input Tables: 
  * /mnt/silver_layer_gen2/drivers/
- Output: /mnt/gold_layer_gen2/dim_driver/
- Format: Delta Lake (optimized for analytics)

Columns:
- driverId: Primary key, unique identifier for each driver
- driverRef: Driver reference name for queries
- number/code: Driver's racing number and code
- forename/surname/full_name: Driver's name components
- dob: Date of birth
- nationality: Driver's nationality

Used by:
- fact_results: For driver performance analysis

Dependencies:
- Requires successful processing of silver.drivers
- Should be processed before fact_results table
"""

from pyspark.sql import SparkSession

# Create dim_drivers table in gold layer
spark.sql("""
CREATE TABLE IF NOT EXISTS gold.dim_drivers
(
driverId integer
,driverRef string
,number integer
,code string
,forename string
,surname string
,dob date
,nationality string
,full_name string
)
using DELTA
location '/mnt/gold_layer_gen2/dim_driver'
""")