# Databricks notebook source

"""
This script creates the silver.drivers table which stores cleansed Formula 1 driver information.
It provides quality-checked and standardized driver data, ensuring consistency in
personal information and racing identifiers.

Storage:
- Input: /mnt/bronze_layer_gen2/drivers/
- Output: /mnt/silver_layer_gen2/drivers/
- Format: Delta Lake (non-partitioned)

Columns:
- driverId: Validated unique identifier for each driver
- driverRef: Standardized driver reference code
- number/code: Verified racing identifiers
- forename/surname: Standardized name components
- dob: Validated and standardized date of birth
- nationality: Standardized nationality
- url: Verified Wikipedia URL

Audit Columns:
- input_file_name: Bronze file tracking
- date_part: Process date
- load_timestamp: Processing timestamp

Transformations:
- Standardizes driver names and removes inconsistencies
- Validates and normalizes date of birth format
- Ensures consistent nationality values
- Verifies driver numbers against historical records
- Standardizes driver reference codes
- Validates URLs are properly formatted

Used By:
- gold.dim_drivers: Feeds into driver dimension table
- gold.fact_results: Links race results to drivers
- gold.driver_standings: Driver championship calculations
- Various driver performance analysis views

Source: bronze.drivers
Dependencies: Requires successful load of bronze.drivers
"""

from pyspark.sql import SparkSession

# Create drivers table in silver layer
spark.sql("""
CREATE TABLE IF NOT EXISTS silver.drivers
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
location '/mnt/silver_layer_gen2/drivers'
""")