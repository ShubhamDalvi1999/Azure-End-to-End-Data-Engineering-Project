# Databricks notebook source

"""
This script creates the silver.circuits table which stores cleansed Formula 1 circuit data.
It provides quality-checked version of circuit information from the bronze layer,
with standardized formats and validated geographical coordinates.

Storage:
- Input: /mnt/bronze_layer_gen2/circuits/
- Output: /mnt/silver_layer_gen2/circuits/
- Format: Delta Lake (non-partitioned)

Columns:
- circuitId: Unique identifier for each circuit
- circuitRef: Standardized circuit reference name
- name: Validated official circuit name
- location: Cleansed city/location name
- country: Standardized country name
- lat/lng: Validated geographical coordinates
- alt: Standardized altitude value
- url: Verified Wikipedia URL

Audit Columns:
- input_file_name: Bronze file tracking
- date_part: Process date
- load_timestamp: Processing timestamp

Transformations:
- Validates and standardizes geographical coordinates
- Ensures consistent country names and location formats
- Verifies altitude values are in correct format
- Validates URLs are properly formatted

Used By:
- gold.dim_circuits: Feeds into the circuit dimension table
- gold.fact_results: Used for race venue analysis

Source: bronze.circuits
Dependencies: Requires successful load of bronze.circuits
"""

from pyspark.sql import SparkSession

# Create circuits table in silver layer
spark.sql("""
CREATE TABLE IF NOT EXISTS silver.circuits
(
circuitId integer
,circuitRef string
,name string
,location string
,country string
,lat double
,lng double
,alt string
,url string
,input_file_name string
,date_part date
,load_timestamp timestamp
)
using DELTA
location '/mnt/silver_layer_gen2/circuits'
""")