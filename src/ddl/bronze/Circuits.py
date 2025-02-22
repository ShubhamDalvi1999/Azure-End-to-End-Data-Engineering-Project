# Databricks notebook source

"""
This script creates the bronze.circuits table which stores raw Formula 1 circuit information.
The table includes circuit details such as circuit ID, name, location, country, and geographical coordinates.
Data is partitioned by date_part for efficient data loading and historical tracking.

Storage:
- Input: /mnt/source_layer_gen2/circuits/*.csv
- Output: /mnt/bronze_layer_gen2/circuits/
- Format: Delta Lake with date_part partitioning

Columns:
- circuitId: Unique identifier for each circuit
- circuitRef: Circuit reference name
- name: Official circuit name
- location: City/location of the circuit
- country: Country where circuit is located
- lat/lng: Geographical coordinates
- alt: Altitude of the circuit
- url: Wikipedia URL for the circuit

Audit Columns:
- input_file_name: Source file tracking
- date_part: Partition date
- load_timestamp: Processing timestamp
"""

from pyspark.sql import SparkSession

# Create circuits table in bronze layer
spark.sql("""
CREATE TABLE IF NOT EXISTS bronze.circuits
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
partitioned By(date_part)
location '/mnt/bronze_layer_gen2/circuits'
""")