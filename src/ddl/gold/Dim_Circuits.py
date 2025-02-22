# Databricks notebook source

"""
This script creates the gold.dim_circuits dimension table which provides a clean,
denormalized view of Formula 1 circuit information for reporting purposes.
It contains essential circuit attributes used in race analysis.

Columns:
- circuitId: Primary key, unique identifier for each circuit
- circuitRef: Circuit reference name for queries
- name: Official circuit name
- location: City/location of the circuit
- country: Country where circuit is located
- alt: Altitude of the circuit

Used by:
- fact_results: For race venue analysis and geographical insights
"""

from pyspark.sql import SparkSession

# Create dim_circuits table in gold layer
spark.sql("""
CREATE TABLE IF NOT EXISTS gold.dim_circuits
(
circuitId integer
,circuitRef string
,name string
,location string
,country string
,alt string
)
using DELTA
location '/mnt/gold_layer_gen2/dim_circuits'
""")