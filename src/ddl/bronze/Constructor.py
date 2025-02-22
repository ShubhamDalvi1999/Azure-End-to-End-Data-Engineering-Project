# Databricks notebook source
from pyspark.sql import SparkSession

# Create constructor table in bronze layer
spark.sql("""
CREATE TABLE IF NOT EXISTS bronze.constructor
(
constructorId integer
,constructorRef string
,name string
,nationality string
,url string
,input_file_name string
,date_part date
,load_timestamp timestamp
)
using DELTA
partitioned By(date_part)
location '/mnt/bronze_layer_gen2/constructor'
""")