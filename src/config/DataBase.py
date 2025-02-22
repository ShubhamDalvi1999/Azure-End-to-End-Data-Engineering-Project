# Databricks notebook source
from pyspark.sql import SparkSession

# Create databases for each layer of the medallion architecture
# Bronze: Raw data landing
spark.sql("CREATE DATABASE IF NOT EXISTS bronze")

# Silver: Cleansed data
spark.sql("CREATE DATABASE IF NOT EXISTS silver")

# Gold: Business aggregated data
spark.sql("CREATE DATABASE IF NOT EXISTS gold")

# Gold views: Business reporting views
spark.sql("CREATE DATABASE IF NOT EXISTS gold_vw")