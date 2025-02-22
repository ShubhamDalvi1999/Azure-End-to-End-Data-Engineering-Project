# Databricks notebook source

"""
This script creates the gold.fact_results table which serves as the main fact table
for race results analysis. It provides an aggregated view of driver performance across
races, including total points and wins. This table is optimized for analytical queries
and reporting.

Storage:
- Input Tables: 
  * /mnt/silver_layer_gen2/results/
  * /mnt/silver_layer_gen2/race/
  * /mnt/silver_layer_gen2/drivers/
- Output: /mnt/gold_layer_gen2/fact_results/
- Format: Delta Lake (optimized for analytics)

Columns:
- raceId/driverId/circuitId: Dimensional references
- date: Race date
- total_points: Total points earned in the race
- total_wins: Number of wins (1 if position=1, 0 otherwise)

Related Dimensions:
- dim_race: Race details
- dim_drivers: Driver information
- dim_circuits: Circuit information

Dependencies:
- Requires successful processing of silver.results, silver.race, silver.drivers
- Should be processed after all dimension tables are updated
"""

from pyspark.sql import SparkSession

# Create fact_results table in gold layer
spark.sql("""
CREATE TABLE IF NOT EXISTS gold.fact_results
(
raceId integer
,driverId integer
,circuitId integer
,date date
,total_points double
,total_wins long
)
using DELTA
location '/mnt/gold_layer_gen2/fact_results'
""")