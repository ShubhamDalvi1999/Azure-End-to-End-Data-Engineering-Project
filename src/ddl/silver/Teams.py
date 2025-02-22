# Databricks notebook source

"""
This script creates the silver.teams table which stores cleansed Formula 1 team information.
It provides quality-checked and standardized team data from the bronze layer, including
validated historical statistics and current team details.

Storage:
- Input: /mnt/bronze_layer_gen2/teams/
- Output: /mnt/silver_layer_gen2/teams/
- Format: Delta Lake (non-partitioned)

Columns:
- id: Validated unique identifier for each team
- name: Standardized team name
- logo/base: Verified team identity and location
- first_team_entry/world_championships: Validated historical data
- position/number/pole_positions/fastest_laps: Cleansed achievement metrics
- president/director/technical_manager: Verified management information
- chassis/engine/tyres: Standardized technical specifications

Audit Columns:
- input_file_name: Bronze file tracking
- date_part: Process date
- load_timestamp: Processing timestamp

Transformations:
- Standardizes team names across historical records
- Validates historical statistics and achievements
- Ensures consistent location and base information
- Normalizes management role titles
- Standardizes technical specifications format
- Verifies logo URLs are properly formatted

Used By:
- gold.dim_teams: Feeds into team dimension table
- gold.fact_results: Links race results to teams
- gold.constructor_standings: Team championship analysis
- Various technical and management analysis views

Source: bronze.teams
Dependencies:
- Requires successful load of bronze.teams
- May reference silver.constructor for historical validation
"""

from pyspark.sql import SparkSession

# Create teams table in silver layer
spark.sql("""
CREATE TABLE IF NOT EXISTS silver.teams
(
id integer
,name string
,logo string
,base string
,first_team_entry integer
,world_championships integer
,position integer
,number integer
,pole_positions integer
,fastest_laps integer
,president string
,director string
,technical_manager string
,chassis string
,engine string
,tyres string
,input_file_name string
,date_part date
,load_timestamp timestamp
)
using DELTA
location '/mnt/silver_layer_gen2/teams'
""")