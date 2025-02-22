# Databricks notebook source

"""
This script creates the bronze.teams table which stores raw Formula 1 team information.
The table captures comprehensive details about Formula 1 teams including historical achievements
and current team structure. Data is partitioned by date_part for efficient data loading.

Columns:
- id: Unique identifier for each team
- name: Official team name
- logo: Team logo URL
- base: Team headquarters location
- first_team_entry: First F1 championship entry year
- world_championships: Number of constructor championships
- position/number: Best race finish details
- pole_positions/fastest_laps: Achievement statistics
- president/director/technical_manager: Team management
- chassis/engine/tyres: Technical specifications
"""

from pyspark.sql import SparkSession

# Create teams table in bronze layer
spark.sql("""
CREATE TABLE IF NOT EXISTS bronze.teams
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
partitioned By(date_part)
location '/mnt/bronze_layer_gen2/teams'
""")