# Databricks notebook source
# MAGIC %run /DeltaLake/Utilities/Common_Functions

# COMMAND ----------

dbutils.widgets.text("tablename","")
tablename=dbutils.widgets.get("tablename")

# COMMAND ----------

from pyspark.sql.types import *

# COMMAND ----------

df=spark.read.table(f'bronze.{tablename}')
schema=df.dtypes[0:-3]

# COMMAND ----------

path=get_latest_file_info(f'/mnt/source_layer_gen2/{tablename}')
df_input=spark.read.format('csv').option("header",True).option("inferschema",True).load(path[1]).cache()
validate_schema_and_get_partitions(df_input,schema)
df=add_audit_columns(df_input,path[0].date(),path[1].split('/')[-1])
df.write.partitionBy("date_part").mode('overwrite').option("replaceWhere","date_part=='{0}'".format(path[0].date())).save(f'/mnt/bronze_layer_gen2/{tablename}/')
df_input.unpersist()