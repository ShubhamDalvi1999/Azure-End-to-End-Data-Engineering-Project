# Databricks notebook source
# MAGIC %run /DeltaLake/Utilities/Common_Functions

# COMMAND ----------

# Configure OAuth credentials
configs = {
    "fs.azure.account.auth.type": "OAuth",
    "fs.azure.account.oauth.provider.type": "org.apache.hadoop.fs.azurebfs.oauth2.ClientCredsTokenProvider",
    "fs.azure.account.oauth2.client.id": get_secret_from_scope(key = "client-id"),
    "fs.azure.account.oauth2.client.secret": get_secret_from_scope(key = "client-secret"),
    "fs.azure.account.oauth2.client.endpoint": get_secret_from_scope(key = "tenantid")
}

# COMMAND ----------

# Mount source data lake container
mountPoint = "/mnt/source_layer_gen2"
if not any(mount.mountPoint == mountPoint for mount in dbutils.fs.mounts()):
    dbutils.fs.mount(
        source = get_secret_from_scope(key = "source-layer-gen2"),
        mount_point = mountPoint,
        extra_configs = configs
    )

# COMMAND ----------

# Mount bronze data lake container
mountPoint = "/mnt/bronze_layer_gen2"
if not any(mount.mountPoint == mountPoint for mount in dbutils.fs.mounts()):
    dbutils.fs.mount(
        source = get_secret_from_scope(key = "bronze-layer-gen2"),
        mount_point = mountPoint,
        extra_configs = configs
    )

# COMMAND ----------

# Mount silver data lake container
mountPoint = "/mnt/silver_layer_gen2"
if not any(mount.mountPoint == mountPoint for mount in dbutils.fs.mounts()):
    dbutils.fs.mount(
        source = get_secret_from_scope(key = "silver-layer-gen2"),
        mount_point = mountPoint,
        extra_configs = configs
    )

# COMMAND ----------

# Mount gold data lake container
mountPoint = "/mnt/gold_layer_gen2"
if not any(mount.mountPoint == mountPoint for mount in dbutils.fs.mounts()):
    dbutils.fs.mount(
        source = get_secret_from_scope(key = "gold-layer-gen2"),
        mount_point = mountPoint,
        extra_configs = configs
    )

# COMMAND ----------

# Print all mount points to verify setup
display(dbutils.fs.mounts())