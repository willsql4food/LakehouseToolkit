# Databricks notebook source
# MAGIC %md
# MAGIC # Tools
# MAGIC ### Working with Time
# MAGIC Precise timestamps tend to come from Google Analytics as very large integers.  These are either:
# MAGIC * plain Unix timestamps (the number of seconds since 01 Jan, 1970)
# MAGIC   * example: __1,700,000,000__ = __14 Nov, 2023 at 10:13.20 PM__
# MAGIC * modified Unix timestamps to add sub-second precision.  In these cases, you may need to divide by 1000 or 1,000,000 to get a number in the 1.7B seconds range
# MAGIC
# MAGIC You can then format the number as a friendly date & time.  
# MAGIC Here's a sample:

# COMMAND ----------

from datetime import datetime
ts = 1.7e9 + 0.123456
print(datetime.utcfromtimestamp(ts).strftime('%Y-%m-%d %H:%M:%S.%f'))

# COMMAND ----------

# MAGIC %md
# MAGIC # Setup Connection
# MAGIC * Storage Account & Container
# MAGIC * Root path to Google Analytics data

# COMMAND ----------

# ====================================================================
# Storage account and protocol for connecting
# ====================================================================
protocol = "abfss://"
store = "sadevdatalakehouse"
container = "datalake"
rootpath = "bronze/GoogleAnalytics/fnd-cloud-project/analytics_250303278/"

# Set Spark configuration
# In this instance, connect to the storage account using SAS (shared access signature) and provide said token from databricks secret
spark.conf.set(f"fs.azure.account.auth.type.{store}.dfs.core.windows.net", "SAS")
spark.conf.set(f"fs.azure.sas.token.provider.type.{store}.dfs.core.windows.net", "org.apache.hadoop.fs.azurebfs.sas.FixedSASTokenProvider")
spark.conf.set(f"fs.azure.sas.fixed.token.{store}.dfs.core.windows.net", dbutils.secrets.get(scope="google-analytics", key=f"ga-sas-key-sadevdatalakehouse"))

datapath = f"{protocol}{container}@{store}.dfs.core.windows.net/{rootpath}"

# COMMAND ----------

# MAGIC %md
# MAGIC ## Simple view of top-level objects available

# COMMAND ----------

objects = dbutils.fs.ls(datapath)
for o in objects:
  print(f"{o.name}\n\t{o.path}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Simple counts of each object
# MAGIC __Note__ Wild cards at end of each path are for /Year/Month/Day/file(s)

# COMMAND ----------

pseudo_users = spark.read.format("parquet").load(f"{datapath}/pseudonymous_users/*/*/*/*")
users = spark.read.format("parquet").load(f"{datapath}/users/*/*/*/*")
events = spark.read.format("parquet").load(f"{datapath}/events/*/*/*/*")

print(f"pseudonymous_users: {pseudo_users.count():,} records")
print(f"users: {users.count():,} records")
print(f"events: {events.count():,} records")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Create Temporary Views
# MAGIC This will allow us to write SQL queries if desired (note __%sql__ magic command in subsequent cells)

# COMMAND ----------

pseudo_users.createOrReplaceTempView("vwPseudoUsers")
users.createOrReplaceTempView("vwUsers")
events.createOrReplaceTempView("vwEvents")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Exploding the JSON
# MAGIC Numerous columns have STRING values, but are actually JSON objects, arrays of objects, or even a mix of both.  
# MAGIC
# MAGIC Here's an example of extracting the contents of a complex column.
# MAGIC
# MAGIC 1. Use `schema_of_json` to find the structures of the JSON contained in the fields of interest.
# MAGIC 1. Use `from_json` and the schema to extract the fields from the JSON.  
# MAGIC     * Doing this in a common table expression helps separate the logic and support further queries.
# MAGIC 1. Consult the Google Big Query instance to see the original schema to determine the field names

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC select  schema_of_json(user_info) user_info_json_schema,
# MAGIC         schema_of_json(device) device_json_schema
# MAGIC from    vwPseudoUsers limit 1

# COMMAND ----------

# MAGIC %sql
# MAGIC with exp as (
# MAGIC         select  pu.pseudo_user_id, 
# MAGIC                 from_json(user_info, 'STRUCT<v: STRUCT<f: ARRAY<STRUCT<v: STRING>>>>').v.f[0].v last_active_timestamp_micros,
# MAGIC                 from_json(user_info, 'STRUCT<v: STRUCT<f: ARRAY<STRUCT<v: STRING>>>>').v.f[1].v user_first_touch_timestamp_micros,
# MAGIC                 from_json(user_info, 'STRUCT<v: STRUCT<f: ARRAY<STRUCT<v: STRING>>>>').v.f[2].v first_purchase_date,
# MAGIC                 from_json(device, 'STRUCT<v: STRUCT<f: ARRAY<STRUCT<v: STRING>>>>').v.f[0].v operating_system,
# MAGIC                 from_json(device, 'STRUCT<v: STRUCT<f: ARRAY<STRUCT<v: STRING>>>>').v.f[1].v category,
# MAGIC                 from_json(device, 'STRUCT<v: STRUCT<f: ARRAY<STRUCT<v: STRING>>>>').v.f[2].v mobile_brand_name,
# MAGIC                 from_json(device, 'STRUCT<v: STRUCT<f: ARRAY<STRUCT<v: STRING>>>>').v.f[3].v mobile_model_name,
# MAGIC                 from_json(device, 'STRUCT<v: STRUCT<f: ARRAY<STRUCT<v: STRING>>>>').v.f[4].v unified_screen_name
# MAGIC         from    vwPseudoUsers pu 
# MAGIC         limit 50
# MAGIC )
# MAGIC select  pseudo_user_id, 
# MAGIC         from_unixtime(last_active_timestamp_micros / 1000000.0) last_active_timestamp_micros, 
# MAGIC         from_unixtime(user_first_touch_timestamp_micros / 1000000.0) user_first_touch_timestamp_micros, 
# MAGIC         from_unixtime(first_purchase_date / 1000000.0) first_purchase_date,
# MAGIC         operating_system,
# MAGIC         category,
# MAGIC         mobile_brand_name,
# MAGIC         mobile_model_name,
# MAGIC         unified_screen_name
# MAGIC from    exp
# MAGIC         

# COMMAND ----------


