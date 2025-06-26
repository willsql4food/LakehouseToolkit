# Databricks notebook source
# MAGIC %md
# MAGIC # Setup Context
# MAGIC Specify storage account which hosts data and configure authorization

# COMMAND ----------

dbutils.widgets.text("folder", "", "Path fragment")
# ========================================================================================================================================
# Storage account and protocol for connecting
# ========================================================================================================================================
protocol = "abfss://"                           # Azure Blob File System
provider_suffix = "dfs.core.windows.net"        # Azure storage v2
container = "datalake"
store = "staab09289802"
rootpath = "bronze/"
datapath = f"{protocol}{container}@{store}.{provider_suffix}/{rootpath}"

# ========================================================================================================================================
# Set Spark configuration
# In this instance, connect to the storage account using SAS (shared access signature) and provide said token from databricks secret
# ========================================================================================================================================
spark.conf.set(f"fs.azure.account.auth.type.{store}.{provider_suffix}", "SAS")
spark.conf.set(f"fs.azure.sas.token.provider.type.{store}.{provider_suffix}", "org.apache.hadoop.fs.azurebfs.sas.FixedSASTokenProvider")
spark.conf.set(f"fs.azure.sas.fixed.token.{store}.{provider_suffix}", dbutils.secrets.get(scope="log_analytics", key=f"sas_key_{store}"))

# COMMAND ----------

# MAGIC %md
# MAGIC ## Build file path based on resource to query
# MAGIC In this case, the resource is Azure Data Factory for multiple instances
# MAGIC Use wildcards for:
# MAGIC * SubscriptionId
# MAGIC * Resource Group name
# MAGIC * Data Factory name
# MAGIC
# MAGIC #### Sample Directory Path
# MAGIC * Absolute:  
# MAGIC     `insights-logs-activityruns@staab09289802.dfs.core.windows.net/resourceId=/SUBSCRIPTIONS/4C87027A-D0A7-4F47-B9C4-31447460FBEF/RESOURCEGROUPS/`
# MAGIC     `RG-SBX-AB092898/PROVIDERS/MICROSOFT.DATAFACTORY/FACTORIES/DF-SBX-AB092898/y=2023/m=12/d=06/h=11/m=00`
# MAGIC * Wildcard:  
# MAGIC     `insights-logs-activityruns@{store}.dfs.core.windows.net/resourceId=/SUBSCRIPTIONS/*/RESOURCEGROUPS/`  
# MAGIC     `*/PROVIDERS/MICROSOFT.DATAFACTORY/FACTORIES/*/*/*/*/*/*`

# COMMAND ----------

# Setup the file path - just looking at events for a given month
folder = dbutils.widgets.get("folder")
filepath = f"{datapath}/{folder}"

# COMMAND ----------

dbutils.fs.ls(filepath)

# COMMAND ----------

# MAGIC %sql
# MAGIC drop table if exists bsTest;

# COMMAND ----------

spark.sql(f"""
create table if not exists bsTest (string1 string, string2 string)
using CSV
options 
(
    header = "true"
)
location "{filepath}/data.csv"
""")
# --create or replace view bsTest as select string1, string2 from csv.`abfss://datalake@staab09289802.dfs.core.windows.net/bronze/Test/data.csv`

# COMMAND ----------

# MAGIC %sql
# MAGIC refresh table bsTest;
# MAGIC
# MAGIC select * from bsTest
