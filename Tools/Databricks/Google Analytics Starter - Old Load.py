# Databricks notebook source
# MAGIC %md
# MAGIC # DEPRECATED
# MAGIC
# MAGIC This notebook points to the originally loaded GBQ data  
# MAGIC `rootpath = "bronze/GoogleAnalytics_ComplexDatatypes/fnd-cloud-project/analytics_250303278/"`

# COMMAND ----------

# MAGIC %md
# MAGIC # Setup Connection
# MAGIC * Storage Account & Container
# MAGIC * Root path to Google Analytics data
# MAGIC * Build dataframe for each of the overall datasets - events, users & pseudonymous_users
# MAGIC   * __Note__ Wild cards at end of each path are for /Year/Month/Day/file(s)

# COMMAND ----------

yyyy = "2024"   # must be four digits or '*' for all
mm = "02"       # must be two digits or '*' for all
dd = "24"       # must be two digits or '*' for all

# COMMAND ----------

# ====================================================================
# Storage account and protocol for connecting
# ====================================================================
protocol = "abfss://"
store = "sadevdatalakehouse"
container = "datalake"
rootpath = "bronze/GoogleAnalytics_ComplexDatatypes/fnd-cloud-project/analytics_250303278/"

# Set Spark configuration
# In this instance, connect to the storage account using SAS (shared access signature) and provide said token from databricks secret
spark.conf.set(f"fs.azure.account.auth.type.{store}.dfs.core.windows.net", "SAS")
spark.conf.set(f"fs.azure.sas.token.provider.type.{store}.dfs.core.windows.net", "org.apache.hadoop.fs.azurebfs.sas.FixedSASTokenProvider")
spark.conf.set(f"fs.azure.sas.fixed.token.{store}.dfs.core.windows.net", dbutils.secrets.get(scope="google-analytics", key=f"ga-sas-key-sadevdatalakehouse"))

datapath = f"{protocol}{container}@{store}.dfs.core.windows.net/{rootpath}"

pseudo_users = spark.read.format("parquet").load(f"{datapath}/pseudonymous_users/{yyyy}/{mm}/{dd}/*")
users = spark.read.format("parquet").load(f"{datapath}/users/{yyyy}/{mm}/{dd}/*")
events = spark.read.format("parquet").load(f"{datapath}/events/{yyyy}/{mm}/{dd}/*")

pseudo_users.createOrReplaceTempView("vwPseudoUsers")
users.createOrReplaceTempView("vwUsers")
events.createOrReplaceTempView("vwEvents")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Now we're ready to use SQL
# MAGIC Just make sure each cell begins with __%sql__

# COMMAND ----------

# MAGIC %sql 
# MAGIC           select  'vwPseudoUsers' obj, count(*) numRows from    vwPseudoUsers pu 
# MAGIC union all select  'vwUsers' obj, count(*) numRows from    vwUsers pu 
# MAGIC union all select  'vwEvents' obj, count(*) numRows from    vwEvents pu 
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC select  *
# MAGIC from    vwEvents
# MAGIC limit   10

# COMMAND ----------

# MAGIC %sql
# MAGIC select  *
# MAGIC from    vwPseudoUsers
# MAGIC limit   10

# COMMAND ----------

# MAGIC %sql
# MAGIC select  *
# MAGIC from    vwUsers
# MAGIC limit   10
