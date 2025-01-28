# Databricks notebook source
# Set configuration
# In this instance, connect to the storage account using SAS (shared access signature) and provide said token from databricks secret
spark.conf.set("fs.azure.account.auth.type.staab09289802.dfs.core.windows.net", "SAS")
spark.conf.set("fs.azure.sas.token.provider.type.staab09289802.dfs.core.windows.net", "org.apache.hadoop.fs.azurebfs.sas.FixedSASTokenProvider")
spark.conf.set("fs.azure.sas.fixed.token.staab09289802.dfs.core.windows.net", dbutils.secrets.get(scope="log_analytics", key="sas_key_staab09289802"))


# COMMAND ----------

# Setup the file path - Insights logs, dig all the way down to a single data factory
filepath = 'insights-logs-activityruns@staab09289802.dfs.core.windows.net/resourceId=/SUBSCRIPTIONS/4C87027A-D0A7-4F47-B9C4-31447460FBEF/RESOURCEGROUPS/RG-SBX-AB092898/'
filepath += 'PROVIDERS/MICROSOFT.DATAFACTORY/FACTORIES/DF-SBX-AB092898/' 

# Test that this directory exists
dbutils.fs.ls(f"abfss://{filepath}")

# COMMAND ----------

# ====================================================================
# Read all the JSON files on the file path into a temporary view
# Note: the wildcards are for year/month/day/hour/minute
# ====================================================================
df = spark.read.format("json").load(f"abfss://{filepath}*/*/*/*/*")
df.createOrReplaceTempView('adflog')

# ====================================================================
# Use explode to open up billing information array for each record
#   and data frame operations to get columns of interest
# ====================================================================
from pyspark.sql.functions import explode
o = spark.sql("select ActivityName, status, properties.Output.billingReference.billableDuration as Bill, properties.Output.executionDuration as ExecTime, properties.Output.recordsAffected, "
              + "properties.Output.dataRead, properties.Output.dataWritten, properties.Output.rowsRead, properties.Output.rowsCopied "
              + "from adflog "
              + "where properties.Output is not null")
o2 = o.select("ActivityName", "status", explode("Bill").alias("Bill"), "ExecTime", "recordsAffected", "dataRead", "dataWritten", "rowsRead", "rowsCopied")
o2.createOrReplaceTempView('billing')

# ====================================================================
# Build and display the final summarized data
# ====================================================================
out = spark.sql("select ActivityName, status, count(*) NumExecutions, sum(Bill.duration) as Duration, Bill.unit, sum(ExecTime) as ExecTime, sum(recordsAffected) as RecordsAffected, "
                + "sum(dataRead) dataRead, sum(dataWritten) dataWritten, sum(rowsRead) rowsRead, sum(rowsCopied) rowsCopied "
                + "from billing group by ActivityName, status, Bill.unit")
out.show()

# COMMAND ----------



# COMMAND ----------

# MAGIC %sql 
# MAGIC
# MAGIC -- ====================================================================
# MAGIC -- Alternately, use SQL to query the original temporary view 
# MAGIC --  and perform the explode in the query itself
# MAGIC -- ====================================================================
# MAGIC
# MAGIC select    ActivityName, status, b.unit,
# MAGIC           sum(b.duration) as Duration, 
# MAGIC           sum(properties.Output.executionDuration) as ExecutionCount, 
# MAGIC           sum(properties.Output.recordsAffected) as RowsAffected, 
# MAGIC           sum(properties.Output.dataRead) as BytesRead, 
# MAGIC           sum(properties.Output.dataWritten) as BytesWritten, 
# MAGIC           sum(properties.Output.rowsRead) as RowsRead, 
# MAGIC           sum(properties.Output.rowsCopied) as RowsWritten
# MAGIC from      adflog
# MAGIC join      lateral explode(properties.Output.billingReference.billableDuration) as Bill(b)
# MAGIC where     properties.Output is not null
# MAGIC group by  ActivityName, status, b.unit
# MAGIC

# COMMAND ----------

_sqldf.write.option("compression", "snappy").mode("overwrite").parquet(f"abfss://{filepath}/results.pq")


# COMMAND ----------

dbutils.fs.ls(f"abfss://{filepath}/results.pq/")

# COMMAND ----------

pdf = spark.read.parquet(f"abfss://{filepath}/results.pq/*")
pdf.show()

# COMMAND ----------


