# Databricks notebook source
# MAGIC %md
# MAGIC # Setup Context
# MAGIC Specify storage account which hosts data and configure authorization

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
spark.conf.set(f"fs.azure.sas.fixed.token.{store}.dfs.core.windows.net", dbutils.secrets.get(scope="google-analytics", key=f"sas_key_{store}"))


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

# Setup the file path - the folder that holds events, users, pseudonymous_users
filepath = f"{protocol}{container}@{store}.dfs.core.windows.net/{rootpath}"
dbutils.fs.ls(f"{filepath}")

# COMMAND ----------

# ====================================================================
# Function to crawl a directory tree and add any files found to an array
# ====================================================================
def dir_crawl(fso, subdir = "\t", file_array = []):
    tab = "\t"
    for obj in dbutils.fs.ls(fso.path):
        if(obj.size > 0):
#            print(f"{subdir}{obj.name} - {obj.size} bytes")
            obj.short_path = f"{subdir}{obj.name}"
            files.append(obj)
        else:
            dir_crawl(obj, subdir + obj.name)

# ====================================================================
# Function to compare input object to those in an array
#   If the object is found in the array, return True
#   If the object is unique, add to the array and return False
# ====================================================================
def unique_list(obj, objects = []):
    #  Look for obj in array; if found, return True
    for o in objects:
        if(obj == o):
            return True

    # Array is empty or no match found - add obj and return False
    objects.append(obj)
    return False


# COMMAND ----------

# ====================================================================
# Read all the JSON files on the file path into a temporary view
# Note: the wildcards are for year/month/day/hour/minute
# ====================================================================
dirs = dbutils.fs.ls(f"{filepath}")

files = []
for d in dirs:
    print(f"Parent directory: {d.path}")
    files.append(dir_crawl(d, " ", files))
        
#  Find total file size across all directories
total_size = 0
for f in files:
    if hasattr(f, 'size'):
        total_size += f.size
        
print("Total file size: {:,} bytes".format(total_size))        

# COMMAND ----------

# Try reading each file and compare its schema to schemas already seen
test_schema = spark.read.json(files[0].path).schema
#print(test_schema)

schemas = []
read_failures = []
for f in files:
    if hasattr(f, 'size'):
        try:
            sch = spark.read.json(f.path).schema
            tf = unique_list(sch, schemas)
        except:
            read_failures.append(f)
        
print(f"Files: {len(files)}\nSchemas: {len(schemas)}\nFile read failures: {len(read_failures)}")


# COMMAND ----------

df = spark.read.format("parquet").load(f"{filepath}/users/2024/01/*/*")
df.createOrReplaceTempView('users')
df.show()

# COMMAND ----------

# ====================================================================
# Use explode to open up billing information array for each record
#   and data frame operations to get columns of interest
# ====================================================================
from pyspark.sql.functions import explode
o = spark.sql("select 'users' obj, count(*) numRows from users;")
o.show()

# COMMAND ----------

# MAGIC %sql 
# MAGIC
# MAGIC -- ====================================================================
# MAGIC -- Alternately, use SQL to query the original temporary view 
# MAGIC --  and perform the explode in the query itself
# MAGIC -- ====================================================================
# MAGIC
# MAGIC select      *
# MAGIC from        users
# MAGIC limit 100;
# MAGIC

# COMMAND ----------


