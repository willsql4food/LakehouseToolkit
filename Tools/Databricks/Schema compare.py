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

# ====================================================================
# Function to crawl a directory tree and add any files found to an array
# ====================================================================
def dir_crawl(fso, subdir = "\t", file_array = []):
    tab = "\t"
    for obj in dbutils.fs.ls(fso.path):
        if hasattr(obj, 'size'):
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
# Read all the file names on the file path into a temporary view
# ====================================================================
dirs = dbutils.fs.ls(filepath)

files = []
for d in dirs:
    files.append(dir_crawl(d, " ", files))
        
#  Find total file size across all directories
total_size = 0
for f in files:
    if hasattr(f, 'size'):
        total_size += f.size
        
print(f"Total file size: {total_size:,} bytes.")        

# COMMAND ----------

# Try reading each file and compare its schema to schemas already seen
test_schema = spark.read.parquet(files[0].path).schema
#print(test_schema)

schemas = []
read_failures = []
num_files = 0

for f in files:
    if hasattr(f, 'size'):
        num_files += 1
        try:
            sch = spark.read.parquet(f.path).schema
            print(f"Columns: {len(sch)} in file: {f.name}")
            tf = unique_list(sch, schemas)
        except:
            read_failures.append(f)
        
print(f"\nFolder: {folder}\nFiles: {num_files}\nSchemas: {len(schemas)}\nFile read failures: {len(read_failures)}")

schema_sizes = []
for s in schemas:
    schema_sizes.append(len(s))

print(f"Distinct Schemas: {schema_sizes}")

# COMMAND ----------

dataTypes = []

for s in schemas:
    print(f"========================================")
    for t in s:
        print(f"{t.name}\t{t.dataType}")
        trash = unique_list(t.dataType, dataTypes)
    print(f"========================================\n")

print("\n***********************************************\nData Types\n---------------------------")
for dt in dataTypes:
    print(f"{dt}")

# COMMAND ----------

rc = spark.read.parquet(f"{filepath}/Audit/20240422-2059_row_counts.parquet")

rc.sort(["TABLE_NAME", "TABLE_SCHEMA"]).display()

# COMMAND ----------

sch = "DPIRANI"
tbl = "POMXTK"
df = spark.read.parquet(f"{filepath}/{sch}.{tbl}.parquet")
df.display()

# COMMAND ----------


