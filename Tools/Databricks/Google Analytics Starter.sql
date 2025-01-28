-- Databricks notebook source
-- MAGIC %md
-- MAGIC # Helpful Links
-- MAGIC
-- MAGIC [Google's explanation of field definitions](https://support.google.com/analytics/answer/7029846?hl=en#zippy=%2Cevent%2Cuser)

-- COMMAND ----------

-- MAGIC %md
-- MAGIC # Setup Connection
-- MAGIC * Storage Account & Container
-- MAGIC * Root path to Google Analytics data
-- MAGIC * Build dataframe for each of the overall datasets - events, users & pseudonymous_users
-- MAGIC
-- MAGIC   * __Note__ Wild cards at end of each path are for /Year/Month/Day/file(s)

-- COMMAND ----------

-- MAGIC %python
-- MAGIC # ========================================================================================================================================
-- MAGIC # Storage account and protocol for connecting
-- MAGIC # ========================================================================================================================================
-- MAGIC protocol = "abfss://"
-- MAGIC store = "sadevdatalakehouse"
-- MAGIC container = "datalake"
-- MAGIC rootpath = "bronze/GoogleAnalytics/fnd-cloud-project/analytics_250303278/"
-- MAGIC # For first-loaded data (prior to flattening complex datatypes):
-- MAGIC # rootpath = "bronze/GoogleAnalytics_ComplexDatatypes/fnd-cloud-project/analytics_250303278/"
-- MAGIC
-- MAGIC # ========================================================================================================================================
-- MAGIC # Set Spark configuration
-- MAGIC # In this instance, connect to the storage account using SAS (shared access signature) and provide said token from databricks secret
-- MAGIC # ========================================================================================================================================
-- MAGIC spark.conf.set(f"fs.azure.account.auth.type.{store}.dfs.core.windows.net", "SAS")
-- MAGIC spark.conf.set(f"fs.azure.sas.token.provider.type.{store}.dfs.core.windows.net", "org.apache.hadoop.fs.azurebfs.sas.FixedSASTokenProvider")
-- MAGIC spark.conf.set(f"fs.azure.sas.fixed.token.{store}.dfs.core.windows.net", dbutils.secrets.get(scope="google-analytics", key=f"ga-sas-key-sadevdatalakehouse"))
-- MAGIC
-- MAGIC datapath = f"{protocol}{container}@{store}.dfs.core.windows.net/{rootpath}"
-- MAGIC
-- MAGIC yyyy = dbutils.widgets.get('yyyy')
-- MAGIC mm = dbutils.widgets.get('mm')
-- MAGIC dd = dbutils.widgets.get('dd')
-- MAGIC
-- MAGIC pseudo_users = spark.read.format("parquet").load(f"{datapath}/pseudonymous_users/{yyyy}/{mm}/{dd}/*")
-- MAGIC users = spark.read.format("parquet").load(f"{datapath}/users/{yyyy}/{mm}/{dd}/*")
-- MAGIC events = spark.read.format("parquet").load(f"{datapath}/events/{yyyy}/{mm}/{dd}/*")
-- MAGIC
-- MAGIC pseudo_users.createOrReplaceTempView("vwPseudoUsers")
-- MAGIC users.createOrReplaceTempView("vwUsers")
-- MAGIC events.createOrReplaceTempView("vwEvents")

-- COMMAND ----------

          select 'vwPseudoUsers' obj, count(*) numRows from vwPseudoUsers pu
union all select 'vwUsers' obj, count(*) numRows from vwUsers pu
union all select 'vwEvents' obj, count(*) numRows from vwEvents pu

-- COMMAND ----------

-- SELECT
--   device_category,
--   COUNT(DISTINCT session_id) AS sessions,
--     SUM(event_count_without_session_start),
--     COUNT(DISTINCT session_id)
--   AS events_per_session
-- FROM
--   (
    -- SELECT
    --   device__category,
    --   CONCAT(
    --     pseudo_user_id      ) AS session_id,
    --   count_if(event__name NOT IN ('session_start')) AS event_count_without_session_start
    -- FROM
    --   vwPseudoUsers
    -- GROUP BY
    --   device_category,
    --   session_id
--   )
-- GROUP BY
--   device_category
-- ORDER BY
--   device_category ASC


-- COMMAND ----------

select  *
from    vwEvents e 
limit   10

-- COMMAND ----------

select  *
from    vwPseudoUsers pu 
limit   10

-- COMMAND ----------

select  *
from    vwUsers e 
limit   10
