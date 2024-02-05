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
# MAGIC         schema_of_json(device) device_json_schema,
# MAGIC         schema_of_json(audiences) audiences_json_schema
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
# MAGIC )
# MAGIC select          operating_system,
# MAGIC                 category,
# MAGIC                 mobile_brand_name,
# MAGIC                 mobile_model_name,
# MAGIC                 count(*) num_rows
# MAGIC from            exp
# MAGIC group by        operating_system,
# MAGIC                 category,
# MAGIC                 mobile_brand_name,
# MAGIC                 mobile_model_name
# MAGIC order by        num_rows desc

# COMMAND ----------

# MAGIC %sql
# MAGIC with j as (
# MAGIC     select      pseudo_user_id,
# MAGIC                 from_json(audiences, 'ARRAY<STRUCT<id BIGINT, name STRING, membership_start_timestamp_micros BIGINT, membership_expiry_timestamp_micros BIGINT, npa INT>>') audiences_json,
# MAGIC                 from_json(audiences, 'STRUCT<v: ARRAY<STRUCT<v: STRUCT<f: ARRAY<STRUCT<v: STRING>>>>>>') audiences_def_json,
# MAGIC                 audiences
# MAGIC     from        vwPseudoUsers 
# MAGIC     where       pseudo_user_id = 1303696881.1679179401
# MAGIC )
# MAGIC
# MAGIC select      pseudo_user_id, explode(audiences_def_json.v)
# MAGIC from        j
# MAGIC -- where       audiences.id is not null
# MAGIC limit       100;
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC select * from vwPseudoUsers limit 100

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC select * from vwEvents limit 100

# COMMAND ----------

/*
Replacements:
    INT64 --> BIGINT
    FLOAT64 --> FLOAT
    BOOL --> INT

table_name         ,ordinal_position ,column_name                   ,data_type
----------------    ----------------  ------------------------       -----------------------
events             ,               1 ,event_date                    ,STRING
events             ,               2 ,event_timestamp               ,BIGINT
events             ,               3 ,event_name                    ,STRING
events             ,               4 ,event_params                  ,"ARRAY<STRUCT<key STRING, value STRUCT<string_value STRING, int_value BIGINT, float_value FLOAT, double_value FLOAT>>>"
events             ,               5 ,event_previous_timestamp      ,BIGINT
events             ,               6 ,event_value_in_usd            ,FLOAT
events             ,               7 ,event_bundle_sequence_id      ,BIGINT
events             ,               8 ,event_server_timestamp_offset ,BIGINT
events             ,               9 ,user_id                       ,STRING
events             ,              10 ,user_pseudo_id                ,STRING
events             ,              11 ,privacy_info                  ,"STRUCT<analytics_storage STRING, ads_storage STRING, uses_transient_token STRING>"
events             ,              12 ,user_properties               ,"ARRAY<STRUCT<key STRING, value STRUCT<string_value STRING, int_value BIGINT, float_value FLOAT, double_value FLOAT, set_timestamp_micros BIGINT>>>"
events             ,              13 ,user_first_touch_timestamp    ,BIGINT
events             ,              14 ,user_ltv                      ,"STRUCT<revenue FLOAT, currency STRING>"
events             ,              15 ,device                        ,"STRUCT<category STRING, mobile_brand_name STRING, mobile_model_name STRING, mobile_marketing_name STRING, mobile_os_hardware_model STRING, operating_system STRING, operating_system_version STRING, vendor_id STRING, advertising_id STRING, language STRING, is_limited_ad_tracking STRING, time_zone_offset_seconds BIGINT, browser STRING, browser_version STRING, web_info STRUCT<browser STRING, browser_version STRING, hostname STRING>>"
events             ,              16 ,geo                           ,"STRUCT<city STRING, country STRING, continent STRING, region STRING, sub_continent STRING, metro STRING>"
events             ,              17 ,app_info                      ,"STRUCT<id STRING, version STRING, install_store STRING, firebase_app_id STRING, install_source STRING>"
events             ,              18 ,traffic_source                ,"STRUCT<name STRING, medium STRING, source STRING>"
events             ,              19 ,stream_id                     ,STRING
events             ,              20 ,platform                      ,STRING
events             ,              21 ,event_dimensions              ,STRUCT<hostname STRING>
events             ,              22 ,ecommerce                     ,"STRUCT<total_item_quantity BIGINT, purchase_revenue_in_usd FLOAT, purchase_revenue FLOAT, refund_value_in_usd FLOAT, refund_value FLOAT, shipping_value_in_usd FLOAT, shipping_value FLOAT, tax_value_in_usd FLOAT, tax_value FLOAT, unique_items BIGINT, transaction_id STRING>"
events             ,              23 ,items                         ,"ARRAY<STRUCT<item_id STRING, item_name STRING, item_brand STRING, item_variant STRING, item_category STRING, item_category2 STRING, item_category3 STRING, item_category4 STRING, item_category5 STRING, price_in_usd FLOAT, price FLOAT, quantity BIGINT, item_revenue_in_usd FLOAT, item_revenue FLOAT, item_refund_in_usd FLOAT, item_refund FLOAT, coupon STRING, affiliation STRING, location_id STRING, item_list_id STRING, item_list_name STRING, item_list_index STRING, promotion_id STRING, promotion_name STRING, creative_name STRING, creative_slot STRING, item_params ARRAY<STRUCT<key STRING, value STRUCT<string_value STRING, int_value BIGINT, float_value FLOAT, double_value FLOAT>>>>>"
events             ,              24 ,collected_traffic_source      ,"STRUCT<manual_campaign_id STRING, manual_campaign_name STRING, manual_source STRING, manual_medium STRING, manual_term STRING, manual_content STRING, gclid STRING, dclid STRING, srsltid STRING>"
events             ,              25 ,is_active_user                ,INT
pseudonymous_users ,               1 ,pseudo_user_id                ,STRING
pseudonymous_users ,               2 ,stream_id                     ,STRING
pseudonymous_users ,               3 ,user_info                     ,"STRUCT<last_active_timestamp_micros BIGINT, user_first_touch_timestamp_micros BIGINT, first_purchase_date STRING>"
pseudonymous_users ,               4 ,device                        ,"STRUCT<operating_system STRING, category STRING, mobile_brand_name STRING, mobile_model_name STRING, unified_screen_name STRING>"
pseudonymous_users ,               5 ,geo                           ,"STRUCT<city STRING, country STRING, continent STRING, region STRING>"
pseudonymous_users ,               6 ,audiences                     ,"ARRAY<STRUCT<id BIGINT, name STRING, membership_start_timestamp_micros BIGINT, membership_expiry_timestamp_micros BIGINT, npa INT>>"
pseudonymous_users ,               7 ,user_properties               ,"ARRAY<STRUCT<key STRING, value STRUCT<string_value STRING, set_timestamp_micros BIGINT, user_property_name STRING>>>"
pseudonymous_users ,               8 ,user_ltv                      ,"STRUCT<revenue_in_usd FLOAT, sessions BIGINT, engagement_time_millis BIGINT, purchases BIGINT, engaged_sessions BIGINT, session_duration_micros BIGINT>"
pseudonymous_users ,               9 ,predictions                   ,"STRUCT<in_app_purchase_score_7d FLOAT, purchase_score_7d FLOAT, churn_score_7d FLOAT, revenue_28d_in_usd FLOAT>"
pseudonymous_users ,              10 ,privacy_info                  ,"STRUCT<is_limited_ad_tracking STRING, is_ads_personalization_allowed STRING>"
pseudonymous_users ,              11 ,occurrence_date               ,STRING
pseudonymous_users ,              12 ,last_updated_date             ,STRING
users              ,               1 ,user_id                       ,STRING
users              ,               2 ,user_info                     ,"STRUCT<last_active_timestamp_micros BIGINT, user_first_touch_timestamp_micros BIGINT, first_purchase_date STRING>"
users              ,               3 ,device                        ,"STRUCT<operating_system STRING, category STRING, mobile_brand_name STRING, mobile_model_name STRING, unified_screen_name STRING>"
users              ,               4 ,geo                           ,"STRUCT<city STRING, country STRING, continent STRING, region STRING>"
users              ,               5 ,audiences                     ,"ARRAY<STRUCT<id BIGINT, name STRING, membership_start_timestamp_micros BIGINT, membership_expiry_timestamp_micros BIGINT, npa INT>>"
users              ,               6 ,user_properties               ,"ARRAY<STRUCT<key STRING, value STRUCT<string_value STRING, set_timestamp_micros BIGINT, user_property_name STRING>>>"
users              ,               7 ,user_ltv                      ,"STRUCT<revenue_in_usd FLOAT, sessions BIGINT, engagement_time_millis BIGINT, purchases BIGINT, engaged_sessions BIGINT, session_duration_micros BIGINT>"
users              ,               8 ,predictions                   ,"STRUCT<in_app_purchase_score_7d FLOAT, purchase_score_7d FLOAT, churn_score_7d FLOAT, revenue_28d_in_usd FLOAT>"
users              ,               9 ,privacy_info                  ,"STRUCT<is_limited_ad_tracking STRING, is_ads_personalization_allowed STRING>"
users              ,              10 ,occurrence_date               ,STRING
users              ,              11 ,last_updated_date             ,STRING
*/
