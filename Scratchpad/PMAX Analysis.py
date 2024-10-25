# Databricks notebook source
dbutils.widgets.text("start_date", "20240823", "Start YYYYMMDD")
dbutils.widgets.text("end_date", "20240926", "End YYYYMMDD")

# COMMAND ----------

# MAGIC %sql
# MAGIC --PMAX QUERY--
# MAGIC
# MAGIC SELECT  e.device__category,
# MAGIC           ep.string_val page_viewed,
# MAGIC           count(*) num_page_views
# MAGIC FROM      bronze_dev.googleanalytics.ga_events e
# MAGIC join      bronze_dev.googleanalytics.ga_events_event_params ep on ep.join_key = e.join_key
# MAGIC WHERE     e.event_date BETWEEN :start_date and :end_date --Fiscal Month
# MAGIC   and     e.event_name = 'page_view'
# MAGIC   and     ep.key = 'page_location'
# MAGIC   and     e.geo__country in ('Mexico', 'United States', 'Canada')
# MAGIC   and   ( regexp_instr(ep.string_val, '[0-9][0-9][0-9][0-9][0-9][0-9][0-9][0-9][0-9]\.html') > 0    -- 9-digit product ID immediately before HMTL
# MAGIC       or  regexp_instr(ep.string_val, 'M[0-9][0-9][0-9][0-9]') > 0  
# MAGIC       or  regexp_instr(ep.string_val, 'KS[0-9][0-9][0-9]') > 0
# MAGIC       or  regexp_instr(ep.string_val, 'SD[0-9][0-9][0-9]') > 0
# MAGIC       or  regexp_instr(ep.string_val, 'B[0-9][0-9][0-9][0-9]') > 0
# MAGIC       or  regexp_instr(ep.string_val, 'C[0-9][0-9][0-9][0-9]') > 0
# MAGIC       or  regexp_instr(ep.string_val, 'DL[0-9][0-9][0-9]') > 0    
# MAGIC       or  regexp_instr(ep.string_val, 'LA[0-9][0-9][0-9]') > 0    
# MAGIC       or  regexp_instr(ep.string_val, 'AV[0-9][0-9][0-9]') > 0   
# MAGIC       or  regexp_instr(ep.string_val, 'S[0-9][0-9][0-9][0-9]') > 0
# MAGIC       or  regexp_instr(ep.string_val, 'SF[0-9][0-9][0-9]') > 0   
# MAGIC       or  regexp_instr(ep.string_val, 'SWT[0-9][0-9][0-9]') > 0     
# MAGIC       or  regexp_instr(ep.string_val, 'VI[0-9][0-9][0-9]') > 0    
# MAGIC       or  regexp_instr(ep.string_val, 'T[0-9][0-9][0-9][0-9]') > 0     
# MAGIC       or  regexp_instr(ep.string_val, 'SR[0-9][0-9][0-9]') > 0
# MAGIC       or  regexp_instr(ep.string_val, 'SPP[0-9][0-9][0-9]') > 0
# MAGIC       or  regexp_instr(ep.string_val, 'SV[0-9][0-9][0-9]') > 0   )                                                                              
# MAGIC and contains (e.session_traffic_source_last_click__google_ads_campaign__campaign_name, '|PMAX|') -- PMAX Channel Filter
# MAGIC GROUP BY  e.device__category,
# MAGIC         ep.string_val
# MAGIC order BY  e.device__category

# COMMAND ----------


