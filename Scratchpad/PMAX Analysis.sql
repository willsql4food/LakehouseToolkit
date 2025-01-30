-- Databricks notebook source
-- DBTITLE 1,Setup Environment Variables
-- MAGIC %python
-- MAGIC dbutils.widgets.dropdown('env', 'dev', ['dev', 'qa', 'prod'])
-- MAGIC dbutils.widgets.text("start_date", "20240823", "Start YYYYMMDD")
-- MAGIC dbutils.widgets.text("end_date", "20240926", "End YYYYMMDD")

-- COMMAND ----------

/* Turn this into a Silver table with a daily summary per url */

--create table if not exists identifier('silver_' || :env || '.googleanalytics.pmax_page_views') as
insert into identifier('silver_' || :env || '.googleanalytics.pmax_page_views') 
SELECT    e.event_date,
          e.device__category,
          CASE 
            WHEN contains(e.session_traffic_source_last_click__google_ads_campaign__campaign_name, '|PLA')
            THEN 'PLA'
            ELSE 'LIA'
          END AS PMAX_Channel,
          ep.string_val page_viewed,
          count(*) num_page_views
FROM      identifier('bronze_' || :env || '.googleanalytics.ga_events') e
join      identifier('bronze_' || :env || '.googleanalytics.ga_events_event_params') ep on ep.join_key = e.join_key
WHERE     e.event_name = 'page_view'
  and     ep.key = 'page_location'
  and     e.geo__country in ('Mexico', 'United States', 'Canada')
  and   ( -- 9-digit product ID immediately before HMTL
          regexp_instr(ep.string_val, '[0-9][0-9][0-9][0-9][0-9][0-9][0-9][0-9][0-9]\.html') > 0    
          -- Various alphaNNN patterns
      or  regexp_instr(ep.string_val, 'M[0-9][0-9][0-9][0-9]') > 0  
      or  regexp_instr(ep.string_val, 'KS[0-9][0-9][0-9]') > 0
      or  regexp_instr(ep.string_val, 'SD[0-9][0-9][0-9]') > 0
      or  regexp_instr(ep.string_val, 'B[0-9][0-9][0-9][0-9]') > 0
      or  regexp_instr(ep.string_val, 'C[0-9][0-9][0-9][0-9]') > 0
      or  regexp_instr(ep.string_val, 'DL[0-9][0-9][0-9]') > 0    
      or  regexp_instr(ep.string_val, 'LA[0-9][0-9][0-9]') > 0    
      or  regexp_instr(ep.string_val, 'AV[0-9][0-9][0-9]') > 0   
      or  regexp_instr(ep.string_val, 'S[0-9][0-9][0-9][0-9]') > 0
      or  regexp_instr(ep.string_val, 'SF[0-9][0-9][0-9]') > 0   
      or  regexp_instr(ep.string_val, 'SWT[0-9][0-9][0-9]') > 0     
      or  regexp_instr(ep.string_val, 'VI[0-9][0-9][0-9]') > 0    
      or  regexp_instr(ep.string_val, 'T[0-9][0-9][0-9][0-9]') > 0     
      or  regexp_instr(ep.string_val, 'SR[0-9][0-9][0-9]') > 0
      or  regexp_instr(ep.string_val, 'SPP[0-9][0-9][0-9]') > 0
      or  regexp_instr(ep.string_val, 'SV[0-9][0-9][0-9]') > 0 )                            
  -- PMAX Channel Filter
  and contains (e.session_traffic_source_last_click__google_ads_campaign__campaign_name, '|PMAX|') 
  -- Processing Date Filter
  and     e.event_date BETWEEN :start_date and :end_date --Fiscal Month
GROUP BY  e.event_date
    ,     e.device__category
    ,     ep.string_val
    ,     e.session_traffic_source_last_click__google_ads_campaign__campaign_name
order BY  e.device__category

-- COMMAND ----------

-- DBTITLE 1,Quick Summary

select    PMAX_Channel, event_date, sum(num_page_views) 
from      identifier('silver_' || :env || '.googleanalytics.pmax_page_views')
group by  PMAX_Channel, event_date
order by  event_date desc, PMAX_Channel

-- COMMAND ----------


