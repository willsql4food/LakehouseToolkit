/* ====================================================================================================================
    Exprloration
==================================================================================================================== */
/* How many rows are there, and how many are not yet loaded? */
-- select      count(*) numRows, sum(case when wm.LoadedDateUTC is null then 1 else 0 end) numToLoad 
-- from        dbo.gbqWatermark wm

/* Sample a dozen rows */
-- select      top 12 *
-- from        dbo.gbqWatermark wm
-- where       wm.LoadedDateUTC is null

/* Find batches for rows not actually loaded */
select      right(TableName, 8) table_suffix
        ,   count(*) num_rows
        ,   min(LoadedDateUTC) earliest_load
        ,   max(LoadedDateUTC) latest_load
from        dbo.gbqWatermark wm
where       wm.RowCountDest is null
group by    right(TableName, 8)
order by    right(TableName, 8)

/* Sample most recently loaded rows */
select 		top 10 *
from		dbo.gbqWatermark wm
order by	LoadedDateUTC desc

/* ====================================================================================================================
    Action
==================================================================================================================== */
/* Mark everything as loaded */
-- update dbo.gbqWatermark set LoadedDateUTC = '1971-01-01' where LoadedDateUTC is null

/* Turn on a single day or range */
/*
update      dbo.gbqWatermark 
set         LoadedDateUTC = null
        ,   RowCountDest = null
where       TableName like '%2023%'
*/
select		*
from		dbo.gbqObjectToLoad
order by	right(TableName, 8) desc

select		*
from		stage.gbqObject
order by	right(TableName, 8) desc
