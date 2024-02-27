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


/* ====================================================================================================================
    Action
==================================================================================================================== */
/* Mark everything as loaded */
-- update dbo.gbqWatermark set LoadedDateUTC = '1971-01-01' where LoadedDateUTC is null

/* Turn on a single day */
update      dbo.gbqWatermark 
set         LoadedDateUTC = null
        ,   RowCountDest = null
where       TableName like '%users%20240225'

select		*
from		dbo.gbqObjectToLoad
