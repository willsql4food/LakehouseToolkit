/* Simple summary of what has been loaded and any marked to load (assume in progress) */
select      case
                when LoadedDateUTC is not null then 'Loaded' 
                when LoadedDateUTC is null and right(TableName, 8) like '2023%' then 'No longer available' 
                else 'Waiting' 
            end UserMessage,
            count(*) BatchCount,
            format(sum(RowCountSource), 'N0') RowsToLoad, 
            format(sum(RowCountDest), 'N0') RowsLoaded,
            min(LoadedDateUTC) EarliestOperation, max(LoadedDateUTC) MostRecentOperation
from        dbo.gbqWatermark
group by    case
                when LoadedDateUTC is not null then 'Loaded' 
                when LoadedDateUTC is null and right(TableName, 8) like '2023%' then 'No longer available' 
                else 'Waiting' 
            end

/* Rows Loaded and variance - must calculate at batch level, accounting for potential sub-batches */
select      Mismatch = case when s.RowCountSource <> coalesce(s.RowCountDest, 0) then '***' else '' end,
            a.LoadedDateUTC, a.TableName, a.BatchId, a.SubBatchId, 
            s.RowCountSource, s.RowCountDest, 
            Variance = s.RowCountSource - s.RowCountDest,
            BatchRowCountDest = a.RowCountDest
from        dbo.gbqWatermark a
left join  (select  TableName, BatchId, RowCountSource = sum(RowCountSource), RowCountDest = sum(RowCountDest), NumSubBatches = count(*) 
            from    dbo.gbqWatermark group by TableName, BatchId) s on s.TableName = a.TableName and s.BatchId = a.BatchId
where       LoadedDateUTC is not null
order by    Mismatch desc, right(a.TableName, 8) desc, a.TableName, a.BatchId, a.SubBatchId

/* ====================================================================================================================
    Diagnostics
==================================================================================================================== */
/*
select      *
from        dbo.gbqWatermark for system_time all
where       TableName = 'events_20240224'
order by    BatchId, SysStart

select      *
from        dbo.gbqWatermark
where       LoadedDateUTC is null
*/