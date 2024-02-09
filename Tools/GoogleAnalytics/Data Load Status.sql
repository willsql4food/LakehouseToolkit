/* Simple summary of what has been loaded and any marked to load (assume in progress) */
select      case when LoadedDateUTC is not null then 'Loaded' else 'Waiting' end msg,
            count(*) BatchCount,
            format(sum(RowCountSource), 'N0') RowsToLoad, 
            format(sum(RowCountDest), 'N0') RowsLoaded,
            min(LoadedDateUTC) EarliestOperation, max(LoadedDateUTC) MostRecentOperation
from        dbo.gbqWatermark
group by    case when LoadedDateUTC is not null then 'Loaded' else 'Waiting' end

/* Rows Loaded and variance - must calculate at batch level, accounting for potential sub-batches */
select      case when s.RowCountSource <> s.RowCountDest then '***' else '' end Mismatch,
            LoadedDateUTC, a.TableName, a.BatchId, a.SubBatchId, RowCountSource, RowCountDest, 
            RowCountSource - RowCountDest Variance
from        dbo.gbqWatermark a
left join  (select  TableName, BatchId, RowCountSource = sum(RowCountSource), RowCountDest = sum(RowCountDest), NumSubBatches = count(*) 
            from    dbo.gbqWatermark group by TableName, BatchId) s on s.TableName = a.TableName and s.BatchId = a.BatchId
where       LoadedDateUTC is not null
order by    Mismatch desc, right(TableName, 8) desc, TableName, BatchId, SubBatchId
