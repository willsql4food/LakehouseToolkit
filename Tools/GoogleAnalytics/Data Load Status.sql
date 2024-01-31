select      case when LoadedDateUTC is not null then 'Loaded' else 'Waiting' end msg,
            count(*) BatchCount,
            format(sum(RowCountSource), 'N0') RowsToLoad, 
            format(sum(RowCountDest), 'N0') RowsLoaded,
            min(LoadedDateUTC) EarliestOperation, max(LoadedDateUTC) MostRecentOperation
from        dbo.gbqWatermark
group by    case when LoadedDateUTC is not null then 'Loaded' else 'Waiting' end


select      case when RowCountSource <> RowCountDest then '***' else '' end Mismatch,
            LoadedDateUTC, TableName, BatchId, RowCountSource, RowCountDest, 
            RowCountSource - RowCountDest Variance
from        dbo.gbqWatermark
where       LoadedDateUTC is not null
order by    Mismatch desc, right(TableName, 8) desc, TableName, BatchId
