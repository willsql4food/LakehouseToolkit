/*
select * from stage.gbqObject where TableName like '%20240101'
select * from stage.gbqObjectDetail where TableName like '%20240101'

select * from dbo.gbqObjectToLoad
select * from dbo.gbqObjectToLoadCmd

select * from dbo.gbqWatermark order by TableCatalog, TableSchema, TableName, BatchId

/* Reload a few files - delete or move the original files from the storage account also... 
---------------------------------------------------------------------------------------------
update  dbo.gbqWatermark 
set     LoadedDateUTC = null, RowCountDest = null, DestinationUri = null
where   Id between 292 and 299
*/

/* Any batches with multiple watermark records? */
select      TableCatalog, TableSchema, TableName, BatchId
from        dbo.gbqWatermark
group by    TableCatalog, TableSchema, TableName, BatchId
having      count(*) > 1

select top 20 * from dbo.gbqWatermark order by Id desc

select top 20 * from dbo.adfPipelineExecution order by StartTimeUtc desc

-- update  dbo.adfPipelineExecution set StatusMessage = 'Testing' where RunId = '1aa10904-0789-4dce-9460-11aa17bd3900'
*/

select      TableName, max(BatchId) highestBatchId, count(BatchId) countBatchId, format(sum(RowCountSource), 'N0') numRows
from        dbo.gbqWatermark
group by    TableName
order by    right(TableName, 8) desc, TableName

select      top 10 *
from        stage.gbqObjectDetail
order by    RowcountSource desc