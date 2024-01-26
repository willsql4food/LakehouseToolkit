
select * from stage.gbqObject where TableName like '%20240101'
select * from stage.gbqObjectDetail where TableName like '%20240101'

select * from dbo.gbqObjectToLoad
select * from dbo.gbqObjectToLoadCmd

select * from dbo.gbqWatermark order by TableCatalog, TableSchema, TableName, BatchId

/* Any batches with multiple watermark records? */
select      TableCatalog, TableSchema, TableName, BatchId
from        dbo.gbqWatermark
group by    TableCatalog, TableSchema, TableName, BatchId
having      count(*) > 1
