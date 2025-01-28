select @@servername srvr, 'dbo.adfPipelineExecution' Tbl, min(id) lowId, max(id) topId, count(*) numRows from dbo.adfPipelineExecution
union all 
select @@servername srvr, 'dbo.gbqWatermark' Tbl, min(id) lowId, max(id) topId, count(*) numRows from dbo.gbqWatermark
