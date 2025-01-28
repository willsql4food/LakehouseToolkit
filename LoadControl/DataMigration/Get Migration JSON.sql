select  Id,
        TableCatalog,
        TableSchema,
        TableName,
        BatchCol,
        BatchId,
        replace(WhereClause, char(39), char(39)+char(39)) WhereClause,
        RowCountSource,
        RowCountDest,
        LoadedDateUTC,
        DestinationUri,
        SourceDeleted,
        PipelineExecutionId,
        SubBatchId
from    dbo.gbqWatermark
--where   Id between 100 and 150
for json path;


select  Id,
        PipelineName,
        RunId,
        StartTimeUtc,
        EndTimeUtc,
        BatchCount,
        StatusMessage
from    dbo.adfPipelineExecution
-- where   Id > 36
for json path;
