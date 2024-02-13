create procedure dbo.uspGetGbqLoadBatches (@RowsLimit int = 1e6, @PipelineExecutionId int = 0)
/*  ===================================================================================================================
    When            Who                     What
    --------------  ---------------------   ---------------------------------------------------------------------------
    2024-01-24      A. Carter Burleigh      Initial development
    2024-02-06      A. Carter Burleigh      Added logic to break single-term batches in multiple pieces when term has
                                            more rows than the limit
    2024-02-12      A. Carter Burleigh      Switched sequence generator from recursive CTE to function call

    --------------
    Purpose
    --------------
    Determine the SELECT statement for each needed batch load of Google Analytics data
    
    GA limits rows returned by queries (1M rows as of Jan, 2024) so this builds batches which gather data
    from GA in datasets smaller than the limit.

    ================================================================================================================ */
as
begin
    /*
    declare @RowsLimit int = 1e6;
    declare @PipelineExecutionId int = 0;
    */
    with cte as (
         /* Number the rows for each batch term, by table in descending order of batch term size */
        select      RowId = row_number() over (partition by TableCatalog, TableSchema, TableName order by RowCountSource desc), 
                    TableCatalog, TableSchema, TableName, BatchCol, BatchTerm, RowCountSource, SubBatchCol, 
                    SubBatchMin, SubBatchMax
        from        stage.gbqObjectDetail
    ), rt as (
        /* Conditional sum with prior row(s) until just before limit, then start new running total */
        select      RowId, TableCatalog, TableSchema, TableName, BatchCol, BatchTerm, 
                    SubBatchCol, SubBatchMin, SubBatchMax, RowCountSource, 
                    RunningTotal = RowCountSource, 
                    BatchId = 1
        from        cte
        where       RowId = 1
        union all
        select      a.RowId, a.TableCatalog, a.TableSchema, a.TableName, a.BatchCol, a.BatchTerm,
                    a.SubBatchCol, a.SubBatchMin, a.SubBatchMax, a.RowCountSource,
                    /* If adding to the running total is still below limit, accumulate; otherwise, start new... */
                    RunningTotal = case 
                        when a.RowCountSource + b.RunningTotal < @RowsLimit then a.RowCountSource + b.RunningTotal 
                        else a.RowCountSource 
                    end,
                    /* ... and increment batch number */
                    BatchId = case 
                        when a.RowCountSource + b.RunningTotal < @RowsLimit then b.BatchId 
                        else b.BatchId + 1 
                    end
        from        cte a
        join        rt b    on  b.TableCatalog = a.TableCatalog 
                            and b.TableSchema = a.TableSchema 
                            and b.TableName = a.TableName
                            and b.RowId = a.RowId - 1
    ), sb as (
        /*  Find any terms that need to be run in multiple sub-batches 
            Use integer division to get number of sub-batches, adding one to get remaining partial */
        select      TableCatalog, TableSchema, TableName, BatchCol, BatchTerm, 
                    SubBatchCol, SubBatchMin, SubBatchMax, 
                    RowCountSource = RunningTotal, 
                    BatchId,
                    /* Find the number of slices needed and the domain of the data */
                    NumSlices = 1 + RowCountSource / @RowsLimit , 
                    SubBatchDomain = SubBatchMax - SubBatchMin 
        from        rt
        where       rt.RowCountSource > @RowsLimit
    ), slc as (
        /* Cut the domain of the sub-batch into adjacent ranges of approximately equal distances */
        select      TableCatalog, TableSchema, TableName, BatchCol, BatchTerm, 
                    SubBatchCol, RowCountSource,  BatchId,
                    SubBatchId = s.value, NumSlices,
                    SubBatchRange = SubBatchDomain / NumSlices,
                    RangeStart = SubBatchMin + s.value * SubBatchDomain / NumSlices,
                    RangeEnd = SubBatchMin + (1 + s.value) * SubBatchDomain / NumSlices
        from        sb 
        join        generate_series(0, (select max(NumSlices) - 1 from sb)) s on s.value < sb.NumSlices /* Zero based SubBatchId sequence... */
    )

    /* Add to the watermark rows for each (Object, Batch, SubBatch) needed */
    insert into dbo.gbqWatermark (TableCatalog, TableSchema, TableName, BatchCol, BatchId, SubBatchId, WhereClause, RowCountSource, PipelineExecutionId)
    /* Add the batches that have row counts below the threshold to the Watermark table */
    select      TableCatalog, TableSchema, TableName, BatchCol, BatchId, 
                SubBatchId = convert(int, null),
                WhereClause = case 
                    when BatchCol is not null then concat(' where ', BatchCol, ' in (', string_agg(quotename(BatchTerm, char(39)), ', '), ')')  
                    else ''
                end, 
                RowCountSource = max(RunningTotal),
                PipelineExecutionId = @PipelineExecutionId
    from        rt
    where       not exists (    select      *
                                from        dbo.gbqWatermark x
                                where       x.TableCatalog = rt.TableCatalog and x.TableSchema = rt.TableSchema and x.TableName = rt.TableName )
        and     TableName <> 'na'
        and     RowCountSource <= @RowsLimit
    group by    TableCatalog, TableSchema, TableName, BatchId, BatchCol
    union all
    /* If the row count is above the limit, break the batch on a secondary condition (modified Unix-style timestamp) */
    select      TableCatalog, TableSchema, TableName, BatchCol, BatchId, SubBatchId,
                WhereClause = concat(' where ', BatchCol, ' = ', quotename(BatchTerm, char(39)), 
                        case 
                            when SubBatchId = 0 then concat(' and ', SubBatchCol, ' <  ', RangeEnd)
                            when SubBatchId = NumSlices - 1 then concat(' and ', SubBatchCol, ' >= ', RangeStart)
                            else  concat(' and ', SubBatchCol, ' >= ', RangeStart, ' and ', SubBatchCol, ' < ', RangeEnd)
                        end),
                RowCountSource = case when SubBatchId = 0 then RowCountSource else null end, 
                PipelineExecutionId = @PipelineExecutionId
    from        slc

    /* Return a list of batches to load.  Include:
        * SELECT statement
        * Folder path 
        * File name 
    */
    select      TableCatalog, TableSchema, TableName, BatchId, SubBatchId,
                concat_ws('/', 
                    TableCatalog, TableSchema, 
                    left(TableName, len(TableName) - 9),            /* Remove _YYYYMMDD */
                    substring(TableName, len(TableName) - 7, 4),    /* YYYY */
                    substring(TableName, len(TableName) - 3, 2),    /* MM */
                    right(TableName, 2)                             /* DD */
                ) FilePath,
                concat_ws('.', TableName, 'Batch', BatchId, SubBatchId, 'parquet') [FileName],
                concat('select * from ', TableSchema, '.', TableName, WhereClause) SelectCmd
    from        dbo.gbqWatermark
    where       LoadedDateUtc is null
end

go
