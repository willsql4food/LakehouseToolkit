create procedure dbo.uspGetGbqLoadBatches (@RowsLimit int = 1e6, @PipelineExecutionId int = 0)
/*  ===================================================================================================================
    When            Who                     What
    --------------  ---------------------   ---------------------------------------------------------------------------
    2024-01-24      A. Carter Burleigh      Initial development
    2024-02-06      A. Carter Burleigh      Added logic to break single-term batches in multiple pieces when term has
                                            more rows than the limit
    2024-02-12      A. Carter Burleigh      Switched sequence generator from recursive CTE to function call
    2024-02-14      A. Carter Burleigh      Switched back to recursive CTE as my target DBs are... compat level 150
    2024-02-26      A. Carter Burleigh      Added expansion of complex fields to their simple components
    2024-04-04      A. Carter Burleigh      Shim to get around some recursion issue caused by April data...
    2024-04-05      A. Carter Burleigh      Remove shim and add recursion safety 100 row bracket on running total
    2024-04-24      A. Carter Burleigh      TEMPORARILY DISABLE pseudonymous_users

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
        select      RowId = row_number() over (partition by TableCatalog, TableSchema, TableName order by BatchTerm desc), 
                    TableCatalog, TableSchema, TableName, BatchCol, BatchTerm, RowCountSource, SubBatchCol, 
                    SubBatchMin, SubBatchMax
        from        stage.gbqObjectDetail
        where       TableName not like '%pseudonymous%'
    ), rt as (
        /* Conditional sum with prior row(s) until just before limit, then start new running total */
        select      RowId, TableCatalog, TableSchema, TableName, BatchCol, BatchTerm, 
                    SubBatchCol, SubBatchMin, SubBatchMax, RowCountSource, 
                    RunningTotal = RowCountSource, 
                    BatchId = 1 + 100 * (RowId / 100)
        from        cte
        where       (RowId % 100) = 1
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
							-- Prevent exceeding recursion limit when there are more than about 100 rows
							-- 	by assuring the combined rows are in the same bracket of 100
							and	(b.RowId / 100) = (a.RowId / 100)
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
    ), generate_series as (
        select      TableCatalog, TableSchema, TableName, BatchId, [value] = NumSlices - 1, NumSlices, RowCountSource
        from        sb
        union all
        select      i.TableCatalog, i.TableSchema, i.TableName, i.BatchId, i.[value] - 1, i.NumSlices, i.RowCountSource
        from        generate_series i
        where       i.[value] > 0
    ), slc as (
        /* Cut the domain of the sub-batch into adjacent ranges of approximately equal distances */
        select      sb.TableCatalog, sb.TableSchema, sb.TableName, sb.BatchCol, sb.BatchTerm, 
                    sb.SubBatchCol, sb.RowCountSource, sb.BatchId,
                    SubBatchId = gs.[value], sb.NumSlices,
                    SubBatchRange = sb.SubBatchDomain / sb.NumSlices,
                    RangeStart = sb.SubBatchMin + gs.[value] * sb.SubBatchDomain / sb.NumSlices,
                    RangeEnd = sb.SubBatchMin + (1 + gs.[value]) * sb.SubBatchDomain / sb.NumSlices
        from        sb 
        -- join        generate_series(0, (select max(NumSlices) - 1 from sb)) gs on gs.[value] < sb.NumSlices /* Zero based SubBatchId sequence... */
        join        generate_series gs on   gs.[value] < sb.NumSlices /* Zero based SubBatchId sequence... */
									and		gs.TableCatalog = sb.TableCatalog
									and		gs.TableSchema = sb.TableSchema
									and		gs.TableName = sb.TableName
									and		gs.BatchId = sb.BatchId
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
                                where       x.TableCatalog = rt.TableCatalog and x.TableSchema = rt.TableSchema and x.TableName = rt.TableName and x.BatchId = rt.BatchId )
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
    where       not exists (    select      *
                                from        dbo.gbqWatermark x
                                where       x.TableCatalog = slc.TableCatalog and x.TableSchema = slc.TableSchema and x.TableName = slc.TableName and x.BatchId = slc.BatchId and x.SubBatchId = slc.SubBatchId )

    /* ====================================================================================================================
        Return a list of batches to load.  Include:
            * SELECT statement
            * Folder path
            * File name
            * JSON definition of column mappings for ADF write step
    ==================================================================================================================== */

    /* How to parse the column data type definitions fetched from Google */
    declare @open nvarchar(255) = 'STRUCT<'
        ,   @close nvarchar(255) = '>'
        ,   @el_delim nchar(1) = ','
        ,   @id_delim nchar(1) = ' '
        ,   @src_sep nvarchar(255) = '.'
        ,   @snk_sep nvarchar(255) = '__';

    /* ================================================================================================================
       A CTE to get all the columns for each table to load
       for compound columns expand the schema to treat each element as a simple column
    ================================================================================================================ */
    with cols as (
        /* Those compound columns */
        select      c.TableCatalog
                ,   c.TableSchema
                ,   c.TableName
                ,   c.ColumnId
                ,   Source = concat_ws(@src_sep, c.ColumnName, d.source)
                ,   Sink = concat_ws(@snk_sep, c.ColumnName, d.sink)
                ,   d.[position]
        from        dbo.gbqColumn c
        cross apply dbo.udtSchemaExpand(c.DataType, @open, @close, @el_delim, @id_delim, @src_sep, @snk_sep) d
        where       c.DataType like concat(@open, '%')
        /* The simple columns */
        union all
        select      c.TableCatalog
                ,   c.TableSchema
                ,   c.TableName
                ,   c.ColumnId
                ,   Source = c.ColumnName
                ,   Sink = c.ColumnName
                ,   [position] = 0
        from        dbo.gbqColumn c
        where       c.DataType not like concat(@open, '%')
    )
    /* ================================================================================================================
        Use the CTE multiple ways
            string_agg the select columns together
            get a JSON format Translator & mapping definition for ADF to use in writing the parquet destination
    ================================================================================================================ */
    select      wm.TableCatalog
            ,   wm.TableSchema
            ,   wm.TableName
            ,   wm.BatchId
            ,   wm.SubBatchId
            ,   concat_ws('/', 
                    wm.TableCatalog, wm.TableSchema, 
                    left(wm.TableName, len(wm.TableName) - 9),            /* Remove _YYYYMMDD */
                    substring(wm.TableName, len(wm.TableName) - 7, 4),    /* YYYY */
                    substring(wm.TableName, len(wm.TableName) - 3, 2),    /* MM */
                    right(wm.TableName, 2)                             /* DD */
                ) FilePath
            ,   concat_ws('.', wm.TableName, 'Batch', wm.BatchId, wm.SubBatchId, 'parquet') [FileName]
            ,   SelectCmd = concat('select ', string_agg(concat(src.Source, ' as ', src.Sink), ', ') within group (order by ColumnId, [position])
                                , ' from ', wm.TableSchema, '.', wm.TableName, wm.WhereClause)
    from        dbo.gbqWatermark wm
    join        cols src on src.TableCatalog = wm.TableCatalog and src.TableSchema = wm.TableSchema and src.TableName = wm.TableName
    where       wm.LoadedDateUtc is null 
    group by    wm.TableCatalog
            ,   wm.TableSchema
            ,   wm.TableName
            ,   wm.BatchId
            ,   wm.SubBatchId
            ,   wm.WhereClause
end

go
