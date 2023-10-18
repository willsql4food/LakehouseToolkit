create or alter procedure dbo.uspConformTable 
/* =======================================================================================================================
dbo.uspConformTable
    Author:		A. Carter Burleigh (ACB)
    Info:		See selfDoc: section
---	----------	-------------------------------------------------------------------------------------------------------
ACB	2023-10-12	Initial development
======================================================================================================================= */
    (   @srcSchema varchar(255)
    ,   @srcTable varchar(255)
    ,   @tgtSchema varchar(255)
    ,   @tgtTable varchar(255)
    ,   @Options varchar(max) = '{"Perform DML": 0}'
    ,   @Help bit = 0
    )
as
begin
    /* ===================================================================================================================
       Table to quantify differences between tables 
    =================================================================================================================== */
    declare @comp table
        (   ColumnName varchar(255)
        ,   RequiresAction bit default 1

        ,   [-->] char(3) default '-->'
        ,   srcColDefinition varchar(2000)
        ,   tgtColDefinition varchar(2000)
        /* Source column attributes */
        ,   srcTypeName varchar(255)
        ,   srcMaxStrLength int
        ,   srcPrecision int
        ,   srcScale int
        /* Target column attributes */
        ,   tgtTypeName varchar(255)
        ,   tgtMaxStrLength int
        ,   tgtPrecision int
        ,   tgtScale int
        /* Differences in column attributes between source and target */
        ,   difType smallint
        ,   difMaxLength smallint
        ,   difPrecision smallint
        ,   difScale smallint

        /* Decisions and actions */
        ,   ColumnExists smallint
        ,   ActionPath varchar(2000)
        ,   DmlStmt varchar(2000)
        ,   SelectClauseNoDml varchar(2000)
        ,   SelectClausePostDml varchar(2000)
        );

    /* =============================================================================================================
        Compare the source and target at the column + type + modifier level
        Note on dif% columns:
            = 0 : no difference
            > 0 : target column is larger (ex. nvarchar(50) -> nvarchar(100))
            < 0 : target column is smaller and action needed (alter / exclude column, warning issued, etc.)
    ============================================================================================================= */
    insert into @comp ( ColumnName
                    ,   tgtTypeName, tgtMaxStrLength, tgtPrecision, tgtScale
                    ,   srcTypeName, srcMaxStrLength, srcPrecision, srcScale
                    ,   ColumnExists, difType, difMaxLength, difPrecision, difScale
                    ,   srcColDefinition, tgtColDefinition)
    select      quotename(coalesce(tgt.ColumnName, src.columnName)), tgt.TypeName, tgt.max_string_length, tgt.[Precision], tgt.Scale
            ,   src.TypeName, src.max_string_length, src.[Precision], src.Scale
            ,   case 
                    when src.ColumnName = tgt.ColumnName then 0 
                    when src.ColumnName is null then -1
                    when tgt.ColumnName is null then 1
                    else NULL 
                end
            ,   case 
                    when src.TypeName = tgt.TypeName then 0 
                    else 1
                end
            ,   src.max_string_length - tgt.max_string_length
            ,   src.[Precision] - tgt.[Precision], src.[Scale] - tgt.[Scale]
            ,   replace(src.DefinitionSql, 'NOT NULL', 'NULL'), replace(tgt.DefinitionSql, 'NOT NULL', 'NULL')
    from        dbo.uftGetTableDefinition(@tgtSchema, @tgtTable, 0) tgt
    full join   dbo.uftGetTableDefinition(@srcSchema, @srcTable, 0) src on src.ColumnName = tgt.ColumnName;

    /* ===================================================================================================================
       Decode for various actions to take and descriptive messages to caller  
    =================================================================================================================== */
    declare @actions table 
        (   ActionPath varchar(200)
        ,   ActionMessage varchar(2000)
        )

    insert into @actions (ActionPath, ActionMessage)
    values  ('RPT.SAME', '/* Source and target column definitions are identical */')
        ,   ('RPT.SMALLER_SOURCE', '/* Target column is of same base type but allows more data (ex. int -> bigint; varchar(50) -> varchar(100)) */')

        ,   ('SQL.NO_SOURCE', 'Column does not exist in source, exclude from target insert')
        ,   ('SQL.NUM_TO_STRING', 'Target column is a string type but source is not - conversion provided')
        ,   ('SQL.ASCII_TO_UNICODE', 'Target column is ASCII but destination is unicode - conversion provided')
        
        ,   ('DML.NO_DEST', 'Target column does not exist - either add it to the table or exclude it from the insert')
        ,   ('DML.SMALLER_STRING', 'Target string is smaller than source - either alter target column or risk truncation')
        ,   ('DML.SMALLER_NUMERIC', 'Target number / datetime is smaller than source - either alter target column or risk truncation')
        ,   ('DML.NUM_TO_SMALL_STRING', 'Target column is a string type too small to accept source data - expand target AND conversion provided')
        ,   ('DML.UNICODE_TO_ASCII', 'Source column is unicode but target is ASCII - either alter target column or perform cast and risk data loss')
        ,   ('DML.PRECISION', 'Target column has lower precision than source - risk of truncation')
        ,   ('DML.SCALE', 'Target column has lower scale than source - risk of truncation')
        ,   ('DML.PRECISION_SCALE', 'Target column has lower precision or scale than source - risk of truncation')
        
        ,   ('ERR.FROM_STRING', 'Source column is a string type but target type is not - investigate')
        ,   ('ERR.UNKNOWN', 'Unknown case - investigate and enhance this stored procedure')

    /* ===================================================================================================================
       Build actions to take 
    =================================================================================================================== */
    /* Short cut for most likely case - column is same in source and target */
    update      c 
    set         c.ActionPath = 'RPT.SAME'
    from        @comp c
    where       (abs(c.ColumnExists) + abs(c.difType) + abs(c.difMaxLength) + abs(c.difPrecision) + abs(c.difScale)) = 0

    /* More complex conditions */
    update      c
    set         c.ActionPath = 
                case
                    /* Source is string but target is something else - this cannot be handled automatically */
                    when    difType <> 0 and (srcTypeName like '%char' or srcTypeName like '%text') and tgtTypeName not like '%char' and tgtTypeName not like '%text'
                        then 'ERR.FROM_STRING'

                    /* Target column does not exist */
                    when    ColumnExists > 0 
                        then 'DML.NO_DEST'

                    /* Going to a different precision and scale in decimal type */
                    when    srcTypeName in ('decimal', 'numeric') and tgtTypeName in ('decimal', 'numeric') and (difPrecision > 0 or difScale > 0) 
                        then 'DML.PRECISION_SCALE'
                    
                    /* Going to a smaller numeric type */
                    when   ( difType = 0 or srcTypeName in ('real', 'float') and tgtTypeName in ('real', 'float'))
                        and difPrecision > 0 and difScale = 0
                        then 'DML.PRECISION'

                    /* Going to a smaller scale in decimal type */
                    when   ( difType = 0 or srcTypeName in ('decimal', 'numeric', 'time', 'datetime2', 'datetimeoffset') and tgtTypeName in ('decimal', 'numeric', 'time', 'datetime2', 'datetimeoffset'))
                        and difPrecision >= 0 and difScale > 0
                        then 'DML.SCALE'

                    /* Non-string data going to string field, but target is not wide enough */
                    when    difType <> 0 
                        and srcTypeName not like '%char' and srcTypeName not like '%text'
                        and (tgtTypeName like '%char' or tgtTypeName like '%text') 
                        and srcPrecision > tgtMaxStrLength 
                        then 'DML.NUM_TO_SMALL_STRING'

                    /* Target string not wide enough */
                    when    difType = 0 and tgtPrecision = 0 and (difMaxLength > 0 and tgtMaxStrLength > 0 or tgtMaxStrLength > 0 and srcMaxStrLength < 0 )
                        or  difType <> 0 and tgtTypeName = 'n' + srcTypeName and srcMaxStrLength > tgtMaxStrLength 
                        then 'DML.SMALLER_STRING'

                    /* Target numeric istype not wide enough */
                    when    difType <> 0 and difPrecision > 0
                        and (   srcTypeName like '%int' and tgtTypeName like '%int' 
                            or  srcTypeName like '%date%' and tgtTypeName like '%date%' 
                            or  srcTypeName like '%money%' and tgtTypeName like '%money%'
                            )
                        then 'DML.SMALLER_NUMERIC'

                    /* Target numeric istype not wide enough */
                    when    difType <> 0 
                        and (   srcTypeName like 'n%har' and tgtTypeName like '[^n]%har' 
                            or  srcTypeName = 'ntext' and tgtTypeName = 'text'
                            )
                        then 'DML.UNICODE_TO_ASCII'

                    /* Target is a string type wide enough to contain source which is numeric (int, float, date, etc.)  */
                    when    difType <> 0 
                        and (tgtTypeName like '%char' or tgtTypeName like '%text') 
                        and srcTypeName not like '%char' and srcTypeName not like '%text'
                        and srcPrecision <= tgtMaxStrLength 
                        then 'SQL.NUM_TO_STRING'

                    /* Target is a unicode string type wide enough to contain source which an ascii string of same length or smaller */
                    when    difType <> 0 
                        and tgtTypeName = 'n' + srcTypeName
                        and srcMaxStrLength <= tgtMaxStrLength 
                        then 'SQL.ASCII_TO_UNICODE'

                    /* Source column does not exist */
                    when    ColumnExists < 0 
                        then 'SQL.NO_SOURCE'
    
                    /* Same string types with bigger target max length */
                    when    difType = 0 and (difMaxLength < 0 and srcMaxStrLength > 0) and tgtPrecision = 0
                            /* Assure same numeric 'family' and are moving to wider field */
                        or  difPrecision < 0 and ( srcTypeName like '%int' and tgtTypeName like '%int' 
                                                or  srcTypeName like '%date%' and tgtTypeName like '%date%' 
                                                or  srcTypeName like '%time%' and tgtTypeName like '%time%'
                                                or  srcTypeName like '%money%' and tgtTypeName like '%money%'
                                                or  srcTypeName = 'real' and tgtTypeName = 'float'
                                                )
                        or  difPrecision < 0 and difScale < 0 and ( srcTypeName in ('decimal', 'numeric') and tgtTypeName in ('decimal', 'numeric'))
                        then 'RPT.SMALLER_SOURCE'

                    /****** As yet unhandled cases ******/
                    else 'ERR.UNKNOWN'
                end
    from        @comp c
    where       ActionPath is null

    /* ===================================================================================================================
        Build Select clause, DML statement and log message based on action needed
    =================================================================================================================== */
    declare @TargetTable varchar(550)
    select  @TargetTable = concat(quotename(s.name), '.', quotename(t.name)) 
    from    sys.schemas s 
    join    sys.tables t on t.schema_id = s.schema_id 
    where   s.name = @tgtSchema and t.name = @tgtTable
    
    /* Report only - Cases where target is same or narrower */
    update      c
    set         c.DmlStmt = ''
            ,   c.SelectClauseNoDml = c.ColumnName
            ,   c.SelectClausePostDml = c.ColumnName
    from        @comp c
    where       c.ActionPath like 'RPT%'
        and     c.DmlStmt is null

    /* Simple - Source does not have the column, so empty string */
    update      c
    set         c.DmlStmt = ''
            ,   c.SelectClauseNoDml = ''
            ,   c.SelectClausePostDml = ''
    from        @comp c
    where       c.ActionPath = 'SQL.NO_SOURCE'
        and     c.DmlStmt is null

    /* Simple SQL - convert to string type to match target */
    update      c
    set         c.DmlStmt = ''
            ,   c.SelectClauseNoDml = concat('convert(', tgtTypeName, '(', tgtMaxStrLength, '), ', c.ColumnName, ')')
            ,   c.SelectClausePostDml = concat('convert(', tgtTypeName, '(', tgtMaxStrLength, '), ', c.ColumnName, ')')
    from        @comp c
    where       c.ActionPath in ('SQL.NUM_TO_STRING', 'SQL.ASCII_TO_UNICODE')
        and     c.DmlStmt is null

    /* Cases where changes to target are needed */
    update      c
    set         c.DmlStmt = 
                    case
                        when c.ActionPath = 'DML.NO_DEST' 
                            then concat('alter table ', @TargetTable, ' add ', c.srcColDefinition)
                        when c.ActionPath in ('DML.SMALLER_STRING', 'DML.SMALLER_NUMERIC')
                            then concat('alter table ', @TargetTable, ' alter column ', c.srcColDefinition)
                        when c.ActionPath = 'DML.NUM_TO_SMALL_STRING'
                            then concat('alter table ', @TargetTable, ' alter column ', c.ColumnName, ' ', c.tgtTypeName, '(', c.srcPrecision, ') NULL')
                        when c.ActionPath = 'DML.UNICODE_TO_ASCII'
                            then concat('alter table ', @TargetTable, ' alter column ', c.ColumnName, ' ', c.srcTypeName, '(', (select max(val) from (values (c.srcPrecision), (c.tgtPrecision), (c.tgtMaxStrLength) ) v(val)), ') NULL')
                        when c.ActionPath = 'DML.NUM_TO_SMALL_STRING'
                            then concat('alter table ', @TargetTable, ' alter column ', c.ColumnName, ' ', c.tgtTypeName, '(', c.srcPrecision, ') NULL')
                        when c.ActionPath = 'DML.PRECISION'
                            then concat('alter table ', @TargetTable, ' alter column ', c.ColumnName, ' ', c.tgtTypeName, '(', c.srcPrecision, ') NULL')
                        when c.ActionPath = 'DML.SCALE'
                            then concat('alter table ', @TargetTable, ' alter column ', c.ColumnName, ' ', c.tgtTypeName, '(', c.srcScale, ') NULL')
                        when c.ActionPath = 'DML.PRECISION_SCALE'
                            then concat('alter table ', @TargetTable, ' alter column ', c.ColumnName, ' ', c.tgtTypeName, '(', (select concat(max(p), ', ', max(s)) from (values (c.srcPrecision, c.srcScale), (c.tgtPrecision, c.tgtScale)) v(p, s)), ') NULL')
                        else '???'
                    end
                /* If choosing not to perform DML, supply a conversion */
            ,   c.SelectClauseNoDml = 
                    case
                        when c.ActionPath = 'DML.NO_DEST' 
                            then ''
                        else concat('convert(', replace(replace(c.tgtColDefinition, c.ColumnName, ''), ' NULL', ''), ', ', c.ColumnName, ')')
                    end
                /* Post DML, destination column needs no conversion */
            ,   c.SelectClausePostDml = c.ColumnName
    from        @comp c
    where       c.ActionPath like 'DML%'
        and     c.DmlStmt is null

    update      c
    set         c.DmlStmt = '', c.SelectClauseNoDml = '', c.SelectClausePostDml = ''
    from        @comp c
    where       c.ActionPath not like 'DML%'
        and     c.DmlStmt is null

    /* ===================================================================================================================
       Return results - adhere to options specified but always return ERR actions
    =================================================================================================================== */
    select      c.ColumnName, c.ActionPath, a.ActionMessage
            ,   c.SelectClauseNoDml, c.DmlStmt, c.SelectClausePostDml
            ,   c.srcColDefinition, [-->], c.tgtColDefinition

            ,   c.tgtTypeName, c.tgtMaxStrLength, c.difMaxLength, c.tgtPrecision, c.difPrecision, c.tgtScale, c.difScale
    from        @comp c
    left join   @actions a on c.ActionPath = a.ActionPath
    where exists (  select  * 
                    from    openjson(@Options) o 
                    where   o.[value] = 1 
                        and (   o.[key] = 'ALL'
                            or  charindex(o.[key], c.ActionPath collate Latin1_General_BIN2) = 1 
                            )
                )
        or      c.ActionPath like 'ERR%'
    order by    ActionPath, ColumnName;
end

go

