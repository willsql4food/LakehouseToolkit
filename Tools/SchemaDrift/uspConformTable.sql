create or alter procedure dbo.uspConformTable 
/* =======================================================================================================================
dbo.uspConformTable
    Author:		A. Carter Burleigh (ACB)
    Info:		See selfDoc: section
---	----------	-------------------------------------------------------------------------------------------------------
ACB	2023-10-12	Initial development
======================================================================================================================= */
    (   @SourceSchema varchar(255)
    ,   @SourceTable varchar(255)
    ,   @TargetSchema varchar(255)
    ,   @TargetTable varchar(255)
    ,   @Options varchar(max) = '{"Perform DDL": 0, "Debug": 0}'
    ,   @Help bit = 0
    )
as
begin
    /* ====================================================================================================================
        Verify the source and target schema and table parameters resolve to existing tables
    ==================================================================================================================== */
    declare @srcSchema varchar(255), @srcTable varchar(255), @srcTblFullName varchar(550)
        ,   @tgtSchema varchar(255), @tgtTable varchar(255), @tgtTblFullName varchar(550)

    select  @srcTblFullName = concat(quotename(s.name), '.', quotename(t.name))
        ,   @srcSchema = s.name, @srcTable = t.name
    from    sys.schemas s 
    join    sys.tables t on t.schema_id = s.schema_id 
    where   s.name = @SourceSchema and t.name = @SourceTable

    select  @tgtTblFullName = concat(quotename(s.name), '.', quotename(t.name)) 
        ,   @tgtSchema = s.name, @tgtTable = t.name
    from    sys.schemas s 
    join    sys.tables t on t.schema_id = s.schema_id 
    where   s.name = @TargetSchema and t.name = @TargetTable

    /* Source Table does not exist... */
    if (@srcTblFullName is null)
    begin
        raiserror('ERROR: Specified Source table (%s.%s) does not exist', 10, 1, @SourceSchema, @SourceTable)
        return;
    end;

    /* Target Table does not exist... */
    if (@tgtTblFullName is null)
    begin
        raiserror('ERROR: Specified Target table (%s.%s) does not exist', 10, 2, @TargetSchema, @TargetTable)
        return;
    end;

    /* Source and Target Table are the same */
    if (@srcTblFullName = @tgtTblFullName)
    begin
        raiserror('ERROR: Source and Target table are the same (%s)', 10, 3, @tgtTblFullName)
        return;
    end;

    set nocount on;
    /* ===================================================================================================================
       Decode for various actions to take and descriptive messages to caller  
    =================================================================================================================== */
    declare @actions table ( ActionReason varchar(200), ActionMessage varchar(2000) )

    insert into @actions (ActionReason, ActionMessage)
    values  ('RPT.SAME',                '/* Source and target column definitions are identical */')
        ,   ('RPT.SMALLER_SOURCE',      '/* Target column is of same base type but allows more data (ex. int -> bigint; varchar(50) -> varchar(100)) */')

        ,   ('SQL.NO_SOURCE',           'Column does not exist in source, exclude from target insert')
        ,   ('SQL.PRIMARY_KEY',         'Target column is a primary key column.  No automatic modification supported - conversion provided')
        ,   ('SQL.NUM_TO_STRING',       'Target column is a string type but source is not - conversion provided')
        ,   ('SQL.ASCII_TO_UNICODE',    'Target column is ASCII but destination is unicode - conversion provided')
        
        ,   ('DDL.NO_DEST',             'Target column does not exist - either add it to the table or exclude it from the insert')
        ,   ('DDL.SMALLER_STRING',      'Target string is smaller than source - either alter target column or risk truncation')
        ,   ('DDL.SMALLER_NUMERIC',     'Target number / datetime is smaller than source - either alter target column or risk truncation')
        ,   ('DDL.NUM_TO_SMALL_STRING', 'Target column is a string type too small to accept source data - expand target AND conversion provided')
        ,   ('DDL.UNICODE_TO_ASCII',    'Source column is unicode but target is ASCII - either alter target column or perform cast and risk data loss')
        ,   ('DDL.PRECISION',           'Target column has lower precision than source - risk of truncation')
        ,   ('DDL.SCALE',               'Target column has lower scale than source - risk of truncation')
        ,   ('DDL.PRECISION_SCALE',     'Target column has lower precision or scale than source - risk of truncation')
        
        ,   ('ERR.FROM_STRING',         'Source column is a string type but target type is not - investigate')
        ,   ('ERR.UNKNOWN',             'Unknown case - investigate and enhance this stored procedure')

    /* ===================================================================================================================
       Table to quantify differences between tables 
    =================================================================================================================== */
    declare @comp table
        (   ColumnName varchar(255)
        ,   srcColumnId smallint
        ,   RequiresAction bit default 1
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
        ,   tgtIsPrimaryKey bit
        /* Differences in column attributes between source and target */
        ,   difType smallint
        ,   difMaxLength smallint
        ,   difPrecision smallint
        ,   difScale smallint

        /* Decisions and actions */
        ,   ColumnExists smallint
        ,   ActionReason varchar(2000)
        ,   DdlStmt varchar(2000)
        ,   SelectClauseNoDdl varchar(2000)
        ,   SelectClausePostDdl varchar(2000)
        );

    /* =============================================================================================================
        Compare the source and target at the column + type + modifier level
        Note on dif% columns:
            = 0 : no difference
            > 0 : target column is larger (ex. nvarchar(50) -> nvarchar(100))
            < 0 : target column is smaller and action needed (alter / exclude column, warning issued, etc.)
    ============================================================================================================= */
    insert into @comp ( ColumnName, srcColumnId
                    ,   tgtTypeName, tgtMaxStrLength, tgtPrecision, tgtScale, tgtIsPrimaryKey
                    ,   srcTypeName, srcMaxStrLength, srcPrecision, srcScale
                    ,   ColumnExists, difType, difMaxLength, difPrecision, difScale
                    ,   srcColDefinition, tgtColDefinition)
    select      quotename(coalesce(tgt.ColumnName, src.columnName)), src.Id
            ,   tgt.TypeName, tgt.max_string_length, tgt.[Precision], tgt.Scale, tgt.IsPrmaryKey
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
       Build actions to take 
    =================================================================================================================== */
    /* Short cut for most likely case - column is same in source and target */
    update      c 
    set         c.ActionReason = 'RPT.SAME'
    from        @comp c
    where       (abs(c.ColumnExists) + abs(c.difType) + abs(c.difMaxLength) + abs(c.difPrecision) + abs(c.difScale)) = 0

    /* More complex conditions */
    update      c
    set         c.ActionReason = 
                case
                    /* Source is string but target is something else - this cannot be handled automatically */
                    when    difType <> 0 and (srcTypeName like '%char' or srcTypeName like '%text') and tgtTypeName not like '%char' and tgtTypeName not like '%text'
                        then 'ERR.FROM_STRING'

                    /* Source is string but target is something else - this cannot be handled automatically */
                    when    tgtIsPrimaryKey = 1 and (difType <> 0 or difMaxLength > 0 or difPrecision > 0 or difScale > 0)
                        then 'SQL.PRIMARY_KEY'

                    /* Target column does not exist */
                    when    ColumnExists > 0 
                        then 'DDL.NO_DEST'

                    /* Going to a different precision and scale in decimal type */
                    when    srcTypeName in ('decimal', 'numeric') and tgtTypeName in ('decimal', 'numeric') and (difPrecision > 0 or difScale > 0) 
                        then 'DDL.PRECISION_SCALE'
                    
                    /* Going to a smaller numeric type */
                    when   ( difType = 0 or srcTypeName in ('real', 'float') and tgtTypeName in ('real', 'float'))
                        and difPrecision > 0 and difScale = 0
                        then 'DDL.PRECISION'

                    /* Going to a smaller scale in decimal type */
                    when   ( difType = 0 or srcTypeName in ('decimal', 'numeric', 'time', 'datetime2', 'datetimeoffset') and tgtTypeName in ('decimal', 'numeric', 'time', 'datetime2', 'datetimeoffset'))
                        and difPrecision >= 0 and difScale > 0
                        then 'DDL.SCALE'

                    /* Non-string data going to string field, but target is not wide enough */
                    when    difType <> 0 
                        and tgtTypeName like '%char'
                        and srcPrecision > tgtMaxStrLength 
                        then 'DDL.NUM_TO_SMALL_STRING'

                    /* Target string not wide enough */
                    when    difType = 0 and tgtPrecision = 0 and (difMaxLength > 0 and tgtMaxStrLength > 0 or tgtMaxStrLength > 0 and srcMaxStrLength < 0 )
                        or  difType <> 0 and tgtTypeName = 'n' + srcTypeName and srcMaxStrLength > tgtMaxStrLength 
                        then 'DDL.SMALLER_STRING'

                    /* Target numeric type is not wide enough */
                    when    difType <> 0 and difPrecision > 0
                        and (   srcTypeName like '%int' and tgtTypeName like '%int' 
                            or  srcTypeName like '%date%' and tgtTypeName like '%date%' 
                            or  srcTypeName like '%money%' and tgtTypeName like '%money%'
                            )
                        then 'DDL.SMALLER_NUMERIC'

                    /* Target numeric istype not wide enough */
                    when    difType <> 0 
                        and (   srcTypeName like 'n%har' and tgtTypeName like '[^n]%har' 
                            or  srcTypeName = 'ntext' and tgtTypeName = 'text'
                            )
                        then 'DDL.UNICODE_TO_ASCII'

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
                        or  difPrecision <= 0 and difScale <= 0 and srcTypeName in ('decimal', 'numeric') and tgtTypeName in ('decimal', 'numeric')
                        then 'RPT.SMALLER_SOURCE'

                    /****** As yet unhandled cases ******/
                    else 'ERR.UNKNOWN'
                end
    from        @comp c
    where       ActionReason is null

    /* ===================================================================================================================
        Build Select clause, DDL statement and log message based on action needed
    =================================================================================================================== */
    /* Report only - Cases where target is same or narrower */
    update      c
    set         c.DdlStmt = ''
            ,   c.SelectClauseNoDdl = c.ColumnName
            ,   c.SelectClausePostDdl = c.ColumnName
    from        @comp c
    where       c.ActionReason like 'RPT%'
        and     c.DdlStmt is null

    /* Simple - Source does not have the column, so empty string */
    update      c
    set         c.DdlStmt = ''
            ,   c.SelectClauseNoDdl = ''
            ,   c.SelectClausePostDdl = ''
    from        @comp c
    where       c.ActionReason = 'SQL.NO_SOURCE'
        and     c.DdlStmt is null

    /* Simple SQL - convert to string type to match target */
    update      c
    set         c.DdlStmt = ''
            ,   c.SelectClauseNoDdl = concat('convert(', tgtTypeName, 
                                        case 
                                            when tgtTypeName like '%char' then concat('(', tgtMaxStrLength, '), ')
                                            when tgtTypeName like '%text' then ', '
                                        end
                                    ,   ColumnName, ')')
            ,   c.SelectClausePostDdl = concat('convert(', tgtTypeName, 
                                        case 
                                            when tgtTypeName like '%char' then concat('(', tgtMaxStrLength, '), ')
                                            when tgtTypeName like '%text' then ', '
                                        end
                                    ,   ColumnName, ')'
                                    ,   case 
                                            when c.ActionReason = 'SQL.PRIMARY_KEY' then ' /* PRIMARY KEY field - no DDL performed */' 
                                            else ''
                                        end)
    from        @comp c
    where       c.ActionReason in ('SQL.NUM_TO_STRING', 'SQL.ASCII_TO_UNICODE', 'SQL.PRIMARY_KEY')
        and     c.DdlStmt is null

    /* Cases where changes to target are needed */
    update      c
    set         c.DdlStmt = 
                    case
                        when c.ActionReason = 'DDL.NO_DEST' 
                            then concat('alter table ', @tgtTblFullName, ' add ', c.srcColDefinition)
                        when c.ActionReason in ('DDL.SMALLER_STRING', 'DDL.SMALLER_NUMERIC')
                            then concat('alter table ', @tgtTblFullName, ' alter column ', c.srcColDefinition)
                        when c.ActionReason = 'DDL.NUM_TO_SMALL_STRING'
                            then concat('alter table ', @tgtTblFullName, ' alter column ', c.ColumnName, ' ', c.tgtTypeName, '(', c.srcPrecision, ') NULL')
                        when c.ActionReason = 'DDL.UNICODE_TO_ASCII'
                            then concat('alter table ', @tgtTblFullName, ' alter column ', c.ColumnName, ' ', c.srcTypeName, '(', (select max(val) from (values (c.srcPrecision), (c.tgtPrecision), (c.tgtMaxStrLength) ) v(val)), ') NULL')
                        when c.ActionReason = 'DDL.NUM_TO_SMALL_STRING'
                            then concat('alter table ', @tgtTblFullName, ' alter column ', c.ColumnName, ' ', c.tgtTypeName, '(', c.srcPrecision, ') NULL')
                        when c.ActionReason = 'DDL.PRECISION'
                            then concat('alter table ', @tgtTblFullName, ' alter column ', c.ColumnName, ' float(', c.srcPrecision, ') NULL')
                        when c.ActionReason = 'DDL.SCALE'
                            then concat('alter table ', @tgtTblFullName, ' alter column ', c.ColumnName, ' ', c.tgtTypeName, '(', c.srcScale, ') NULL')
                        when c.ActionReason = 'DDL.PRECISION_SCALE'
                            then concat('alter table ', @tgtTblFullName, ' alter column ', c.ColumnName, ' ', c.tgtTypeName, '(', (select concat(max(p), ', ', max(s)) from (values (c.srcPrecision, c.srcScale), (c.tgtPrecision, c.tgtScale)) v(p, s)), ') NULL')
                        else '???'
                    end
                /* If choosing not to perform DDL, supply a conversion */
            ,   c.SelectClauseNoDdl = 
                    case
                        when c.ActionReason = 'DDL.NO_DEST' 
                            then ''
                        else concat('convert(', replace(replace(c.tgtColDefinition, c.ColumnName, ''), ' NULL', ''), ', ', c.ColumnName, ')')
                    end
                /* Post DDL, destination column needs no conversion */
            ,   c.SelectClausePostDdl = c.ColumnName
    from        @comp c
    where       c.ActionReason like 'DDL%'
        and     c.DdlStmt is null

    update      c
    set         c.DdlStmt = '', c.SelectClauseNoDdl = '', c.SelectClausePostDdl = ''
    from        @comp c
    where       c.ActionReason not like 'DDL%'
        and     c.DdlStmt is null

    /* ===================================================================================================================
        "Perform DDL" is requested - make changes to target table and return appropriate insert statement
    =================================================================================================================== */
    if exists (select * from openjson(@Options) o where o.[value] = 1 and o.[key] = 'Perform DDL')
    begin
        /* Get the DDL to execute */
        declare @exec table 
            (   id int
            ,   ddl nvarchar(2000)
            ,   SchemaName sysname
            ,   TableName sysname
            ,   ColumnName sysname
            ,   PriorDefinition nvarchar(2000)
            ,   [Definition] nvarchar(2000)
            ,   ActionReason varchar(50)
            );

        insert into @exec (id, ddl, SchemaName, TableName, ColumnName, PriorDefinition, [Definition], ActionReason) 
        select      srcColumnId, DdlStmt, @tgtSchema, @tgtTable, ColumnName
                ,   tgtColDefinition, replace(replace(replace(replace(DdlStmt, 'alter table ', ''), @tgtTblFullName, ''), ' add ', ''), ' alter column ', '')
                ,   ActionReason
        from        @comp 
        where       DdlStmt <> '';

        /* If debugging, pass back all the execution statements to run... */
        if exists (select * from openjson(@Options) o where o.[value] = 1 and o.[key] = 'Debug')
            select 'DDL command to execute' DebugMsg, * from @exec order by ColumnName;

        /* SQL ForEach: Iterate over the changes needed, executing them and logging the action */
        declare @iter smallint;
        declare @cmd nvarchar(2000);
        while exists (select * from @exec)
        begin
            /* Get next and execute DDL */
            select @iter = min(id) from @exec;
            select @cmd = ddl from @exec where id = @iter;
            
            execute sp_executesql @cmd;

            /* Log the action taken - remove prior row if this column has been modified before */
            delete      d
            from        audit.SchemaDrift d
            join        @exec e on e.ColumnName = d.ColumnName and d.SchemaName = e.SchemaName and d.TableName = e.TableName
            where       e.id = @iter;

            insert into audit.SchemaDrift (SchemaName, TableName, ColumnName, PriorDefinition, [Definition], ActionReason)
            select      e.SchemaName, e.TableName, e.ColumnName, e.PriorDefinition, e.[Definition], e.ActionReason
            from        @exec e
            where       e.id = @iter;

            /* Remove this from steps to execute */
            delete from @exec where id = @iter;
        end

        /* Return an INSERT... FROM <source table> statement based on columns having been altered */
        select      InsertCmd = concat('insert into ', @tgtTblFullName, ' (', string_agg(ColumnName, ', ') within group (order by srcColumnId, ColumnName), ')')
                ,   SelectCmd = concat('select ', string_agg(SelectClausePostDdl, ', ') within group (order by srcColumnId, ColumnName), ' from ', @srcTblFullName)
        from        @comp
        where       srcColDefinition is not null
    end

    /* ====================================================================================================================
       "Perform DDL" is NOT requested - record suggested changes to target table and return insert...convert() statement
    ==================================================================================================================== */
    else
    begin
        /* Record proposed DDL changes 
            1 - delete any prior suggestion for this column which is different */
        delete      d
        from        audit.SchemaDrift d
        join        @comp c on d.ColumnName = c.ColumnName
        where       d.SchemaName = @tgtSchema 
            and     d.TableName = @tgtTable 
            and     c.DdlStmt <> ''
            and    (d.DdlProposed <> c.DdlStmt or d.DdlProposed is null)

        /*  2 - Add any suggested DDL which has not been suggested most recently */
        insert into audit.SchemaDrift (SchemaName, TableName, ColumnName, [Definition], ActionReason, DdlProposed)
        select      @tgtSchema, @tgtTable, c.ColumnName, c.tgtColDefinition, c.ActionReason, c.DdlStmt
        from        @comp c
        where       c.DdlStmt <> ''
            and     not exists (select * from audit.SchemaDrift where SchemaName = @tgtSchema and TableName = @tgtTable and ColumnName = c.ColumnName and DdlProposed = c.DdlStmt)

        /* Return an INSERT... FROM <source table> statement with convert logic for columns that have different definitions */
        select      InsertCmd = concat('insert into ', @tgtTblFullName, ' (', string_agg(ColumnName, ', ') within group (order by srcColumnId, ColumnName), ')')
                ,   SelectCmd = concat('select ', string_agg(SelectClauseNoDdl, ', ') within group (order by srcColumnId, ColumnName), ' from ', @srcTblFullName)
        from        @comp
        where       srcColDefinition is not null
            and     tgtColDefinition is not null
    end

    /* ===================================================================================================================
        Return Debug information if requested in @Options
    =================================================================================================================== */
    if exists (select * from openjson(@Options) o where o.[value] = 1 and o.[key] = 'Debug')
        select      c.ColumnName, c.ActionReason
                ,   c.srcColDefinition, '-->' [Change], c.tgtColDefinition
                ,   c.tgtTypeName, c.tgtMaxStrLength, c.difMaxLength, c.tgtPrecision, c.difPrecision, c.tgtScale, c.difScale
                ,   c.SelectClauseNoDdl, c.DdlStmt, c.SelectClausePostDdl
                ,   @Options Options, a.ActionMessage
        from        @comp c
        left join   @actions a on c.ActionReason = a.ActionReason
        order by    ActionReason, ColumnName;
end

go
