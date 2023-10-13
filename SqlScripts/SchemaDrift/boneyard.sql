    /* List of actions to take to return to caller */
    declare @actionsDEP table 
        (   ColumnId int
        ,   SchemaName varchar(255)
        ,   TableName varchar(255)
        ,   ColumnName varchar(255)
        ,   Class varchar(255)
        ,   Subclass varchar(255)
        ,   ActionToTake varchar(255)
        ,   Operation varchar(2000)
        ,   PriorDefinition varchar(2000)
        )

    /* Show options selected 
    select      [key] OptName, [value] OptValue
    from        openjson(@Options)
    */

    /* =============================================================================================================
        Report
       ============================================================================================================= */
    /* ------------------------------------------------------------------------
       Columns defined identically in both tables
    ------------------------------------------------------------------------ */
    insert into @actionsDEP (ColumnId, SchemaName, TableName, ColumnName, Class, Subclass, ActionToTake, Operation, PriorDefinition)
    select      dst.ColumnId, dst.SchemaName, dst.TableName, dst.ColumnName, 'Report', 'Same', 'None'
            ,   concat('/* [', dst.ColumnName, '] esDiff between tables */')
            ,   ''
    from        dbo.uftGetTableDefinition(@srcSchema, @srcTable, 0) src
    full join   dbo.uftGetTableDefinition(@dstSchema, @dstTable, 0) dst on dst.ColumnName = src.ColumnName
    where       src.DefinitionSql = dst.DefinitionSql
    
    /* ------------------------------------------------------------------------
       Columns larger in destination table (no risk of data loss)
    ------------------------------------------------------------------------ */
    insert into @actionsDEP (ColumnId, SchemaName, TableName, ColumnName, Class, Subclass, ActionToTake, Operation, PriorDefinition)
    select      dst.ColumnId, dst.SchemaName, dst.TableName, dst.ColumnName, 'Report', 'Larger', 'None'
            ,   concat('/* [', dst.ColumnName, '] larger in destination table', src.DefinitionSql, ' < ', dst.DefinitionSql, ' */')
            ,   ''
    from        dbo.uftGetTableDefinition(@srcSchema, @srcTable, 0) src
    full join   dbo.uftGetTableDefinition(@dstSchema, @dstTable, 0) dst on dst.ColumnName = src.ColumnName
    where       src.max_length <= dst.max_length
        and     src.precision <= dst.precision
        and     src.scale <= dst.scale
        and   ( src.max_length + src.precision + src.scale) < (dst.max_length + dst.precision + dst.scale)
    
    /* =============================================================================================================
        Widen columns? 
       ============================================================================================================= */
    /* ------------------------------------------------------------------------
       Same character data type but need increased max_length
        Ex. varchar(50) -> varchar(100)
    ------------------------------------------------------------------------ */

    insert into @actionsDEP (ColumnId, SchemaName, TableName, ColumnName, Class, Subclass, ActionToTake, Operation, PriorDefinition)
    select      dst.ColumnId, dst.SchemaName, dst.TableName, dst.ColumnName, 'Widen', 'max_length', 'DML'
            ,   concat('alter table [', @dstSchema, '].[', @dstTable, '] alter column ', src.DefinitionSql)
            ,   dst.DefinitionSql
    from        dbo.uftGetTableDefinition(@srcSchema, @srcTable, 0) src
    full join   dbo.uftGetTableDefinition(@dstSchema, @dstTable, 0) dst on dst.ColumnName = src.ColumnName
    where       src.DefinitionSql != dst.DefinitionSql
        and     src.TypeName = dst.TypeName
        and     src.TypeName in ('char', 'nchar', 'varchar', 'nvarchar', 'binary', 'varbinary')
        and     src.max_length > dst.max_length

    /* ------------------------------------------------------------------------
       Smaller to larger integer or floating point type
        Ex. tinyint -> smallint -> int -> bigint
    ------------------------------------------------------------------------ */
    insert into @actionsDEP (ColumnId, SchemaName, TableName, ColumnName, Class, Subclass, ActionToTake, Operation, PriorDefinition)
    select      dst.ColumnId, dst.SchemaName, dst.TableName, dst.ColumnName, 'Widen', 'Larger Int', 'DML'
            ,   concat('alter table [', @dstSchema, '].[', @dstTable, '] alter column ', src.DefinitionSql)
            ,   dst.DefinitionSql
    from        dbo.uftGetTableDefinition(@srcSchema, @srcTable, 0) src
    full join   dbo.uftGetTableDefinition(@dstSchema, @dstTable, 0) dst on dst.ColumnName = src.ColumnName
            /* Option logic here */
    join        openjson(@Options) o on 1 = 1
    where       src.DefinitionSql != dst.DefinitionSql
        and   ( dst.TypeName in ('tinyint') and src.TypeName = 'smallint'
            or  dst.TypeName in ('tinyint', 'smallint') and src.TypeName = 'int'
            or  dst.TypeName in ('tinyint', 'smallint', 'int') and src.TypeName = 'bigint'
        )

/*
                ,   case 
                        when src.DefinitionSql is null then concat('alter table [', @srcSchema, '].[', @srcTable, '] add ', dst.DefinitionSql)
                        when src.DefinitionSql <> dst.DefinitionSql then concat('alter table [', @srcSchema, '].[', @srcTable, '] alter column ', dst.DefinitionSql)
                        else concat('/* [', src.ColumnName, '] esDiff between tables */')
                    end ActionToTake
*/

    /* Return results */
    select      *
    from        @actions
    order by    Class, Subclass
