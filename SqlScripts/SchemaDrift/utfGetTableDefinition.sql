create or alter function dbo.uftGetTableDefinition(@SchemaName sysname, @TableName sysname, @IncludeHdrFtr bit)
    returns @TableDefinition table
    (
            Id int
        ,   SchemaName varchar(255)
        ,   TableName varchar(255)
        ,   ColumnName varchar(255)
        ,   ColumnId int
        ,   TemporalTypeId int default -1
        ,   TemporalType varchar(25) default 'n/a'
        ,   TypeName varchar(255)
        ,   DefinitionSql nvarchar(1000)
        ,   DefinitionHtml nvarchar(1000)
        ,   DefinitionMermaid nvarchar(1000)
    )
/* Function to return a table containing definitions of each column in the specified table 
    and the option to include header and footer information for HTML, Mermaid, etc. representation

Execution sample:
======================================================
declare @sch sysname = 'dbo', @tbl sysname = 'funky'

select      *
from        dbo.uftGetTableDefinition(@sch, @tbl, 1)
order by    Id
======================================================
*/
as
begin
    /* Get each column of the table and its definition */
    insert into @TableDefinition (Id, SchemaName, TableName, ColumnName, ColumnId, TemporalTypeId, TemporalType, TypeName, DefinitionSql, DefinitionHtml, DefinitionMermaid)
    select      c.column_id, s.name, t.name, c.name, c.column_id, c.generated_always_type, c.generated_always_type_desc, ty.name
            ,   concat(
                    c.name, ' ', ty.name
                    /* SQL definition (ex. "Id int NOT NULL") */
                ,   case    
                        when ty.name in ('char', 'varchar', 'varbinary', 'binary') then concat('(', case when c.max_length > 0 then convert(varchar, c.max_length) else 'MAX' end, ')')
                        when ty.name in ('nchar', 'nvarchar') then concat('(', case when c.max_length > 0 then  convert(varchar, c.max_length / 2) else 'MAX' end, ')')
                        when ty.name in ('time', 'datetime2', 'datetimeoffset') then concat('(', c.scale, ')')
                        when ty.name in ('float') then concat('(', c.[precision], ')')
                        when ty.name in ('decimal', 'numeric') then concat('(', c.[precision], ', ', c.scale, ')')
                    end
                ,   case when c.is_nullable = 0 then ' NOT' end, ' NULL'
                ) DefinitionSql
                /* HTML definition - a table row / item tagged column definiton */
            ,   concat(
                    '<tr><td>', convert(varchar, c.column_id), '</td><td>', c.name, '</td><td>', ty.name
                ,   case    
                        when ty.name in ('char', 'varchar', 'varbinary', 'binary') then concat('(', case when c.max_length > 0 then convert(varchar, c.max_length) else 'MAX' end, ')')
                        when ty.name in ('nchar', 'nvarchar') then concat('(', case when c.max_length > 0 then  convert(varchar, c.max_length / 2) else 'MAX' end, ')')
                        when ty.name in ('time', 'datetime2', 'datetimeoffset') then concat('(', c.scale, ')')
                        when ty.name in ('float') then concat('(', c.[precision], ')')
                        when ty.name in ('decimal', 'numeric') then concat('(', c.[precision], ', ', c.scale, ')')
                    end
                ,   '</td><td>'
                ,   case when ixc.column_id is null then 'False' else 'True' end, '</td><td>'
                ,   case when c.is_nullable = 0 then 'False' else 'True' end, '</td></tr>'
                ) DefinitionHtml
                /* Mermaid.js script for column's line in an ER Diagram */
            ,   concat(
                    ty.name
                ,   case    
                        when ty.name in ('char', 'varchar', 'varbinary', 'binary') then concat('[', case when c.max_length > 0 then  convert(varchar, c.max_length) else 'MAX' end, ']')
                        when ty.name in ('nchar', 'nvarchar') then concat('[', case when c.max_length > 0 then  convert(varchar, c.max_length / 2) else 'MAX' end, ']')
                        when ty.name in ('time', 'datetime2', 'datetimeoffset') then concat('[', c.scale, ']')
                        when ty.name in ('float') then concat('[', c.[precision], ']')
                        when ty.name in ('decimal', 'numeric') then concat('[', c.[precision], '_', c.scale, ']')
                    end
                ,   ' ', c.name, case when ixc.column_id is not null then ' PK' else ' ' end
                ,   ' "', case when c.is_nullable = 0 then 'NOT ' end, 'NULL"'
                ) DefinitionMermaid
    from        sys.schemas s
    join		sys.tables t on t.schema_id = s.schema_id
    join		sys.columns c on c.object_id = t.object_id
    join		sys.types ty on ty.user_type_id = c.user_type_id
    left join   sys.indexes ix on ix.object_id = t.object_id and ix.is_primary_key = 1
    left join   sys.index_columns ixc on ixc.object_id = t.object_id and ixc.index_id = ix.index_id and ixc.column_id = c.column_id
    where       s.name = @SchemaName and t.name = @TableName

    /* Add header and footer if requested */
    if (@IncludeHdrFtr = 1)
    begin
        /* Header */
        insert into @TableDefinition (Id, SchemaName, TableName, DefinitionSql, DefinitionHtml, DefinitionMermaid)
        select      0, s.name, t.name
                    /* SQL - bracketed table name */
                ,   concat('[', s.name, '].[', t.name, ']')
                    /* HTML - Header with table name, start a table with header row defining output columns */
                ,   concat('<H3>', s.name, '.', t.name, '</H3><table><tr><th>ColumnId</th><th>Name</th><th>Type</th><th>Is Primary Key</th><th>Nullable</th></tr>')
                    /* Mermaid.js - Start a <pre> tag and the enclosed erDiagram */
                ,   concat('<pre class="mermaid">erDiagram ', s.name, '_', t.name, ' {')
        from        sys.schemas s
        join		sys.tables t on t.schema_id = s.schema_id
        where       s.name = @SchemaName and t.name = @TableName

        /* Footer */
        insert into @TableDefinition (Id, SchemaName, TableName, DefinitionSql, DefinitionHtml, DefinitionMermaid)
        select      power(2, 16), s.name, t.name
                    /* SQL - nothing to do here */
                ,   ''
                    /* HTML - close out the table tag */
                ,   '</table>'
                    /* Mermaid.js - close code block and <pre> tag */
                ,   '} </pre>'
        from        sys.schemas s
        join		sys.tables t on t.schema_id = s.schema_id
        where       s.name = @SchemaName and t.name = @TableName
    end
    return
end

go


