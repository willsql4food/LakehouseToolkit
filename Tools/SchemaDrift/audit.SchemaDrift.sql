/* ====================================================================================================================
audit.SchemaDrift table for tracking changes made by schema drift process
    Author:		A. Carter Burleigh (ACB)
    Info:		See selfDoc: section
---	----------	-------------------------------------------------------------------------------------------------------
ACB	2023-10-20	Initial development
==================================================================================================================== */

/* ====================================================================================================================
    Assure needed schemas exist
==================================================================================================================== */
if not exists (select name from sys.schemas where name = 'history')
    execute sp_executesql N'Create schema history';
go

if not exists (select name from sys.schemas where name = 'audit')
    execute sp_executesql N'Create schema audit';
go

/* ====================================================================================================================
    Uncomment to remove audit.SchemaDrift for rebuild
==================================================================================================================== */
/*
if exists (select s.name sch, t.name tbl from sys.tables t join sys.schemas s on t.schema_id = s.schema_id where s.name = 'audit' and t.name = 'SchemaDrift' and t.temporal_type_desc = 'SYSTEM_VERSIONED_TEMPORAL_TABLE')
    if exists (select s.name sch, t.name tbl from sys.tables t join sys.schemas s on t.schema_id = s.schema_id where s.name = 'history' and t.name = 'SchemaDrift' and temporal_type_desc = 'HISTORY_TABLE')
    begin
        alter table audit.SchemaDrift set (system_versioning = off);
        drop table history.SchemaDrift;
        drop table audit.SchemaDrift;
    end

if exists (select s.name sch, t.name tbl from sys.tables t join sys.schemas s on t.schema_id = s.schema_id where s.name = 'history' and t.name = 'SchemaDrift')
    drop table history.SchemaDrift;

if exists (select s.name sch, t.name tbl from sys.tables t join sys.schemas s on t.schema_id = s.schema_id where s.name = 'audit' and t.name = 'SchemaDrift')
    drop table audit.SchemaDrift;
*/
/* ====================================================================================================================
    Create the table and its history table
==================================================================================================================== */
if not exists (select s.name sch, t.name tbl from sys.tables t join sys.schemas s on t.schema_id = s.schema_id where s.name = 'history' and t.name = 'SchemaDrift')
    create table history.SchemaDrift
        (   SchemaName      sysname
        ,   TableName       sysname
        ,   ColumnName      sysname
        ,   [Definition]    nvarchar(2000)
        ,   PriorDefinition nvarchar(2000)
        ,   ActionReason    varchar(50)
        ,   DdlProposed     nvarchar(2000)
        ,	SysStartTime    datetime2 not null
        ,	SysEndTime      datetime2 not null
        )

if not exists (select s.name sch, t.name tbl from sys.tables t join sys.schemas s on t.schema_id = s.schema_id where s.name = 'audit' and t.name = 'SchemaDrift')
    create table audit.SchemaDrift
        (   SchemaName      sysname
        ,   TableName       sysname
        ,   ColumnName      sysname
        ,   constraint pk_audit_SchemaDrift primary key clustered (SchemaName, TableName, ColumnName)
        ,   [Definition]    nvarchar(2000)
        ,   PriorDefinition nvarchar(2000)
        ,   ActionReason    varchar(50)
        ,   DdlProposed     nvarchar(2000)
        ,	SysStartTime    datetime2 generated always as row start not null
        ,	SysEndTime      datetime2 generated always as row end not null
	    ,	period for SYSTEM_TIME (SysStartTime, SysEndTime)
        )
        with (system_versioning = on (history_table = history.SchemaDrift))

    