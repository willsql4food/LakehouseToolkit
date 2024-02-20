CREATE TABLE dbo.gbqColumn (
	Id              INT NOT NULL identity(1,1) PRIMARY KEY,
	TableCatalog		nvarchar(255) not null,
	TableSchema			nvarchar(255) not null,
	TableName				nvarchar(255) not null,
  ColumnName      nvarchar(255) not null,
  constraint uk_gbqColumn__TableCatalog__TableSchema__TableName__ColumnName unique (TableCatalog, TableSchema, TableName, ColumnName),
  ColumnId        int not null,
  DataType        nvarchar(max),
	SysStart				DATETIME2 (7) GENERATED ALWAYS AS ROW START NOT NULL,
	SysEnd					DATETIME2 (7) GENERATED ALWAYS AS ROW END NOT NULL,
	PERIOD FOR SYSTEM_TIME (SysStart, SysEnd)
)
WITH ( SYSTEM_VERSIONING = ON ( HISTORY_TABLE = history.gbqColumn, DATA_CONSISTENCY_CHECK = ON))


