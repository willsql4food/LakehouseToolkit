/* 
	Table to control load of data from Google Analytics / Big Query
*/
CREATE TABLE dbo.gbqWatermark
(
	Id						INT NOT NULL identity(1,1) PRIMARY KEY,
	TableCatalog			nvarchar(255) not null,
	TableSchema				nvarchar(255) not null,
	TableName				nvarchar(255) not null,
	BatchCol				nvarchar(255) null,
	BatchId					int not null default 0,
	WhereClause				nvarchar(max) not null default 'where 1 = 1',
	RowCountSource			int null,
	RowCountDest			int null,
	LoadedDateUTC			datetime null,
	DestinationUri			nvarchar(4000) null,
	SourceDeleted			bit null,
	PipelineExecutionId		int not null default 0,
	constraint fk_PipelineExecutionId__adfPipelineExecution foreign key (PipelineExecutionId) references dbo.adfPipelineExecution (Id),
	SysStart				DATETIME2 (7) GENERATED ALWAYS AS ROW START NOT NULL,
	SysEnd					DATETIME2 (7) GENERATED ALWAYS AS ROW END NOT NULL,
	PERIOD FOR SYSTEM_TIME (SysStart, SysEnd),
	SubBatchId				int null
)
WITH ( SYSTEM_VERSIONING = ON ( HISTORY_TABLE = history.gbqWatermark, DATA_CONSISTENCY_CHECK = ON))
