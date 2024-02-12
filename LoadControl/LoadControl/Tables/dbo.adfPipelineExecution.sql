CREATE TABLE dbo.adfPipelineExecution
(
	Id				INT NOT NULL identity(1,1) PRIMARY KEY,
	PipelineName	nvarchar(255) not null,
	RunId			varchar(255) not null,
	StartTimeUtc	datetime2 (3) not null,
	EndTimeUtc		datetime2 (3) null,
	BatchCount		int null,
	StatusMessage	varchar(25) null,
	SysStart		DATETIME2 (7) GENERATED ALWAYS AS ROW START NOT NULL,
	SysEnd			DATETIME2 (7) GENERATED ALWAYS AS ROW END NOT NULL,
	PERIOD FOR SYSTEM_TIME (SysStart, SysEnd)
)
WITH ( SYSTEM_VERSIONING = ON ( HISTORY_TABLE = history.adfPipelineExecution, DATA_CONSISTENCY_CHECK = ON))
