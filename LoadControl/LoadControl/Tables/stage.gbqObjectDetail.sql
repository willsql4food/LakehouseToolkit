CREATE TABLE stage.gbqObjectDetail
(
	TableCatalog	nvarchar(255) not null,
	TableSchema		nvarchar(255) not null,
	TableName		nvarchar(255) not null,
	BatchCol		nvarchar(2000) null,
	BatchTerm		nvarchar(2000) null,
	RowCountSource	int not null,
	SubBatchCol		nvarchar(2000) null,
	SubBatchMin		bigint null,
	SubBatchMax		bigint null
)
