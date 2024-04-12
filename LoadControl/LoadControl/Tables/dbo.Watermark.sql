/* ====================================================================================================================
	Table:		dbo.Watermark
	Author:		Chris Hatcher
---	----------	-------------------------------------------------------------------------------------------------------
ACB	2024-04-11	Scripted from DB for inclusion in DB Project / DACPAC deployment
==================================================================================================================== */
create table [dbo].[Watermark]
(
		[Id]							int identity(1,1) not null
	,	constraint [PK_Watermark] primary key clustered (Id)
	,	[BatchName]						varchar(255) not null
	,	[BatchType]						varchar(30) not null
	,	[BatchStep]						smallint not null
	,	[Environment]					varchar(30) not null
	,	[SourceServiceType]				varchar(60) not null
	,	[SourceName]					varchar(255) not null
	,	[SourceConnection]				varchar(2000) null
	,	[SourceObjectName]				varchar(255) not null
	,	[SourcePKFieldName]				varchar(255) not null
	,	[SourcePKFieldValue]			varchar(255) null
	,	[SourceWatermarkFieldName]		varchar(255) not null
	,	[SourceWatermarkFieldValue]		varchar(255) null
	,	[SourceWatermarkDataType]		varchar(255) not null
	,	[SourceWatermarkTimezone]		varchar(30) null
	,	[SinkServiceType]				varchar(60) not null
	,	[SinkName]						varchar(255) not null
	,	[SinkConnection]				varchar(2000) null
	,	[SinkObjectName]				varchar(255) not null
	,	[IsActive]						bit not null
	,	[CreateDateUtc]					datetime2(7) constraint [DF_Watermark_CreateDateUtc] default (sysutcdatetime()) null
	,	[UpdateDateUtc]					datetime2(7) constraint [DF_Watermark_UpdateDateUtc] default (sysutcdatetime()) null
	,	[SourceQuery]					varchar(2000) null
) on [PRIMARY]
go

/* ====================================================================================================================
	Update trigger for capturing Update date
==================================================================================================================== */
create trigger [dbo].[tru_Watermark_UpdateDateUtc]
	on [dbo].[Watermark]
	for update
	as
	if not update(UpdateDateUtc)
	begin
		update	o
		set 	o.UpdateDateUtc = SYSUTCDATETIME()
		from 	dbo.Watermark o
		join	inserted i on o.Id = i.Id
	end;

alter table [dbo].[Watermark] ENABLE trigger [tru_Watermark_UpdateDateUtc];
