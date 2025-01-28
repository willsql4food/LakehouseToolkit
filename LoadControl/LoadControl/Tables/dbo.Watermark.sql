/* ====================================================================================================================
	Table:		dbo.Watermark
	Author:		Chris Hatcher
---	----------	-------------------------------------------------------------------------------------------------------
ACB	2024-04-11	Scripted from DB for inclusion in DB Project / DACPAC deployment
==================================================================================================================== */
CREATE TABLE [dbo].[Watermark] (
    [Id]                        INT            IDENTITY (1, 1) NOT NULL,
    [BatchName]                 VARCHAR (255)  NOT NULL,
    [BatchType]                 VARCHAR (30)   NOT NULL,
    [BatchStep]                 SMALLINT       NOT NULL,
    [Environment]               VARCHAR (30)   NOT NULL,
    [SourceServiceType]         VARCHAR (60)   NOT NULL,
    [SourceName]                VARCHAR (255)  NOT NULL,
    [SourceConnection]          VARCHAR (2000) NULL,
    [SourceObjectName]          VARCHAR (255)  NOT NULL,
    [SourcePKFieldName]         VARCHAR (255)  NOT NULL,
    [SourcePKFieldValue]        VARCHAR (255)  NULL,
    [SourceWatermarkFieldName]  VARCHAR (255)  NOT NULL,
    [SourceWatermarkFieldValue] VARCHAR (255)  NULL,
    [SourceWatermarkDataType]   VARCHAR (255)  NOT NULL,
    [SourceWatermarkTimezone]   VARCHAR (30)   NULL,
    [SinkServiceType]           VARCHAR (60)   NOT NULL,
    [SinkName]                  VARCHAR (255)  NOT NULL,
    [SinkConnection]            VARCHAR (2000) NULL,
    [SinkObjectName]            VARCHAR (255)  NOT NULL,
    [IsActive]                  BIT            NOT NULL,
    [CreateDateUtc]             DATETIME2 (7)  CONSTRAINT [DF_Watermark_CreateDateUtc] DEFAULT (sysutcdatetime()) NULL,
    [UpdateDateUtc]             DATETIME2 (7)  CONSTRAINT [DF_Watermark_UpdateDateUtc] DEFAULT (sysutcdatetime()) NULL,
    [SourceQuery]               VARCHAR (2000) NULL,
    CONSTRAINT [PK_Watermark] PRIMARY KEY CLUSTERED ([Id] ASC)
);
go

/* ====================================================================================================================
	Update trigger for capturing Update date
==================================================================================================================== */
CREATE trigger [dbo].[tru_Watermark_UpdateDateUtc]
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
