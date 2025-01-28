CREATE TABLE [Config].[Dataset] (
    [Id]                 INT           IDENTITY (1, 1) NOT NULL,
    [DatasetName]        VARCHAR (255) NOT NULL,
    [LinkedServiceName]  VARCHAR (255) NOT NULL,
    [LinkedServiceType]  VARCHAR (255) NOT NULL,
    [ConnectionTemplate] VARCHAR (255) NOT NULL,
    [CreateDateUtc]      DATETIME2 (7) NOT NULL,
    [UpdateDateUtc]      DATETIME2 (7) NOT NULL
);
GO

ALTER TABLE [Config].[Dataset]
    ADD CONSTRAINT [DF_Config_Dataset_CreateDateUtc] DEFAULT (sysutcdatetime()) FOR [CreateDateUtc];
GO

ALTER TABLE [Config].[Dataset]
    ADD CONSTRAINT [DF_Config_Dataset_UpdateDateUtc] DEFAULT (sysutcdatetime()) FOR [UpdateDateUtc];
GO


CREATE TRIGGER Config.tru_Config_Dataset_UpdateDateUtc
ON Config.Dataset
FOR UPDATE

AS

IF NOT UPDATE(UpdateDateUtc)
BEGIN
	UPDATE o
	SET UpdateDateUtc = SYSUTCDATETIME()
	FROM Config.Dataset o
	JOIN inserted i ON o.Id = i.Id
END
GO

ALTER TABLE [Config].[Dataset]
    ADD CONSTRAINT [PK_Config_Dataset] PRIMARY KEY CLUSTERED ([Id] ASC);
GO

