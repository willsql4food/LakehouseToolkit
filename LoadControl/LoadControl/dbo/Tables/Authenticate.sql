CREATE TABLE [dbo].[Authenticate] (
    [Id]                 INT            IDENTITY (1, 1) NOT NULL,
    [BatchName]          VARCHAR (255)  NOT NULL,
    [Environment]        VARCHAR (30)   NOT NULL,
    [AuthenticateURL]    VARCHAR (1000) NOT NULL,
    [AuthenticateSecret] VARCHAR (1000) NULL,
    [KVBaseURL]          VARCHAR (1000) NULL,
    [CreateDateUtc]      DATETIME2 (7)  NOT NULL,
    [UpdateDateutc]      DATETIME2 (7)  NOT NULL
);
GO

ALTER TABLE [dbo].[Authenticate]
    ADD CONSTRAINT [PK_Authenticate] PRIMARY KEY CLUSTERED ([Id] ASC);
GO

ALTER TABLE [dbo].[Authenticate]
    ADD CONSTRAINT [DF_Authenticate_CreateDateUtc] DEFAULT (sysdatetime()) FOR [CreateDateUtc];
GO

ALTER TABLE [dbo].[Authenticate]
    ADD CONSTRAINT [DF_Authenticate_UpdateDateUtc] DEFAULT (sysdatetime()) FOR [UpdateDateutc];
GO

