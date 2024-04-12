set ANSI_NULLS on
go
set QUOTED_IDENTIFIER on
go

/* ====================================================================================================================
	Table:		dbo.ParameterMap 
	Author:		Chris Hatcher
---	----------	-------------------------------------------------------------------------------------------------------
ACB	2024-04-11	Scripted from DB for inclusion in DB Project / DACPAC deployment
==================================================================================================================== */
create table [dbo].[ParameterMap]
(
		[Id]							int identity(1,1) not null
	,	constraint [PK_ParameterMap] primary key clustered (Id)
	,	[ServiceType]					varchar(60) null
	,	[ParameterString]				varchar(1000) null
	,	[ParameterDefinitionString]		varchar(2000) null
	,	[IsActive]						bit not null
	,	[CreateDateUtc]					datetime2(7) null
	,	constraint [DF_ParameterMap_CreateDateUtc]  default (sysutcdatetime()) for [CreateDateUtc]
	,	[UpdateDateUtc]					datetime2(7) null
	,	constraint [DF_ParameterMap_UpdateDateUtc]  default (sysutcdatetime()) for [UpdateDateUtc]
) on [PRIMARY]
go

/* ====================================================================================================================
	Update trigger for capturing Update date
==================================================================================================================== */
create trigger [dbo].[tru_ParameterMap_UpdateDateUtc]
	on [dbo].[ParameterMap]
	for update
	as
	if not update(UpdateDateUtc)
	begin
		update	o
		set		UpdateDateUtc = SYSUTCDATETIME()
		from	dbo.Watermark o
		join	inserted i on o.Id = i.Id
	end
go

alter table [dbo].[ParameterMap] ENABLE trigger [tru_ParameterMap_UpdateDateUtc]
go
