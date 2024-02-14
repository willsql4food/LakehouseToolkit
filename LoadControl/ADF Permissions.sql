create user [df-sbx-ab092898] from external provider
go

grant execute to [df-sbx-ab092898]

alter role db_datawriter add member [df-sbx-ab092898]
alter role db_datareader add member [df-sbx-ab092898]
