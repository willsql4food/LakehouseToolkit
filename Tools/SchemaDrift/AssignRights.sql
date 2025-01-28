/* ====================================================================================================================
    Grant rights to the ADF account to execute the uspConformTable stored procedure, read and write data in all tables
==================================================================================================================== */
if not exists (select * from sys.sysusers where name = 'df-sbx-ab092898' )
    create user [df-sbx-ab092898] from external provider
go
grant execute to [df-sbx-ab092898]
alter role db_datareader add member [df-sbx-ab092898]
alter role db_datawriter add member [df-sbx-ab092898]
alter role db_ddladmin add member [df-sbx-ab092898]
