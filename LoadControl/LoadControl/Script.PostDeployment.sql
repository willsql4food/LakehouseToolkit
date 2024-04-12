/*
Post-Deployment Script Template							
--------------------------------------------------------------------------------------
 This file contains SQL statements that will be appended to the build script.		
 Use SQLCMD syntax to include a file in the post-deployment script.			
 Example:      :r .\myfile.sql								
 Use SQLCMD syntax to reference a variable in the post-deployment script.		
 Example:      :setvar TableName MyTable							
               SELECT * FROM [$(TableName)]					
--------------------------------------------------------------------------------------
*/
if not exists(select * from dbo.adfPipelineExecution where Id = 0)
begin
	set identity_insert dbo.adfPipelineExecution on
	insert into	dbo.adfPipelineExecution (Id, PipelineName, RunId, StartTimeUtc) 
	select		0, 'Dummy', 'Dummy', '1901-01-01'
	set identity_insert dbo.adfPipelineExecution off
end
go

:r .\Scripts\GoogleAnalyticsParameters.sql