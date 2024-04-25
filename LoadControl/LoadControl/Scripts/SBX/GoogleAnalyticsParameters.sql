/* ====================================================================================================================
Watermark and Parameters for GoogleAnalytics loading
	Author:		A. Carter Burleigh (ACB)
---	----------	-------------------------------------------------------------------------------------------------------
ACB	2024-04-12	Initial development
==================================================================================================================== */

/* ====================================================================================================================
	Clean up
==================================================================================================================== */
delete from dbo.Watermark where BatchName = 'Google Analytics to datalakehouse' and SourceName = 'n/a'

/* ====================================================================================================================
	Watermark entries
==================================================================================================================== */
merge into dbo.Watermark as tgt
using (
	values (
			'Google Analytics to datalakehouse', 'Pipeline_getGoogleAnalytics', 0, 'SBX'
		,	'Pipeline_getGoogleAnalytics', 'GoogleAnalytics'
		,	'{"parameters": [{"name": "dsDatabricksWorkspaceUrl", "value": "https://adb-7286372339506263.3.azuredatabricks.net"}
, {"name": "dsDatabricksWorkspaceId", "value": "/subscriptions/4c87027a-d0a7-4f47-b9c4-31447460fbef/resourceGroups/rg-sbx-ab092898/providers/Microsoft.Databricks/workspaces/dbx-sbx-ab092898"}]}'
		,	'n/a', 'n/a', 'n/a'
		,	'n/a', 'n/a', 'n/a', 'n/a'
		,	'Pipeline_getGoogleAnalytics', 'n/a'
		,	'{"parameters": [{"name": "dsURL", "value": "https://staab09289802.dfs.core.windows.net"}
, {"name": "dsSecretName", "value": "key-staab09289802"}
, {"name": "dsKVBaseURL", "value": "https://akv-092898.vault.azure.net/"}
, {"name": "dsStorageAccount", "value": "staab09289802"}
, {"name": "dsFileSystem", "value": "datalake"}
, {"name": "dsDirectory", "value": "bronze"}]}'
		,	'n/a', 1, 'n/a'
		)
	-- ,	(	
	--		'BatchName', 'BatchType', -1 /* BatchStep */, 'Environment'
	-- 	,	'SourceServiceType', 'SourceName', 'SourceConnection'
	--	,	'SourceObjectName', 'SourcePKFieldName', 'SourcePKFieldValue'
	-- 	,	'SourceWatermarkFieldName', 'SourceWatermarkFieldValue', 'SourceWatermarkDataType', 'SourceWatermarkTimezone'
	-- 	,	'SinkServiceType', 'SinkName'
	--	,	'SinkConnection'
	--	,	'SinkObjectName', 1 /* IsActive */, 'SourceQuery'
	-- 	)
	) as src (
		BatchName, BatchType, BatchStep, Environment
	,	SourceServiceType, SourceName, SourceConnection
	,	SourceObjectName, SourcePKFieldName, SourcePKFieldValue
	,	SourceWatermarkFieldName, SourceWatermarkFieldValue, SourceWatermarkDataType, SourceWatermarkTimezone
	,	SinkServiceType, SinkName, SinkConnection, SinkObjectName
	,	IsActive, SourceQuery
	) on	src.BatchName = tgt.BatchName and src.BatchType = tgt.BatchType and src.BatchStep = tgt.BatchStep and src.Environment = tgt.Environment
		and	src.SourceServiceType = tgt.SourceServiceType and src.SinkServiceType = tgt.SinkServiceType and src.SourceObjectName = tgt.SourceObjectName
when matched then 
	update
	set	SourceName = src.SourceName
	,	SourceConnection = src.SourceConnection
	,	SourcePKFieldName = src.SourcePKFieldName
	,	SourcePKFieldValue = src.SourcePKFieldValue
	,	SourceWatermarkFieldName = src.SourceWatermarkFieldName
	,	SourceWatermarkFieldValue = src.SourceWatermarkFieldValue
	,	SourceWatermarkDataType = src.SourceWatermarkDataType
	,	SourceWatermarkTimezone = src.SourceWatermarkTimezone
	,	SinkName = src.SinkName
	,	SinkConnection = src.SinkConnection
	,	SinkObjectName = src.SinkObjectName
	,	IsActive = src.IsActive
	,	SourceQuery = src.SourceQuery
when not matched by target then 
	insert (
		BatchName, BatchType, BatchStep, Environment
	,	SourceServiceType, SourceName, SourceConnection, SourceObjectName
	,	SourcePKFieldName, SourcePKFieldValue
	,	SourceWatermarkFieldName, SourceWatermarkFieldValue, SourceWatermarkDataType, SourceWatermarkTimezone
	,	SinkServiceType, SinkName, SinkConnection, SinkObjectName
	,	IsActive, SourceQuery
	)
	values (
		BatchName, BatchType, BatchStep, Environment
	,	SourceServiceType, SourceName, SourceConnection, SourceObjectName
	,	SourcePKFieldName, SourcePKFieldValue
	,	SourceWatermarkFieldName, SourceWatermarkFieldValue, SourceWatermarkDataType, SourceWatermarkTimezone
	,	SinkServiceType, SinkName, SinkConnection, SinkObjectName
	,	IsActive, SourceQuery
	)
;
go

/* ====================================================================================================================
	ParameterMap entries
==================================================================================================================== */
merge into dbo.ParameterMap as tgt
using (
	values (
			'Pipeline_getGoogleAnalytics'
		,	'dsDatabricksWorkspaceUrl, dsDatabricksWorkspaceId, dsURL, dsSecretName, dsKVBaseURL, dsStorageAccount, dsFileSystem, dsDirectory'
		,	'<Direction>dsDatabricksWorkspaceUrl varchar(255), <Direction>dsDatabricksWorkspaceId varchar(255), <Direction>dsURL varchar(255), <Direction>dsSecretName varchar(255), <Direction>dsKVBaseURL varchar(255), <Direction>dsStorageAccount varchar(255), <Direction>dsFileSystem varchar(255), <Direction>dsDirectory varchar(255)'
		,	1
		)
	-- ,	(
	-- 		'ServiceType'
	-- 	,	'ParameterString'
	-- 	,	'ParameterDefinitionString'
	-- 	,	0 /* IsActive */
	-- 	)
	) as src (ServiceType, ParameterString, ParameterDefinitionString, IsActive)
	on src.ServiceType = tgt.ServiceType
when matched then update
	set ParameterString = src.ParameterString
	,	ParameterDefinitionString = src.ParameterDefinitionString
	,	IsActive = src.IsActive
when not matched by target then 
	insert (ServiceType, ParameterString, ParameterDefinitionString, IsActive)
	values (ServiceType, ParameterString, ParameterDefinitionString, IsActive)
;
go
