SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO

CREATE PROCEDURE [dbo].[GetWatermark]
 	@BatchName varchar(255),
	@SourceServiceType varchar(60),
	@SinkServiceType varchar(60),
	@Environment varchar(30),
	@RollingDays int = NULL,
	@WatermarkTimestamp datetime = NULL

AS

SET NOCOUNT ON 

DECLARE @sql nvarchar(MAX),
	@spid int

BEGIN TRY
	BEGIN TRAN
		SELECT @sql = N'CREATE TABLE ##SourceParameters (SrcSourceObjectName varchar(255), ' + REPLACE(ParameterDefinitionString, '<Direction>', 'Source') +')' FROM dbo.ParameterMap WHERE ServiceType = @SourceServiceType
		EXECUTE sp_executesql @sql

		UPDATE ##SourceParameters WITH (TABLOCK, XLOCK, HOLDLOCK) SET SrcSourceObjectName = '' 
	--	SELECT * INTO #SourceParameters FROM ##SourceParameters WITH (XLOCK, HOLDLOCK)
	--	DROP TABLE ##SourceParameters
	--COMMIT

	SELECT @sql = ''

	--BEGIN TRAN
		SELECT @sql = N'CREATE TABLE ##SinkParameters (SnkSourceObjectName varchar(255), ' + REPLACE(ParameterDefinitionString, '<Direction>', 'Sink') +')' FROM dbo.ParameterMap WHERE ServiceType = @SinkServiceType
		EXECUTE sp_executesql @sql

		UPDATE ##SinkParameters WITH (TABLOCK, XLOCK, HOLDLOCK) SET SnkSourceObjectName = ''
	-- SELECT * INTO #SinkParameters FROM ##SinkParameters WITH (XLOCK, HOLDLOCK)
--		DROP TABLE ##SinkParameters
--	COMMIT
--END TRY

-- Source 
		DECLARE csrJSON CURSOR LOCAL FORWARD_ONLY FOR 
			SELECT 'Source' AS Direction, w.SourceServiceType, w.SourceConnection, w.SourceObjectName, pm.ParameterString, pm.ParameterDefinitionString
			FROM dbo.Watermark w
			JOIN dbo.ParameterMap pm ON w.SourceServiceType = pm.ServiceType
			WHERE w.BatchName = @BatchName 
				AND w.SourceServiceType = @SourceServiceType
				AND w.Environment = @Environment
			UNION ALL
			SELECT 'Sink' AS Direction, w.SinkServiceType, w.SinkConnection, w.SourceObjectName, pm.ParameterString, pm.ParameterDefinitionString
			FROM dbo.Watermark w
			JOIN dbo.ParameterMap pm ON w.SinkServiceType = pm.ServiceType
			WHERE w.BatchName = @BatchName 
				AND w.SinkServiceType = @SinkServiceType
				AND w.Environment = @Environment

		DECLARE @Direction varchar(10),
			@ServiceType varchar(60),
			@Connection varchar(2000),
			@ObjectName varchar(255),
			@ParameterString varchar(1000),
			@ParameterDefinitionString varchar(2000)

		OPEN csrJSON	

		FETCH NEXT FROM csrJSON INTO @Direction, @ServiceType, @Connection, @ObjectName, @ParameterString, @ParameterDefinitionString

		WHILE @@FETCH_STATUS = 0
		BEGIN
		--	SELECT @Direction, @ServiceType, @Connection, @ObjectName, @ParameterString, @ParameterDefinitionString

			SELECT @sql = N'INSERT INTO ##' + @Direction + 'Parameters SELECT @ObjectName, ' + @ParameterString + -- dsURL, dsSecretName, dsFileSystem, dsDirectory, dsFileName, dsKVBaseURL
		'	FROM
				(SELECT ServiceType, SourceObjectName, name, value FROM
					(SELECT @ServiceType ServiceType, @ObjectName SourceObjectName, * FROM OPENJSON (@JSON, ''$.parameters'') WITH (name varchar(30), value varchar(200)) as params) a) pvt
			PIVOT
				(MAX(value) FOR name IN (' + @ParameterString + ')) pvt' --dsURL, dsSecretName, dsFileSystem, dsDirectory, dsFileName, dsKVBaseURL)) pvt'

			EXECUTE sp_executesql @sql, N'@ServiceType varchar(60), @ObjectName varchar(255), @JSON varchar(2000)', @ServiceType, @ObjectName, @Connection

			FETCH NEXT FROM csrJSON INTO @Direction, @ServiceType, @Connection, @ObjectName, @ParameterString, @ParameterDefinitionString
		END

		CLOSE csrJSON
		DEALLOCATE csrJSON


		SELECT Id AS WatermarkId, BatchType, SourceName, SourceConnection, w.SourceObjectName, SourcePKFieldName, SourceWatermarkFieldName, SourceWatermarkFieldValue, SourceWatermarkTimezone, SinkName, SinkConnection, w.SinkObjectName, 
			REPLACE(REPLACE(w.SourceQuery, '<varWatermarkTimestamp>', @WatermarkTimestamp), '<prmRollingDays>', @RollingDays) SourceQuery, 
			srcp.*,
			snkp.*
		FROM dbo.Watermark w
		JOIN ##SourceParameters srcp ON w.SourceObjectName = srcp.SrcSourceObjectName
		JOIN ##SinkParameters snkp ON w.SourceObjectName = snkp.SnkSourceObjectName
		WHERE Environment = @Environment
			AND IsActive = 1
			AND BatchName = @BatchName
			AND SourceServiceType = @SourceServiceType
			AND SinkServiceType = @SinkServiceType

		DROP TABLE ##SourceParameters
		DROP TABLE ##SinkParameters
	COMMIT

END TRY
BEGIN CATCH
	select ERROR_MESSAGE()
	WHILE @@TRANCOUNT > 0
	BEGIN
		ROLLBACK
	END

	

	DROP TABLE ##SourceParameters
	DROP TABLE ##SinkParameters

	RETURN
END CATCH
GO
