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

IF NOT EXISTS (SELECT BatchName FROM dbo.Watermark WHERE BatchName = @BatchName)
BEGIN
	RAISERROR('%s is not a valid batch name.', 16, 1, @BatchName)
	RETURN
END

IF NOT EXISTS (SELECT BatchName FROM dbo.Watermark WHERE BatchName = @BatchName AND SourceServiceType = @SourceServiceType)
BEGIN
	RAISERROR('%s is not a valid Service Type for batch "%s".', 16, 1, @SourceServiceType, @BatchName)
	RETURN
END

IF NOT EXISTS (SELECT BatchName FROM dbo.Watermark WHERE BatchName = @BatchName AND SinkServiceType = @SinkServiceType)
BEGIN
	RAISERROR('%s is not a valid Service Type for batch "%s".', 16, 1, @SinkServiceType, @BatchName)
	RETURN
END

BEGIN TRY
	BEGIN TRAN
		SELECT @sql = N'CREATE TABLE ##SourceParameters (SrcSourceObjectName varchar(255), ' + REPLACE(ParameterDefinitionString, '<Direction>', 'Source') +')' FROM dbo.ParameterMap WHERE ServiceType = @SourceServiceType
		EXECUTE sp_executesql @sql

		UPDATE ##SourceParameters WITH (TABLOCK, XLOCK, HOLDLOCK) SET SrcSourceObjectName = '' 

	SELECT @sql = ''

		SELECT @sql = N'CREATE TABLE ##SinkParameters (SnkSourceObjectName varchar(255), ' + REPLACE(ParameterDefinitionString, '<Direction>', 'Sink') +')' FROM dbo.ParameterMap WHERE ServiceType = @SinkServiceType
		EXECUTE sp_executesql @sql

		UPDATE ##SinkParameters WITH (TABLOCK, XLOCK, HOLDLOCK) SET SnkSourceObjectName = ''

-- Source 
		DECLARE csrJSON CURSOR LOCAL FORWARD_ONLY FOR 
			SELECT 'Source' AS Direction, w.SourceServiceType, w.SourceConnection, w.SourceObjectName, pm.ParameterString, pm.ParameterDefinitionString, w.SourceWatermarkFieldValue
			FROM dbo.Watermark w
			JOIN dbo.ParameterMap pm ON w.SourceServiceType = pm.ServiceType
			WHERE w.BatchName = @BatchName 
				AND w.SourceServiceType = @SourceServiceType
				AND w.Environment = @Environment
			UNION ALL
			SELECT 'Sink' AS Direction, w.SinkServiceType, w.SinkConnection, w.SourceObjectName, pm.ParameterString, pm.ParameterDefinitionString, ''
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
			@ParameterDefinitionString varchar(2000),
			@WatermarkFieldValue varchar(30),
			@WatermarkFieldValueString varchar(30),
			@WatermarkTimestampString varchar(30)

		OPEN csrJSON	

		FETCH NEXT FROM csrJSON INTO @Direction, @ServiceType, @Connection, @ObjectName, @ParameterString, @ParameterDefinitionString, @WatermarkFieldValue

		WHILE @@FETCH_STATUS = 0
		BEGIN
--select @Connection

--SELECT REPLACE(@Connection, '<SourceWatermarkFieldValue>', CONVERT(varchar, YEAR(@WatermarkFieldValue)) + '-' +
--	RIGHT('00' + CONVERT(varchar, MONTH(@WatermarkFieldValue)), 2) + '-' + 
--	RIGHT('00' + CONVERT(varchar, DAY(@WatermarkFieldValue)), 2) + 'T' +
--	RIGHT('00' + CONVERT(varchar, DATEPART(HOUR, @WatermarkFieldValue)), 2) + ':' +
--	RIGHT('00' + CONVERT(varchar, DATEPART(MINUTE, @WatermarkFieldValue)), 2) + ':' +
--	RIGHT('00' + CONVERT(varchar, DATEPART(SECOND, @WatermarkFieldValue)), 2) + 'Z')	

--SELECT ISNULL(REPLACE(@Connection, '<WatermarkTimestamp>', CONVERT(varchar, YEAR(@WatermarkTimestamp)) + '-' +
--	RIGHT('00' + CONVERT(varchar, MONTH(@WatermarkTimestamp)), 2) + '-' + 
--	RIGHT('00' + CONVERT(varchar, DAY(@WatermarkTimestamp)), 2) + 'T' +
--	RIGHT('00' + CONVERT(varchar, DATEPART(HOUR, @WatermarkTimestamp)), 2) + ':' +
--	RIGHT('00' + CONVERT(varchar, DATEPART(MINUTE, @WatermarkTimestamp)), 2) + ':' +
--	RIGHT('00' + CONVERT(varchar, DATEPART(SECOND, @WatermarkTimestamp)), 2) + 'Z'), '')

			SET DATEFORMAT ymd

			IF ISDATE(@WatermarkFieldValue) = 1
			BEGIN
				SELECT @Connection = ISNULL(REPLACE(@Connection, '<SourceWatermarkFieldValue>', CONVERT(varchar, YEAR(@WatermarkFieldValue)) + '-' +
				RIGHT('00' + CONVERT(varchar, MONTH(@WatermarkFieldValue)), 2) + '-' + 
				RIGHT('00' + CONVERT(varchar, DAY(@WatermarkFieldValue)), 2) + 'T' +
				RIGHT('00' + CONVERT(varchar, DATEPART(HOUR, @WatermarkFieldValue)), 2) + ':' +
				RIGHT('00' + CONVERT(varchar, DATEPART(MINUTE, @WatermarkFieldValue)), 2) + ':' +
				RIGHT('00' + CONVERT(varchar, DATEPART(SECOND, @WatermarkFieldValue)), 2) + 'Z'), '')
			END

			IF @WatermarkTimestamp IS NOT NULL
			BEGIN
				SELECT @Connection = REPLACE(@Connection, '<WatermarkTimestamp>', CONVERT(varchar, YEAR(@WatermarkTimestamp)) + '-' +
					RIGHT('00' + CONVERT(varchar, MONTH(@WatermarkTimestamp)), 2) + '-' + 
					RIGHT('00' + CONVERT(varchar, DAY(@WatermarkTimestamp)), 2) + 'T' +
					RIGHT('00' + CONVERT(varchar, DATEPART(HOUR, @WatermarkTimestamp)), 2) + ':' +
					RIGHT('00' + CONVERT(varchar, DATEPART(MINUTE, @WatermarkTimestamp)), 2) + ':' +
					RIGHT('00' + CONVERT(varchar, DATEPART(SECOND, @WatermarkTimestamp)), 2) + 'Z')
			END

			SELECT @sql = N'INSERT INTO ##' + @Direction + 'Parameters SELECT @ObjectName, ' + @ParameterString + -- dsURL, dsSecretName, dsFileSystem, dsDirectory, dsFileName, dsKVBaseURL
		'	FROM
				(SELECT ServiceType, SourceObjectName, name, value FROM
					(SELECT @ServiceType ServiceType, @ObjectName SourceObjectName, * FROM OPENJSON (@JSON, ''$.parameters'') WITH (name varchar(30), value varchar(200)) as params) a) pvt
			PIVOT
				(MAX(value) FOR name IN (' + @ParameterString + ')) pvt' --dsURL, dsSecretName, dsFileSystem, dsDirectory, dsFileName, dsKVBaseURL)) pvt'

			EXECUTE sp_executesql @sql, N'@ServiceType varchar(60), @ObjectName varchar(255), @JSON varchar(2000)', @ServiceType, @ObjectName, @Connection

			FETCH NEXT FROM csrJSON INTO @Direction, @ServiceType, @Connection, @ObjectName, @ParameterString, @ParameterDefinitionString, @WatermarkFieldValue
		END

		CLOSE csrJSON
		DEALLOCATE csrJSON
/*
		IF @SourceServiceType LIKE 'RestService%'
		BEGIN
			UPDATE ##SourceParameters 
			SET SourcewebParameters = REPLACE(SourcewebParameters, '<SourceWatermarkFieldValue>', CONVERT(varchar, YEAR(w.SourceWatermarkFieldValue)) + '-' +
				RIGHT('00' + CONVERT(varchar, MONTH(w.SourceWatermarkFieldValue)), 2) + '-' + 
				RIGHT('00' + CONVERT(varchar, DAY(w.SourceWatermarkFieldValue)), 2) + 'T' +
				RIGHT('00' + CONVERT(varchar, DATEPART(HOUR, w.SourceWatermarkFieldValue)), 2) + ':' +
				RIGHT('00' + CONVERT(varchar, DATEPART(MINUTE, w.SourceWatermarkFieldValue)), 2) + ':' +
				RIGHT('00' + CONVERT(varchar, DATEPART(SECOND, w.SourceWatermarkFieldValue)), 2) + 'Z')	
			FROM dbo.Watermark w
			WHERE Environment = @Environment
				AND IsActive = 1
				AND BatchName = @BatchName
				AND SourceServiceType = @SourceServiceType
				AND SinkServiceType = @SinkServiceType

			UPDATE ##SourceParameters SET SourcewebParameters = REPLACE(SourcewebParameters, '<WatermarkTimestamp>', CONVERT(varchar, YEAR(@WatermarkTimestamp)) + '-' +
				RIGHT('00' + CONVERT(varchar, MONTH(@WatermarkTimestamp)), 2) + '-' + 
				RIGHT('00' + CONVERT(varchar, DAY(@WatermarkTimestamp)), 2) + 'T' +
				RIGHT('00' + CONVERT(varchar, DATEPART(HOUR, @WatermarkTimestamp)), 2) + ':' +
				RIGHT('00' + CONVERT(varchar, DATEPART(MINUTE, @WatermarkTimestamp)), 2) + ':' +
				RIGHT('00' + CONVERT(varchar, DATEPART(SECOND, @WatermarkTimestamp)), 2) + 'Z')

		END
*/

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

	WHILE @@TRANCOUNT > 0
	BEGIN
		COMMIT
	END
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
