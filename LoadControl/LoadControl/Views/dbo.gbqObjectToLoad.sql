CREATE VIEW dbo.gbqObjectToLoad
	AS 
	SELECT		n.TableCatalog, n.TableSchema, n.TableName,
				case when n.TableName like 'events%' then 'event_name'
					else null
				end BatchCol,
				case when n.TableName like 'events%' then 'event_timestamp'
					else null
				end SubBatchCol
	FROM		stage.gbqObject n
	where		not exists (select	* 
							from	dbo.gbqWatermark o
							where	o.TableCatalog = n.TableCatalog and o.TableSchema = n.TableSchema and o.TableName = n.TableName
								and	o.LoadedDateUTC is not null)