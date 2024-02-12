CREATE VIEW dbo.gbqObjectToLoadCmd
	AS 
	SELECT		l.TableCatalog, l.TableSchema, l.TableName, l.BatchCol,
				convert(nvarchar(max), 
					concat(	'select ', quotename(l.TableCatalog, char(39)), ' TableCatalog, ', 
							quotename(l.TableSchema, char(39)), ' TableSchema, ', 
							quotename(l.TableName, char(39)), ' TableName, ',
							coalesce(quotename(l.BatchCol, char(39)), 'NULL'), ' BatchCol, ',
							coalesce(l.BatchCol, 'NULL'), ' BatchTerm, count(*) RowCountSource, ',
							case 
								when l.SubBatchCol is not null 
									then concat(  quotename(l.SubBatchCol, char(39)), ' SubBatchCol, '
												, 'MIN(', l.SubBatchCol, ') SubBatchMin, '
												, 'MAX(', l.SubBatchCol, ') SubBatchMax ')
								else 'cast(NULL as STRING) SubBatchCol, cast(NULL as INT64) SubBatchMin, cast(NULL as INT64) SubBatchMax '
							end,
							'from ', l.TableSchema, '.', l.TableName,
							case 
								when l.BatchCol is not null then ' group by ' + l.BatchCol
								else '' 
							end)) CountCmd
	FROM		dbo.gbqObjectToLoad l
