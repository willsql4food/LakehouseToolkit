
select		@@SERVERNAME srvr, DB_NAME() dbName, s.name schemaName, t.name tableName
		,	c.column_id, c.name columnName, c.is_nullable, c.generated_always_type, c.generated_always_type_desc
		,	coalesce(ix.name, '') indexName, coalesce(ix.is_primary_key, 0) is_primary_key
		,	coalesce(ic.index_column_id, -1) index_column_id
		,	ty.*
from		sys.schemas s
join		sys.tables t on t.schema_id = s.schema_id
join		sys.columns c on c.object_id = t.object_id
join		sys.types ty on ty.user_type_id = c.user_type_id
left join	sys.index_columns ic on ic.object_id = t.object_id and ic.column_id = c.column_id
left join	sys.indexes ix on ix.object_id = t.object_id and ic.index_id = ix.index_id
--where		t.name = 'SchemaDrift'
order by	s.name, t.name, ix.name, c.column_id, ic.index_column_id
