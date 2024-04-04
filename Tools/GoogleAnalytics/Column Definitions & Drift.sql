/* ====================================================================================================================
	Column analysis - is there schema drift?
==================================================================================================================== */
with col as (
	/* Get the number of times we've seen each column with the same data type */
	select		TableCatalog
			,	TableSchema
			,	left(TableName, len(TableName) - 9) BaseTable
			,	max(right(TableName, 8)) TableSuffix
			,	ColumnName
			,	DataType
			,	count(*) NumOccurrences
	from		dbo.gbqColumn
	group by	TableCatalog
			,	TableSchema
			,	left(TableName, len(TableName) - 9)
			,	ColumnName
			,	DataType
), drift as (
	/* Find the number of different definitions that have been seen for each column */
	select		TableCatalog
			,	TableSchema
			,	BaseTable
			,	TableSuffix
			,	ColumnName
			,	count(*) NumDefinitions
	from		col
	group by	TableCatalog
			,	TableSchema
			,	BaseTable
			,	TableSuffix
			,	ColumnName
)

select		d.TableCatalog
		,	d.TableSchema
		,	d.BaseTable
		,	d.TableSuffix
		,	d.ColumnName
		,	d.NumDefinitions
		,	c.DataType
		,	c.NumOccurrences
from		drift d
join		col c on c.TableCatalog = d.TableCatalog and c.TableSchema = d.TableSchema and c.BaseTable = d.BaseTable and c.ColumnName = d.ColumnName
-- where		c.DataType like '%ARRAY%'
order by	d.NumDefinitions desc
		,	d.TableCatalog
		,	d.TableSchema
		,	d.BaseTable
		,	d.ColumnName
