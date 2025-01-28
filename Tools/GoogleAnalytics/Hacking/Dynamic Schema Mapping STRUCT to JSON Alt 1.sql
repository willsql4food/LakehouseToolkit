/* 
    Figuring out how to handle mapping columns with complex types 
    1 - Want any that are STRUCT to simply be flattened out
    2 - Array types will need to be put in separate tables, flattened out there, and have enough reference to tie back to the original table
*/

with cte as (
    select      TableName, ColumnName, ColumnId, DataType, 
                replace(replace(replace(replace(DataType, 'STRUCT<', '"path":"'), '>', '"'), ', ', '","path":"'), ' ', '"!"')  toJson,
                case when DataType like 'STRUCT%STRUCT%' then '***' else '' end nested
    from        dbo.gbqColumn
    where       DataType like 'STRUCT%'
)

select      TableName, ColumnName, ColumnId, nested, toJson,
            s.ordinal prime, x.*, DataType
from        cte
cross apply string_split(toJson, ',', 1) s
cross apply string_split(s.value, '!', 1) x
where       x.ordinal = 1
order by    TableName, ColumnName, ColumnId, prime, ordinal


/*
/*  Structured types that include array(s)
    Deferred as there are none 
*/
select      *
from        dbo.gbqColumn
where       DataType like 'STRUCT%ARRAY%'

/* Arrays */
select      *
from        dbo.gbqColumn
where       DataType like 'ARRAY%'


/* Nested arrays */
select      *
from        dbo.gbqColumn
where       DataType like 'ARRAY%ARRAY%' 
*/