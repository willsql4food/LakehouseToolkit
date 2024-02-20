with cte as (
    select      TableName, ColumnName, ColumnId, DataType, 
                replace(replace(replace(replace(DataType, 'STRUCT<', '{'), '>', '}'), ', ', ','), ' ', '!') csv,
                case when DataType like 'STRUCT%STRUCT%' then '***' else '' end nested
    from        dbo.gbqColumn
    where       DataType like 'STRUCT%'
), ex as (
    select      TableName, ColumnName, ColumnId, DataType, x.ordinal xord, x.[value] [path]
    from        cte
    cross apply string_split(csv, ',', 1) s
    cross apply string_split(s.value, '!', 1) x
    where       x.ordinal = 1
), comb as (
    select      TableName, ColumnName, ColumnId, DataType, [path]
    from        ex
--    group by    TableName, ColumnName, ColumnId, DataType
)

select      TableName, ColumnName, ColumnId, DataType, csv, 
            strt.ordinal startNum, strt.[value] startVal, 
            stp.ordinal stopNum, stp.[value] stopVal
from        cte
cross apply string_split(csv, '{', 1) strt
cross apply string_split(csv, '}', 1) stp
where       nested = '***'
/*
select      TableName, [path], sink = ColumnName + '_' + [path]
from        comb 
for         json path, root('Transform')
*/