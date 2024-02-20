declare @json varchar(2000) = '{"id":2, "name": {"first": "Joe", "middle_initial": "Q", "last": "Public"}, "address": {"street": "123 Main", "city": "Townsville", "state": "KS"}}';

with ssp as (
    select      baseValue = b.[value], 
                Id = ltrim(replace(id.[value], '"', '')), 
                Val = ltrim(replace(val.[value], '"', '')), 
                concat(e.ordinal, '.', s.ordinal, '.', b.ordinal) ordinal
    from        string_split(@json, '{', 1) e
    cross apply string_split(e.[value], '}', 1) s
    cross apply string_split(s.[value], ',', 1) b
    cross apply string_split(b.[value], ':', 1) id
    cross apply string_split(b.[value], ':', 1) val
    where       e.[value] <> '' and s.[value] <> '' and b.[value] <> '' and id.ordinal = 1 and val.ordinal = 2
), pos as (
    
)

select		*
from		ssp
order by    ordinal