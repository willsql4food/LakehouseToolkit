declare @json varchar(2000) = 
'{"id":2, "name": {"first": "Joe", "middle_initial": "Q", "last": "Public"}, "address": {"street": {"s1": "123 Main", "s2": "Suite 4"}, "city": "Townsville", "state": "KS"}}';

with sb as (
    /* Find positions of each of the opening curly braces */
    select      startBracket = charindex('{', @json), ordinal = 1
    union all
    select      startBracket = charindex('{', @json, sb.startBracket + 1), sb.ordinal + 1
    from        sb
    where       charindex('{', @json, sb.startBracket + 1) > 0
), eb as (
    /* Find the corresponding closing curly brace for each of these */
    select      ordinal, startBracket,
                endBracket = charindex('}', @json, (select max(startBracket) from sb)), 
                nodeLength = charindex('}', @json, (select max(startBracket) from sb)) - startBracket,
                modString = stuff(@json, startBracket, charindex('}', @json, (select max(startBracket) from sb)) - startBracket + 1, replicate('-', charindex('}', @json, (select max(startBracket) from sb) + 1) - startBracket + 1))
    from        sb
    where       startBracket = (select max(startBracket) from sb)
    union all
    select      sb.ordinal, sb.startBracket,
                endBracket = charindex('}', stuff(eb.modString, eb.startBracket, eb.nodeLength + 1, replicate('-', eb.nodeLength + 1)), sb.startBracket),
                nodeLength = charindex('}', stuff(eb.modString, eb.startBracket, eb.nodeLength + 1, replicate('-', eb.nodeLength + 1)), sb.startBracket) - sb.startBracket,
                modString = stuff(eb.modString, eb.startBracket, eb.nodeLength + 1, replicate('-', eb.nodeLength + 1))
    from        sb
    join        eb on eb.ordinal = sb.ordinal + 1
)

select      *, substring(@json, eb.startBracket, eb.nodeLength + 1) node
from        eb