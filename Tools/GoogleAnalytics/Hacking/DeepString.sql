declare @open varchar(255) = '{'
declare @close varchar(255) = '}'

declare @json varchar(2000) = '{"id":2, "name": {"first": "Joe", "middle_initial": "Q", "last": "Public"}, "address": {"street": {"s1": "123 Main", "s2": "Suite 4"}, "city": "Townsville", "state": "KS"}}';
/*
{"id":2, "name": {"first": "Joe", "middle_initial": "Q", "last": "Public"}, "address": {"street": {"s1": "123 Main", "s2": "Suite 4"}, "city": "Townsville", "state": "KS"}}
1                18                                                      74            88         99                                133                                   171|172
O1               O2                                                                    O3         O4                                                                      
L0               L1                                                                    L1         L2                                                                      
                                                                         C1                                                         C2                                    C3 | C4
                                                                         L1                                                         L2                                    L1 | L0
*/

/* Tear the string up looking for start and end brackets with their proximity to each other */
with ripo as (   
    /* Get the first open bracket's position, start a series and set level to 0 / root */
    select      position = charindex(@open, @json, 1),
                prior_position = 0,
                ordinal = 1,
                lvl = 0
    union all
    /* Recursion - Get next successive open bracket's position, increment series, 
                    and set level - increment if no closing bracket found; otherwise, keep level same as above */
    select      position = charindex(@open, @json, r.position + 1),
                prior_position = r.position,
                ordinal = r.ordinal + 1,
                lvl = r.lvl + case 
                                when charindex(@open, @json, r.position + 1) > charindex(@close, @json, r.position) then 0 
                                else 1 end
    from        ripo r
    where       r.position < charindex(@open, @json, r.position + 1)
)
, ripc as (
    /* Get the first closing bracket's position, start a series */
    select      position = charindex(@close, @json, 1),
                prior_position = 0,
                ordinal = 1,
                lvl = 0
    union all
    select      position = charindex(@close, @json, r.position + 1),
                prior_position = r.position,
                ordinal = r.ordinal + 1,
                lvl = r.lvl - case 
                                when charindex(@open, @json, r.position + 1) > charindex(@close, @json, r.position) then 0 
                                else 1 end
    from        ripc r
    where       r.position < charindex(@close, @json, r.position + 1)
)

select      'Open' msg, [position], ordinal, lvl, 
            candidate = 0,
            prior_position
from        ripo
union all
select      'Close' msg, [position], ordinal, lvl,
            candidate = (select max(position) from ripo where ripo.position < ripc.position and (ripo.position > ripc.prior_position or ripo.prior_position = 0)),
            prior_position
from        ripc
