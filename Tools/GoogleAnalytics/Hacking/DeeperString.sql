declare @open varchar(255) = '{';
declare @close varchar(255) = '}';

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
with rip as (   
    /* Get the first open bracket's openPos, start a series and set level to 0 / root */
    select      openPos = charindex(@open, @json, 1)
            ,   closePos = charindex(@close, @json, 1)
            ,   prior_open_position = 0
            ,   prior_close_position = 0
            ,   ordinal = 1
            ,   lvl = 0
    union all
    /* Recursion - Get next successive open bracket's openPos, increment series, 
                    and set level - increment if no closing bracket found; otherwise, keep level same as above */
    select      openPos = charindex(@open, @json, r.openPos + 1)
            ,   closePos = charindex(@close, @json, r.closePos + 1)
            ,   prior_open_position = r.openPos
            ,   prior_close_position = r.closePos
            ,   ordinal = r.ordinal + 1
            ,   lvl = r.lvl + case 
                                when charindex(@open, @json, r.openPos + 1) > charindex(@close, @json, r.openPos) then 0 
                                else 1 end
    from        rip r
    where       r.openPos < charindex(@open, @json, r.openPos + 1)
)
, ord as (
    select      bracket = @open
            ,   position = openPos
            ,   prior_position = prior_open_position
            ,   ordinal
            ,   lvl
    from        rip
    union all   
    select      bracket = @close
            ,   position = closePos
            ,   prior_position = prior_close_position
            ,   ordinal
            ,   lvl
    from        rip
)
, seq as (
    select      id = row_number() over (order by position)
            ,   bracket
            ,   position
            ,   prior_position
    from        ord
)
, pair as (
    select      cur.id
            ,   cur.bracket
            ,   next_bracket = nxt.bracket
            ,   cur.position
            ,   next_position = nxt.position
            ,   cur.prior_position
            ,   pair_id = case 
                            when cur.bracket = @open and nxt.bracket = @close then nxt.id 
                            else 0 end 
            ,   offset = 1
    from        seq cur
    left join   seq nxt on nxt.id - 1 = cur.id 
    union all
    select      id = row_number() over (order by cur.position)
            ,   cur.bracket
            ,   next_bracket = nxt.bracket
            ,   cur.position
            ,   next_position = nxt.position
            ,   cur.prior_position
            ,   pair_id = case 
                            when cur.bracket = @open and nxt.bracket = @close then nxt.id 
                            else 0 end 
            ,   offset = nxt.offset + 1
    from        seq cur
    join        pair nxt on nxt.id != row_number() over (order by cur.position) and nxt.pair_id != row_number() over (order by cur.position) and nxt.pair_id = 0
)

select      distinct id, pair_id, bracket, next_bracket, position, next_position
from        pair
order by    pair_id, id

/*
select      * 
from        pair
where       is_pair = 1
*/