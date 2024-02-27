declare @open varchar(255) = '{'
declare @close varchar(255) = '}'

declare @json varchar(2000) = 
'{"id":2, "name": {"first": "Joe", "middle_initial": "Q", "last": "Public"}, "address": {"street": {"s1": "123 Main", "s2": "Suite 4"}, "city": "Townsville", "state": "KS"}}';

with bkt as (
    /* Find positions of all the open and close delimiters (curly braces, < / >, etc.) */
    select      openBracket = charindex(@open, @json),
                closeBracket = charindex(@close, @json)
    union all
    select      charindex(@open, @json, bkt.openBracket + 1) openBracket, 
                charindex(@close, @json, bkt.closeBracket + 1) closeBracket
    from        bkt
    where       bkt.openBracket < charindex(@open, @json, bkt.openBracket + 1)
), poss as (
    /* Every possible pairing - the open delimiter comes before the end delimiter */
    select      o.openBracket, c.closeBracket,
                distance = c.closeBracket - o.openBracket
    from        bkt o
    join        bkt c on c.closeBracket > o.openBracket
), enum as (
    select      openBracket, count(distinct closeBracket) closeCount
    from        poss
    group by    openBracket
), pairs as (
    /* Find the innermost pairs - there is no opening bracket between them and the shortest distance from that open to the possible close */
    select      p.openBracket, 
                p.closeBracket,
                nestLevel = 1
    from        poss p
    join       (select openBracket, distance = min(distance) from poss group by openBracket) mind on mind.openBracket = p.openBracket and mind.distance = p.distance
    where not exists (select * from poss x  where x.openBracket > p.openBracket and x.openBracket < p.closeBracket)
    /* Recursively get the next outer parings - excluding list found so far, find pairs with no other open between and shortest distance */
    union all
    select      p.openBracket, 
                p.closeBracket,
                nestLevel = 2
    from        poss p
    join       (select openBracket, distance = min(distance) from poss group by openBracket) mind on mind.openBracket = p.openBracket and mind.distance = p.distance
)

select      * 
from        poss
order by    openBracket
/*
select		'Pairs' msg, 
            openBracket, 
            closeBracket, 
            distance = closeBracket - openBracket,
            nestLevel

from		pairs
*/
/*
union all
select		'Possibles' msg, openBracket, closeBracket, 
            distance = closeBracket - openBracket
from		poss
order by    msg, openBracket, closeBracket
*/