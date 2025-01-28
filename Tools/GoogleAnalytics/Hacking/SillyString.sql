declare @open varchar(255) = '{';
declare @close varchar(255) = '}';

declare @string varchar(2000) 
select @string = '{"id":2, "name": {"first": "Joe", "middle_initial": "Q", "last": "Public"}, "address": {"street": {"s1": "123 Main", "s2": "Suite 4"}, "city": "Townsville", "state": "KS"}}';
/*
{"id":2, "name": {"first": "Joe", "middle_initial": "Q", "last": "Public"}, "address": {"street": {"s1": "123 Main", "s2": "Suite 4"}, "city": "Townsville", "state": "KS"}}
1                18                                                      74            88         99                                133                                   171|172
O1               O2                                                                    O3         O4                                                                      
L0               L1                                                                    L1         L2                                                                      
                                                                         C1                                                         C2                                    C3 | C4
                                                                         L1                                                         L2                                    L1 | L0
*/
select @string = '{"id":7, "name": {"first": "Joe", "middle_initial": "Q", "last": {"maiden": "N/A", "married": "Public", "divorced": "Private"}, "alias":"Joey-Q"}, "physio": {"height": {"feet": 5, "inches": 10}, "weight": 215}, "address": {"street": {"s1": "123 Main", "s2": "Suite 4"}, "city": "Townsville", "state": "KS"}}';

/*
declare @struct nvarchar(max)
select @struct = 'STRUCT<thingy STRUCT<category STRING, mobile_brand_name STRING, mobile_model_name STRING, mobile_marketing_name STRING, mobile_os_hardware_model STRING, os STRUCT<operating_system STRING, operating_system_version STRING>, vendor_id STRING, advertising_id STRING, language STRING, is_limited_ad_tracking STRING, time_zone_offset_seconds INT64, browser STRING, browser_version STRING, web_info STRUCT<browser STRING, browser_version STRING, hostname STRING>>>'
select @struct = 'STRUCT<total_item_quantity INT64, purchase_revenue_in_usd FLOAT64, purchase_revenue FLOAT64, refund_value_in_usd FLOAT64, refund_value FLOAT64, shipping_value_in_usd FLOAT64, shipping_value FLOAT64, tax_value_in_usd FLOAT64, tax_value FLOAT64, unique_items INT64, transaction_id STRING>'
select @struct = 'STRUCT<category STRING, mobile_brand_name STRING, mobile_model_name STRING, mobile_marketing_name STRING, mobile_os_hardware_model STRING, operating_system STRING, operating_system_version STRING, vendor_id STRING, advertising_id STRING, language STRING, is_limited_ad_tracking STRING, time_zone_offset_seconds INT64, browser STRING, browser_version STRING, web_info STRUCT<browser STRING, browser_version STRING, hostname STRING>>'

select @string = replace(replace(replace(replace(@struct
                                            , 'STRUCT<', '{')
                                        , '>', '}')
                                    , ', ', '},{')
                                , ' ', ':')

select @string = replace(@struct, ', ', ','), @open = 'STRUCT<', @close = '>'
*/
print @string;

declare @brackets table(id int, bracket varchar(255), position int);

declare @pairs table(id int not null identity(1,1), open_position int, close_position int, part_length int, part nvarchar(max), inner_part nvarchar(max), parent_id int null, nest_level tinyint);

/* Tear the string up looking for start and end brackets with their proximity to each other */
with rip as (   
    /* Get the first open bracket's openPos, start a series and set level to 0 / root */
    select      openPos = charindex(@open, @string, 1)
            ,   closePos = charindex(@close, @string, 1)
    union all
    /* Recursion - Get next successive open bracket's openPos, increment series, 
                    and set level - increment if no closing bracket found; otherwise, keep level same as above */
    select      openPos = charindex(@open, @string, r.openPos + 1)
            ,   closePos = charindex(@close, @string, r.closePos + 1)
    from        rip r
    where       r.openPos < charindex(@open, @string, r.openPos + 1)
)
insert into @brackets (bracket, position)
select      bracket = @open
        ,   position = openPos
from        rip
union all   
select      bracket = @close
        ,   position = closePos
from        rip

declare @nest_level tinyint = 0
while exists (select * from @brackets)
begin

    update      @brackets
    set         id = sub.id
    from        @brackets b
    join    (   select id = row_number() over (order by position), position from @brackets ) sub on sub.[position] = b.[position]

    insert into @pairs (open_position, close_position, part_length, part, inner_part, nest_level)
    select      o.[position]
            ,   c.[position]
            ,   c.[position] - o.[position] + 1
            ,   substring(@string, o.[position], 1 + c.[position] - o.[position])
            ,   substring(@string, o.[position] + len(@open), 1 + c.[position] - o.[position] - len(@open) - len(@close))
            ,   @nest_level
    from        @brackets o
    join        @brackets c on c.id = o.id + 1
    where       o.bracket = @open 
        and     c.bracket = @close

    update      pr
    set         pr.parent_id = pnt.id
    from        @pairs pr
    join        @pairs pnt on pnt.open_position < pr.open_position and pnt.close_position > pr.close_position and pnt.nest_level = pr.nest_level + 1
    where       pr.parent_id is null

    delete      b 
    from        @brackets b
    where   exists (select * from @pairs po where po.open_position = b.[position])
        or  exists (select * from @pairs pc where pc.close_position = b.[position])
    
    select @nest_level = @nest_level + 1
end;

with pears as (
    select      p.id
            ,   p.parent_id
            ,   p.nest_level
            ,   p.open_position
            ,   p.close_position
            ,   p.part_length
            ,   p.part
            ,   p.inner_part
            ,   ss.ordinal
            ,   node_name = rtrim(sss.[value])
    from        @pairs p
    cross apply string_split(p.inner_part, ',', 1) ss
    cross apply string_split(ltrim(ss.[value]), ' ', 1) sss
    where       ss.ordinal = 1 and sss.ordinal = 1
    -- order by    open_position desc
)
, pp as (
    select      p.nest_level
            ,   p.parent_id
            ,   p.id
            ,   p.open_position
            ,   p.close_position
            ,   p.inner_part
            ,   p.node_name
            ,   source_json = p.node_name
            ,   sink_json = p.node_name
    from        pears p
    where       p.parent_id is null
    union all
    select      p.nest_level
            ,   p.parent_id
            ,   p.id
            ,   p.open_position
            ,   p.close_position
            ,   p.inner_part
            ,   p.node_name
            ,   source_json = concat_ws('.', pnt.source_json, p.node_name)
            ,   sink_json = concat_ws('__', pnt.sink_json, p.node_name)
    from        pears p
    join        pp pnt on pnt.id = p.parent_id
)

select      pp.* 
from        pp
where not exists (select id from pp x where x.parent_id = pp.id)
order by    nest_level, open_position
