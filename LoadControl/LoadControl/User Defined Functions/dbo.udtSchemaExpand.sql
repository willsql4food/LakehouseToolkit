create function dbo.udtSchemaExpand(
        @string nvarchar(max)
    ,   @open nvarchar(255)
    ,   @close nvarchar(255)
    ,   @element_delimiter nchar(1)
    ,   @id_delimiter nchar(1)
    ,   @source_sep nvarchar(255)
    ,   @sink_sep nvarchar(255)
)
returns @final table (position int, element nvarchar(max), source nvarchar(max), sink nvarchar(max))
with schemabinding
as
begin
    declare @id int;

    /*
    First, find the nested structures
    Use the element right before a struct as the parent name
    Every comma separated field in the struct belongs to that parent
    Parents nest as we go out levels
    */

    /* ====================================================================================================================
        Tables to hold working datasets
            Brackets    : position of each open and close bracket found
            Layers      : pairs of adjacent brackets and layer id accounting for inner brackets
            Elements    : items extracted from inside each pair of brackets with parent-child linkage
    ==================================================================================================================== */
    declare @brackets table (id int, bracket nvarchar(255), position int)
    declare @layers table (id int identity(1,1), open_position int, close_position int, layer_id tinyint, string nvarchar(max));
    declare @replace_elements table (id int, string nvarchar(max))
    declare @elements table (layer_id tinyint, position int, element nvarchar(max));

    /* ====================================================================================================================
    Tear the string up looking for start and end brackets with their proximity to each other  
    ==================================================================================================================== */
    with rip as (   
        /* Get the first open bracket's openPos, start a series and set level to 0 / root */
        select      openPos = charindex(@open, @string, 1)
                ,   closePos = charindex(@close, @string, 1)
        union all
        /*  ***** Recursion *****
            Get next successive open bracket's openPos, increment series, 
            and set level - increment if no closing bracket found; otherwise, keep level same as above 
        */
        select      openPos = charindex(@open, @string, r.openPos + 1)
                ,   closePos = charindex(@close, @string, r.closePos + 1)
        from        rip r
        where       r.openPos < charindex(@open, @string, r.openPos + 1)
    )
    /* ====================================================================================================================
        Put brackets together into an ordered table
    ==================================================================================================================== */
    /* Get the openers... */
    insert into @brackets (bracket, position)
    select      bracket = @open
            ,   position = openPos
    from        rip
    /* ... and the closers */
    union all   
    select      bracket = @close
            ,   position = closePos
    from        rip


    /* Build the layers - inner most structure is level 0, its parent 1, etc. */
    declare @layer_id tinyint = 0
    while exists (select 1 from @brackets)
    begin
        /*  Reorder the remaining brackets so we can find newly adjacent pairs
            Set the id field to their relative order based on position */
        update      b
        set         b.id = cnt.id
        from        @brackets b
        join      ( select position, id = row_number() over (order by position) from @brackets) cnt on cnt.position = b.position

        /* Get the deepest remaining structures */
        insert into @layers(open_position, close_position, layer_id, string)
        select      o.position
                ,   c.position
                ,   @layer_id
                ,   substring(@string, o.position + len(@open), 1 + c.position - o.position - len(@open) - len(@close))
        from        @brackets o
        join        @brackets c on c.id = o.id + 1
        where       o.bracket = @open 
            and     c.bracket = @close

        /* Blank out each element from the layer below in the remaining string */
        insert into @replace_elements (id, string)
        select      id
                ,   string
        from        @layers
        where       layer_id = @layer_id - 1

        while exists(select 1 from @replace_elements)
        begin
            select  @id = (select top 1 id from @replace_elements)

            update  l
            set     l.string = replace(l.string, r.string, replicate('#', len(r.string)))
            from    @layers l
            join    @replace_elements r on r.id = @id
            where   layer_id = @layer_id

            delete from @replace_elements where id = @id
        end

        /* Split the elements in the layers, along with their positions */
        insert into @elements(layer_id, position, element)
        select      l.layer_id
                ,   position = charindex(ltrim(ss.[value]), @string, l.open_position)
                ,   element = ltrim(ss.[value])
        from        @layers l
        cross apply string_split(l.string, @element_delimiter) s
        cross apply string_split(ltrim(s.[value]), @id_delimiter, 1) ss
        where       l.layer_id = @layer_id
            and     ss.ordinal = 1

        /*  It's possible there are items with the same name under different elements
            Remove any elements found at lower level */
        delete      e
        from        @elements e
        join        @layers x on x.open_position < e.position and x.close_position > e.position
        where       x.layer_id < e.layer_id

        /* ====================================================================================================================
        Prepare next loop iteration 
        ==================================================================================================================== */
        /* Increment layer */
        select @layer_id = @layer_id + 1

        /* Remove bracket rows already paired up */
        delete      b
        from        @brackets b
        where exists (  select 1 from @layers x where x.open_position = b.position or x.close_position = b.position)
    end;

    /* ====================================================================================================================
        Populate source and sink 'fully qualified' names
    ==================================================================================================================== */
    select @id = (select max(layer_id) from @elements)
    while exists (select 1 from @elements where layer_id <= @id)
    begin

        insert into @final (position, element, source, sink)
        select      m.position
                ,   m.element
                ,   source = concat_ws(@source_sep, coalesce(f.source, p.element), m.element)
                ,   sink = concat_ws(@sink_sep, coalesce(f.sink, p.element), m.element)
        from    (
            select      e.layer_id
                    ,   e.position
                    ,   e.element
                    ,   parent_position = max(p.position)
            from        @elements e
            left join   @elements p on p.layer_id = e.layer_id + 1 and p.position < e.position
            where       e.layer_id = @id
            group by    e.layer_id
                    ,   e.position
                    ,   e.element
                ) m
        left join   @elements p on p.position = m.parent_position
        left join   @final f on f.position = m.parent_position

        /* Increment for loop */
        select @id = @id - 1
    end
    return
end
go


/* ====================================================================================================================
    TESTING
==================================================================================================================== */
/*
select      *
from        dbo.udtSchemaExpand(
                    'STRUCT<overall STRUCT<category STRING, mobile_brand_name STRING, mobile_model_name STRING, mobile_marketing_name STRING, os STRUCT<mobile_os_hardware_model STRING, operating_system STRING, operating_system_version STRING>, vendor_id STRING, advertising_id STRING, language STRING, is_limited_ad_tracking STRING, time_zone_offset_seconds INT64, browser STRING, browser_version STRING, web_info STRUCT<browser STRING, browser_version STRING, hostname STRING>>>'
                ,   'STRUCT<'
                ,   '>'
                ,   ','
                ,   ' '
                ,   '.'
                ,   '__')
order by    position
*/