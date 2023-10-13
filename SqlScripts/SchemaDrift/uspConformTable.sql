create or alter procedure dbo.uspConformTable 
/* =======================================================================================================================
dbo.uspConformTable
    Author:		A. Carter Burleigh (ACB)
    Info:		See selfDoc: section
---	----------	-------------------------------------------------------------------------------------------------------
ACB	2023-10-12	Initial development
======================================================================================================================= */
    (   @srcSchema varchar(255)
    ,   @srcTable varchar(255)
    ,   @dstSchema varchar(255)
    ,   @dstTable varchar(255)
    ,   @Options varchar(max) = '{"ALL": 0, "RPT": 0, "SQL": 1, "DML": 1}'
    ,   @Help bit = 0
    )
as
begin
    /* ===================================================================================================================
       Table to quantify differences between tables 
    =================================================================================================================== */
    declare @comp table
        (   ColumnName varchar(255)
        ,   RequiresAction bit default 1
        ,   sTypeName varchar(255)
        ,   sMaxLength int
        ,   sPrecision int
        ,   sScale int
        ,   [-->] char(3) default '-->'
        ,   dTypeName varchar(255)
        ,   dMaxLength int
        ,   dPrecision int
        ,   dScale int
        ,   ColumnExists smallint
        ,   TypeDiff smallint
        ,   MaxLengthDiff smallint
        ,   PrecisionDiff smallint
        ,   ScaleDiff smallint
        ,   ActionMessage varchar(2000)
        ,   AlterStmt varchar(2000)
        ,   SqlClause varchar(2000)
        );

    /* =============================================================================================================
        Compare the source and destination at the column + type + modifier level
        Note on %Diff columns:
            = 0 : no difference
            > 0 : destination column is larger (ex. nvarchar(50) -> nvarchar(100))
            < 0 : destination column is smaller and action needed (alter / exclude column, warning issued, etc.)
    ============================================================================================================= */
    insert into @comp ( ColumnName, dTypeName, dMaxLength, dPrecision, dScale, sTypeName, sMaxLength, sPrecision, sScale
                    ,   ColumnExists, TypeDiff, MaxLengthDiff, PrecisionDiff, ScaleDiff)
    select      coalesce(dst.ColumnName, src.columnName), dst.TypeName, dst.max_length, dst.[Precision], dst.Scale
            ,   src.TypeName, src.max_length, src.[Precision], src.Scale
            ,   case 
                    when src.ColumnName = dst.ColumnName then 0 
                    when src.ColumnName is null then -1
                    when dst.ColumnName is null then 1
                    else NULL 
                end
            ,   case 
                    when src.TypeName = dst.TypeName then 0 
                    else 1
                end
            ,   src.max_length - dst.max_length, src.[Precision] - dst.[Precision], src.[Scale] - dst.[Scale]
    from        dbo.uftGetTableDefinition(@dstSchema, @dstTable, 0) dst
    full join   dbo.uftGetTableDefinition(@srcSchema, @srcTable, 0) src on src.ColumnName = dst.ColumnName;

    /* ===================================================================================================================
       Decode for various actions to take and descriptive messages to caller  
    =================================================================================================================== */
    declare @actions table 
        (   Class varchar(10)
        ,   Subclass varchar(25)
        ,   ActionMessage varchar(2000)
        )

    insert into @actions (Class, Subclass, ActionMessage)
    values  ('RPT', 'SAME', '/* Source and destination column definitions are identical */')
        ,   ('RPT', 'SMALLER_SOURCE', '/* Destination column is of same base type but allows more data (ex. int -> bigint; varchar(50) -> varchar(100)) */')

        ,   ('SQL', 'NO_SOURCE', 'Column does not exist in source, exclude from destination insert')
        ,   ('SQL', 'TO_STRING', 'Destination column is a string type but source is not - use explicit cast')
        
        ,   ('DML', 'NO_DEST', 'Destination column does not exist - either add it to the table or exclude it from the insert')
        ,   ('DML', 'SMALLER_STRING', 'Destination string is smaller than source - either alter destination column or risk truncation')
        ,   ('DML', 'SMALLER_NUMERIC', 'Destination number / datetime is smaller than source - either alter destination column or risk truncation')
        ,   ('DML', 'TO_SMALL_STRING', 'Destination column is a string type too small to accept source data - expand destination AND use explicit cast')
        ,   ('DML', 'ASCII_TO_UNICODE', 'Source column is unicode but destination is ASCII - either alter destination column or perform cast and risk error')
        ,   ('DML', 'PRECISION', 'Destination column has lower precision than source - risk of truncation')
        ,   ('DML', 'SCALE', 'Destination column has lower scale than source - risk of truncation')
        ,   ('DML', 'PRECISION_SCALE', 'Destination column has lower precision or scale than source - risk of truncation')
        
        ,   ('ERR', 'FROM_STRING', 'Source column is a string type but destination type is not - investigate')
        ,   ('ERR', 'UNKNOWN', 'Unknonwn case - investigate and enhance this stored procedure')

    /* ===================================================================================================================
       Build actions to take 
    =================================================================================================================== */
    /* Short cut for most likely case - column is same in source and destination */
    update      c 
    set         c.RequiresAction = 0, c.ActionMessage = 'RPT.SAME'
    from        @comp c
    where       (abs(c.ColumnExists) + abs(c.TypeDiff) + abs(c.MaxLengthDiff) + abs(c.PrecisionDiff) + abs(c.ScaleDiff)) = 0

    /* More complex conditions */
    update      c
    set         c.ActionMessage = 
                case
                    /* Source is string but destination is something else - this cannot be handled automatically */
                    when    TypeDiff <> 0 and (sTypeName like '%char' or sTypeName like '%text') and dTypeName not like '%char' and dTypeName not like '%text'
                        then 'ERR.FROM_STRING'

                    /* Destination column does not exist */
                    when    ColumnExists > 0 
                        then 'DML.NO_DEST'

                    /* Going to a different precision and scale in decimal type */
                    when    sTypeName in ('decimal', 'numeric') and dTypeName in ('decimal', 'numeric') and (PrecisionDiff > 0 and ScaleDiff >= 0 or PrecisionDiff >= 0 and ScaleDiff > 0) 
                        then 'DML.PRECISION_SCALE'
                    
                    /* Going to a smaller numeric type */
                    when   ( TypeDiff = 0 or sTypeName in ('real', 'float') and dTypeName in ('real', 'float'))
                        and PrecisionDiff > 0 and ScaleDiff = 0
                        then 'DML.PRECISION'

                    /* Going to a smaller scale in decimal type */
                    when   ( TypeDiff = 0 or sTypeName in ('decimal', 'numeric', 'time', 'datetime2', 'datetimeoffset') and dTypeName in ('decimal', 'numeric', 'time', 'datetime2', 'datetimeoffset'))
                        and PrecisionDiff >= 0 and ScaleDiff > 0
                        then 'DML.SCALE'

                    /* Non-string data going to string field, but destination is not wide enough */
                    when    TypeDiff <> 0 
                        and sTypeName not like '%char' and sTypeName not like '%text'
                        and (dTypeName like '%char' or dTypeName like '%text') 
                        and sPrecision > case when left(dTypeName, 1) = 'n' then dMaxLength / 2 else dMaxLength end
                        then 'DML.TO_SMALL_STRING'

                    /* Destination string not wide enough */
                    when    TypeDiff = 0 and dPrecision = 0 and MaxLengthDiff > 0
                        then 'DML.SMALLER_STRING'

                    /* Destination numeric istype not wide enough */
                    when    TypeDiff <> 0 and PrecisionDiff > 0
                        and (   sTypeName like '%int' and dTypeName like '%int' 
                            or  sTypeName like '%date%' and dTypeName like '%date%' 
                            or  sTypeName like '%money%' and dTypeName like '%money%'
                            )
                        then 'DML.SMALLER_NUMERIC'

                    /* Source is a numeric (int, float, date, etc.) and destination is a string type wide enough to contain it */
                    when    TypeDiff <> 0 
                        and (dTypeName like '%char' or dTypeName like '%text') 
                        and sTypeName not like '%char' and sTypeName not like '%text'
                        and sPrecision <= case when left(dTypeName, 1) = 'n' then dMaxLength / 2 else dMaxLength end
                        then 'SQL.TO_STRING'

                    /* Source column does not exist */
                    when    ColumnExists < 0 
                        then 'SQL.NO_SOURCE'
    
                    /* Same string types with bigger destination max length */
                    when    TypeDiff = 0 and MaxLengthDiff < 0 and dPrecision = 0
                            /* Assure same numeric 'family' and are moving to wider field */
                        or  PrecisionDiff < 0 and ( sTypeName like '%int' and dTypeName like '%int' 
                                                or  sTypeName like '%date%' and dTypeName like '%date%' 
                                                or  sTypeName like '%time%' and dTypeName like '%time%'
                                                or  sTypeName like '%money%' and dTypeName like '%money%'
                                                or  sTypeName = 'real' and dTypeName = 'float'
                                                )
                        or  PrecisionDiff < 0 and ScaleDiff < 0 and ( sTypeName in ('decimal', 'numeric') and dTypeName in ('decimal', 'numeric'))
                        then 'RPT.SMALLER_SOURCE'

                    /****** As yet unhandled cases ******/
                    else 'ERR.UNKNOWN'
                end
    from        @comp c
    where       RequiresAction = 1

    /* Return results - adhere to options specified but always return *** results */
    select      ColumnName, RequiresAction
            ,   ActionMessage 
            ,   *
    from        @comp
    where exists (  select  * 
                    from    openjson(@options) o 
                    where   o.[value] = 1 
                        and (   o.[key] = 'ALL'
                            or  charindex(o.[key], ActionMessage collate Latin1_General_BIN2) = 1 
                            )
                )
        or      charindex('ERR', ActionMessage) = 1 
    order by    3, 2, 1;
end

go

