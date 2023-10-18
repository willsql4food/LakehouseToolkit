declare @smallSchema varchar(255) = 'dbo';
declare @smallTable varchar(255)  = 'smaller';
declare @bigSchema varchar(255) = 'dbo';
declare @bigTable varchar(255)  = 'bigger';

declare @o varchar(2000) = 
    '{"ALL": 1}'
    --'{"DML": 1, "SQL": 1}'
    -- '{}'
    --'{"RPT": 1}'

exec uspConformTable @smallSchema, @smallTable, @bigSchema, @bigTable, @o
