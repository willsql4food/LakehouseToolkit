declare @srcSchema varchar(255) = 'dbo';
declare @srcTable varchar(255)  = 'smaller';
declare @dstSchema varchar(255) = 'dbo';
declare @dstTable varchar(255)  = 'bigger';

declare @o varchar(2000) = 
    --'{"ALL": 1}'
    '{"DML": 1, "SQL": 1}'
    -- '{}'
    --'{"RPT": 1}'

exec uspConformTable @srcSchema, @srcTable, @dstSchema, @dstTable, @o

exec uspConformTable @dstSchema, @dstTable, @srcSchema, @srcTable, @o
