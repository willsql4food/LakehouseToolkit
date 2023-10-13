if exists (select * from sys.schemas s join sys.tables t on t.schema_id = s.schema_id where s.name = 'dbo' and t.name = 'smaller')
	drop table dbo.smaller;
go

create table dbo.smaller (
		id int not null identity(1, 1)

	,	i1 tinyint
	,	i2 smallint
	,	i3 int
	--,	i3 bigint
	
	,	dt1 date
	,	dt2 smalldatetime
	,	dt3 datetime
	,	dt4 datetime2(5)
	,	dt5 datetimeoffset(5)
	,	t time(5)
	
	,	bucks smallmoney
	--,	bucks money
	
	,	r1 real
--	,	r1 float

	,	n1 decimal(20, 5) -- / numeric

	,	cs char(10)
	,	ncs nchar(10)
	,	s varchar(200)
	,	ns nvarchar(200)

	,	bin binary(90)
	,	vbin varbinary(90)

	,	ft real
)

go

if exists (select * from sys.schemas s join sys.tables t on t.schema_id = s.schema_id where s.name = 'dbo' and t.name = 'bigger')
	drop table dbo.bigger;
go

create table dbo.bigger (
		id int not null identity(1, 1)

	,	i1 smallint
	,	i2 int
	,	i3 bigint
	
	,	dt1 smalldatetime
	,	dt2 datetime
	,	dt3 datetime2(7)
	,	dt4 datetimeoffset(5)
	,	dt5 datetimeoffset(7)
	,	t time(7)
	
	,	bucks money
	
	,	r1 float

	,	n1 decimal(35, 10) -- / numeric

	,	cs char(15)
	,	ncs nchar(15)
	,	s varchar(250)
	,	ns nvarchar(250)

	,	bin binary(120)
	,	vbin varbinary(120)

	,	ft float
)