-- SQL database definition file
-- run to create TQASched database framework

BEGIN TRAN

--	# create update table
create table [TQASched].dbo.[Updates] (
	update_id int not null identity(1,1),
	name varchar(255) not null unique,
	priority tinyint,
	is_legacy bit
)

--# create update/schedule linking table
create table [TQASched].dbo.[Update_Schedule] (
	sched_id int not null identity(1,1),
	update_id int not null,
	weekday tinyint not null,
	sched_epoch int not null,
	enabled bit
)
--# create history tracking table
create table [TQASched].dbo.[Update_History] (
	hist_id int not null identity(1,1),
	update_id int not null,
	sched_id int not null,
	hist_epoch int,
	filedate int,
	filenum tinyint,
	timestamp DateTime,
	late char(1),
	transnum int,
	feed_date DateTime not null,
	seq_num int
)

--# constrain history tracking to prevent duplicate records per feed_date
CREATE UNIQUE CLUSTERED INDEX [IX_Update_History] ON [TQASched].[dbo].[Update_History] 
(
	[sched_id] DESC,
	[feed_date] DESC
)WITH (PAD_INDEX  = OFF, STATISTICS_NORECOMPUTE  = OFF, SORT_IN_TEMPDB = OFF, IGNORE_DUP_KEY = ON, DROP_EXISTING = OFF, ONLINE = OFF, ALLOW_ROW_LOCKS  = ON, ALLOW_PAGE_LOCKS  = ON) ON [PRIMARY]


--# create linking table from DIS feed_ids to update_ids
	create table [TQASched].dbo.[Update_DIS] (
	update_dis_id int not null identity(1,1),
	feed_id varchar(20) not null,
	update_id int not null
)

COMMIT TRAN