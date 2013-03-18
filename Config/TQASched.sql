-- SQL database definition file
-- run to create TQASched2 database framework

BEGIN TRAN

use TQASched2;

--	# create update table
create table [TQASched2].dbo.[Updates] (
	update_id int not null identity(1,1) primary key,
	name varchar(255) not null unique,
	priority tinyint,
	is_legacy bit
)

--# create update/schedule linking table
create table [TQASched2].dbo.[Update_Schedule] (
	sched_id int not null identity(1,1) primary key,
	update_id int not null,
	weekday tinyint not null,
	sched_epoch int not null
	
)
--# create history tracking table
create table [TQASched2].dbo.[Update_History] (
	hist_id int not null identity(1,1) primary key,
	update_id int not null,
	sched_id int not null,
	hist_ts DateTime,
	filedate int,
	filenum tinyint,
	timestamp DateTime,
	late char(1),
	transnum int
)
--# create linking table from DIS feed_ids to update_ids
	create table [TQASched2].dbo.[Update_DIS] (
	update_dis_id int not null identity(1,1) primary key,
	feed_id varchar(20) not null,
	update_id int not null
)

COMMIT TRAN