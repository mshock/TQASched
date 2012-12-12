#! perl -w

package TQASched;

use strict;
use feature qw(say switch);
use Getopt::Std qw(getopts);
use Spreadsheet::ParseExcel;
use Spreadsheet::ParseExcel::Utility qw(ExcelFmt);
use Config::Simple;
use DBI;

# opts d,i,e for initialization

# for inheritance later - only for daemon's webserver so far
our @ISA;

# globals
my ($daemon_lock);

# number of minutes before a scheduled update is marked late
my $late_threshold = 30;

my %opts;
getopts( 'c:def:his', \%opts );

( usage() and exit ) if $opts{h};

my $conf_file = $opts{c} || 'tqa_sched.conf';

say 'parsing config file...';

# load config file
my ( $sched_db, $auh_db, $prod1_db ) = load_conf($conf_file);

# get excel file containing schedule info
my $sched_xls = $opts{f} || find_sched();

say 'initializing database handles...';

# initialize database handle
my $dbh_sched = init_handle($sched_db);
my $dbh_auh   = init_handle($auh_db);
my $dbh_prod1 = init_handle($prod1_db);

say 'done';

# initialize scheduling database from blank Excel file
init_sched() if $opts{i};

# run in daemon (server) mode
daemon() if $opts{s};

####################################################################################
#	subs
#
####################################################################################

# match DIS feeds from AUH to spreadsheet names in db
sub import_dis {

	say 'importing DIS feed mapping info...';

	# create linking table
	$dbh_sched->do( "
		create table [TQASched].dbo.[Update_DIS] (
		update_dis_id int not null identity(1,1),
		feed_id varchar(20) not null,
		update_id int not null
		)
	" )
		or warn "could not create DIS linking table - Update_DIS, may already exist\n";

	# open mapping file, populate mapping table
	open( MAP, '<', 'mapping.csv' );
	while (<MAP>) {
		chomp;
		my ( $name, $feed_id );
		if (m/"(.*)",(.*)/) {
			( $name, $feed_id ) = ( $1, $2 );
		}
		else {
			( $name, $feed_id ) = split ',';
		}

		my $update_id = get_update_id($name)
			or next;

		$dbh_sched->do( "
			insert into [TQASched].dbo.[Update_DIS]
			values ('$feed_id',$update_id)
		" )
			or warn
			"could not insert mapping for : $feed_id / $update_id -> $name\n";
	}
	close MAP;
}

# fill database with initial scheduling data
sub init_sched {

	# create parser and parse xls
	my $xlsparser = Spreadsheet::ParseExcel->new();
	my $workbook  = $xlsparser->parse($sched_xls)
		or die "unable to parse spreadsheet: $sched_xls\n",
		$xlsparser->error();
	say 'done';

	# optionally create database and tables
	( create_db() or die "failed to create database\n" ) if $opts{d};

	# populate database from excel file

	# iterate over each weekday (worksheets)
	for my $worksheet ( $workbook->worksheets() ) {
		my $weekday = $worksheet->get_name();
		say "parsing $weekday...";
		my $weekday_code = code_weekday($weekday);

		# skip if this is an unrecognized worksheet
		say "\tunable to parse weekday, skipping" and next
			if $weekday_code eq 'U';

		# find the row and column bounds for iteration
		my ( $col_min, $col_max ) = $worksheet->col_range();
		my ( $row_min, $row_max ) = $worksheet->row_range();

		my $sched_block = '';

		# iterate over each row and store scheduling data
		for ( my $row = $row_min; $row <= $row_max; $row++ ) {
			next if $row <= 1;

			# per-update hash of column values
			my $row_data = {};
			for ( my $col = $col_min; $col <= $col_max; $col++ ) {
				my $cell = $worksheet->get_cell( $row, $col );
				unless ( extract_row( $col, $cell, $row_data ) ) {

					#last;
				}
				else {
					if (    $row_data->{time_block}
						 && $sched_block ne $row_data->{time_block} )
					{
						$sched_block = $row_data->{time_block};
					}
					else {
						$row_data->{time_block} = $sched_block;
					}
				}
			}

			# skip rows that have no values, degenerates (ha)
			next unless $row_data->{update};

			# attempt to store rows that had values
			store_row( $weekday_code, $row_data, 0 )
				or warn "failed to store row $row for $weekday\n";
		}
	}

	# import the DIS mapping
	say 'creating and importing DIS mapping table...';
	import_dis() if $opts{e};
}

# run in daemon mode until interrupted
sub daemon {

	say 'starting daemon...';

	# length of time to sleep before updating report
	# in seconds
	# defaults to 1 minute
	my $update_freq = 60;

	# fork child http server to host report
	fork or server();

	# trap interrupts to prevent exiting mid-update
	$SIG{'INT'} = 'INT_handler';

	# run indefinitely
	# polling spreadsheet and AUH db to update report at specified frequency
	while (1) {

		# lock against interrupt
		$daemon_lock = 1;

		# parse spreadsheet and insert new updates
		refresh_xls();

		# examine AUH metadata and insert new updates
		refresh_auh();

		# check if interrupts were caught
		if ( $daemon_lock > 1 ) {
			say sprintf(
					   'update completed, caught %u interrupts during update',
					   $daemon_lock - 1 );
			say 'interrupting TQASched services';
			exit(0);
		}
		else {
			$daemon_lock = 0;
		}

		sleep($update_freq);
	}
}

# server to be run in another process
# hosts the report webmon
sub server {

	# load webserver module and ISA relationship at runtime in child
	require HTTP::Server::Simple::CGI;
	push @ISA, 'HTTP::Server::Simple::CGI';
	my $server = TQASched->new(80);
	$server->run();

	# just in case server ever returns
	die 'server has returned and is no longer running';
}

# interrupt (Ctrl+C) signal handler
# postpones interrupts recieved during update until finished
sub INT_handler {
	if ($daemon_lock) {

		# count the number of interrupts caught
		# also tracks whether interrupt was caught
		$daemon_lock++;
		say 'SIGINT caught, exiting when daemon releases update lock...';
	}
	else {

		# if not locked, business as usual
		say 'interrupted';
		exit(0);
	}
}

# override request handler for HTTP::Server::Simple
sub handle_request {
	my ( $self, $cgi ) = @_;

	# parse POST into CLI argument key/value pairs
	my $params_string = '';
	for ( $cgi->param ) {
		$params_string .= sprintf( '--%s=%s ', $_, $cgi->param($_) );
	}
	print `perl report.pl $params_string`;
}

# extract row into hash based on column number
sub extract_row {
	my ( $col, $cell, $row_href ) = @_;

	# get formatted excel value for most columns
	my $value = $cell ? $cell->value() : '';
	given ($col) {

		# time scheduled/expected
		when (/^0$/) {
			$row_href->{time_block} = $value if $value;
		}

		# update name (needs to be exactly the same every time)
		when (/^1$/) {
			$row_href->{update} = $value ? $value : return;
		}

		# priority - 'x' if not scheduled for the day
		when (/^2$/) {
			return unless $value;
			$row_href->{priority} = $value =~ m/x/ ? 0 : $value;
		}

		# file date
		when (/^3$/) {
			return unless $value;

			# extract unformatted datetime and convert to filedate integer
			my $time_excel = $cell->unformatted();
			my $value = ExcelFmt( 'yyyymmdd', $time_excel );

			# skip if not scheduled for this day
			$row_href->{filedate} = $value ? $value : return;
		}

		# file number
		when (/^4$/) {
			if ( $value ne '0' ) {
				$row_href->{is_legacy} = 1;
				$row_href->{filenum} = $value ? $value : return;
			}
			else {
				$row_href->{is_legacy} = 0;
			}
		}

		# ID
		when (/^5$/) {
			$row_href->{id} = $value;
		}

		# comment
		when (/^6$/) {
			$row_href->{comment} = $value;
		}

		# outside of parsing scope
		# return and go to next row
		default {return};
	}
	return 1;
}

# analyze and store a row from scheduling spreadsheet in database
sub store_row {
	my $weekday_code = shift;
	my $row_href     = shift;
	my ( $update, $time_block, $priority, $is_legacy, $filedate, $filenum )
		= map { $row_href->{$_} }
		qw(update time_block priority is_legacy filedate filenum);

# if storing history (legacy filedate/filenum) rather than refreshing db image
	my $history_flag = shift;

	# only story legacy data from spreadsheet
	if ($history_flag) {
		return 1 unless $is_legacy;
	}

	# don't store row if not scheduled for today
	# or row is blank
	# but not an error so return true
	return 1
		unless $update =~ m/\w+/
			&& defined $time_block
			&& defined $priority;

	# check if this update name has been seen before
	my ( $update_id, $sched_id );

	# storing history or not
	unless ($history_flag) {
		unless ( $update_id = get_update_id($update) ) {

			warn
				"missing row info update: $update priority: $priority is_legacy: $is_legacy\n"
				unless defined $update && defined $priority && defined $is_legacy;
			# if not, insert it into the database
			my $update_insert = "insert into [TQASched].dbo.[Updates] values 
				('$update','$priority', '$is_legacy')";
			$dbh_sched->do($update_insert)
				or warn
				"error inserting update: $update, probably already inserted\n",
				$dbh_sched->errstr
				and return;

			# get the id of the new update
			$update_id = get_update_id($update)
				or warn "could not retrieve last insert id\n",
				$update, $dbh_sched->errstr
				and return;

		}

		# put entry in scheduling table
		my $time_offset  = time2offset($time_block);
		my $sched_insert = "
			insert into [TQASched].dbo.[Update_Schedule] values 
				('$update_id','$weekday_code','$time_offset')
		";
		$dbh_sched->do($sched_insert)
			or warn
			"failed to insert update schedule info for update: $update\n",
			$dbh_sched->errstr
			and return;

	}
	else {
		unless ( $update_id = get_update_id($update) ) {
			warn "could not find update: $update\n" and return;
		}
		unless ( $sched_id = get_sched_id($update_id) ) {
			warn "could not find sched history ID: $update ID: $update_id\n"
				and return;
		}
		my $time_offset    = now_offset();
		my $history_insert = "
			insert into [TQASched].dbo.[Update_History] values
			('$update_id', '$sched_id', '$time_offset', '$filedate', '$filenum')
		";
		$dbh_sched->do($history_insert)
			or warn "failed to insert update history for udpate: $update\n";
	}

}

# generate time offset for the current time IST
# TODO: resolve times from spreadsheet and GMT
sub now_offset {

	# calculate GM Time
	my ( $sec, $min, $hour, $mday, $mon, $year, $wday, $yday, $isdst )
		= gmtime(time);
	return time2offset("$hour:$min");
}

# retrieve schedule ID from database for update ID
sub get_sched_id {
	my ($update_id) = @_;

	my $res = (
		$dbh_sched->selectrow_arrayref( "
		select top 1 sched_id from [TQASched].dbo.[Update_Schedule]
		where update_id = '$update_id'
	" )
	)[0];
	warn "no schedule id found for $update_id\n" unless $res;
	return $res;
}

# returns code for current weekday
sub now_wd {
	my ( $sec, $min, $hour, $mday, $mon, $year, $wday, $yday, $isdst )
		= gmtime(time);
	my @weekdays = qw(N M T W R F S);
	return $weekdays[$wday];
}

# poll auh metadata for DIS feed statuses
sub refresh_auh {
	my $current_wd     = now_wd();
	my $current_offset = now_offset();

	# get all updates expected for the current day
	my $expected = "
		select ud.feed_id, us.time, us.sched_id, us.update_id
		from 
			Update_Schedule us,
			Update_DIS ud
		where ud.update_id = us.update_id
		and us.weekday = '$current_wd'
		";
	my $sth_expected = $dbh_sched->prepare($expected);
	$sth_expected->execute();
	my $updates_aref = $sth_expected->fetchall_arrayref();

	# iterate over each of them and determine if they are completed
	for my $update_aref ( @{$updates_aref} ) {

		# extract update info
		my ( $feed_id, $name, $offset, $sched_id, $update_id )
			= @{$update_aref};
		my $build_number;
		if ( $name =~ m/#(\d+)/ ) {
			$build_number = $1;
		}

		# get last AUH transaction for this update's feed_id
		my $transactions = sprintf( "
			select top 1 FeedBuildNumber, Status, ExecEnd, TransactionNumber 
			from [DataIngestionInfrastructure].[dbo].[Transactions]
			where feedid = '%s'
			%s
			order by ExecEnd desc", $feed_id,
			( $build_number ? "and FeedBuildNumber = $build_number" : '' ) );
		my $sth_trans = $dbh_auh->prepare($transactions);
		$sth_trans->execute();

		# check ExecEnd value to verify schedule data
		my ( $feed_build_number, $status, $exec_end, $trans_num )
			= $sth_trans->fetchrow_array();

		# convert DateTime to offset and compare against current time
		if ($exec_end) {
			my $trans_offset = datetime2offset($exec_end);

			# no transaction offset means that the last one was a previous day
			if ( !$trans_offset ) {
				next;
			}

			# if it's within an hour of the scheduled time, mark as on time
			if (    $trans_offset <= $offset + $late_threshold
				 || $trans_offset >= $offset - $late_threshold )
			{
				update_history( { update_id    => $update_id,
								  sched_id     => $sched_id,
								  trans_offset => $trans_offset,
								  ontime       => 1,
								  trans_num    => $trans_num
								}
				);
			}

			# otherwise it either has not come in or it is late
			# late
			elsif ( $trans_offset > $offset + $late_threshold ) {
				update_history( { update_id    => $update_id,
								  sched_id     => $sched_id,
								  trans_offset => $trans_offset,
								  ontime       => 0,
								  trans_num    => $trans_num
								}
				);
			}

			# possibly just not recvd yet
			elsif ( $trans_offset < $offset - $late_threshold ) {

				# if current offset is past the threshold, mark it as late
				if ( $current_offset > $offset + $late_threshold ) {
					update_history( { update_id    => $update_id,
									  sched_id     => $sched_id,
									  trans_offset => $trans_offset,
									  ontime       => 0,
									}
					);
				}
			}
			else {
				warn
					"FAILED transaction offset sanity check: $name $offset $trans_offset\n";
				next;
			}
		}
		else {
			warn "no transactions found for $name : sched_id = $sched_id\n";
			next;
		}

	}
}

# store/modify update history entry
sub update_history {
	my $hashref = shift;
	my ( $update_id, $sched_id, $trans_offset, $ontime, $trans_num )
		= ( $hashref->{update_id},    $hashref->{sched_id},
			$hashref->{trans_offset}, $hashref->{ontime},
			$hashref->{trans_num}
		);

	# update if late and not yet recvd
	# or skip if it was already recvd
	# otherwise, insert
	my ( $hist_id, $late, $fd, $fn ) = $dbh_sched->selectrow_array( "
		select hist_id, late, filedate, filenum
		from [TQASched].dbo.Update_History
		where DateDiff(dd, timestamp, GetUTCDate()) < 1
		and sched_id = $sched_id
	" );

	# recvd already, return
	if ( $fd && $fn ) {
		return;
	}

	# late and not recvd, update
	elsif ( $late eq 'Y' && ( !$fd || !$fn ) ) {

		# retrieve filedate and filenum from TQALic on nprod1
		my ( $fd, $fn ) = get_fdfn($trans_num);
		$dbh_sched->do( "
			update Update_History
			set filedate = $fd, filenum = $fn 
			where hist_id = $hist_id
		" );
	}

	# not recvd and never seen, insert
	else {

		# retrieve filedate and filenum from TQALic on nprod1
		my ( $fd, $fn ) = get_fdfn($trans_num);
		$dbh_sched->do( "
			insert into Update_History 
			values
			($update_id, $sched_id, $trans_offset, $fd, $fn, GetUTCDate())
		" );
	}

}

# retrieve filedate and filenum from TQALic on nprod1
sub get_fdfn {
	my $trans_num = shift;
	my ( $fd, $fn ) = $dbh_prod1->selectrow_array( "
		select FileDate, FileNum 
		from PackageQueue
		where TransactionNumber = $trans_num
	" );
	warn "could not find fd/fn for $trans_num\n" unless $fd && $fn;

	return ( $fd, $fn );
}

# convert SQL datetime to offset if it is in the current day
# otherwise return false
sub datetime2offset {
	my ($datetime) = @_;
	my ( $sec, $min, $hour, $mday, $mon, $year, $wday, $yday, $isdst )
		= gmtime(time);
	if ( my ( $dt_year, $dt_month, $dt_day, $dt_hour, $dt_min )
		 = $datetime =~ m/(\d+)-(\d+)-(\d+) (\d+):(\d+)/ )
	{
		if (    $dt_year == $year + 1900
			 && $dt_month == $mon
			 && $dt_day == $mday )
		{
			return time2offset("$dt_hour:$dt_min");
		}

		# no transactions for this feed_id today
		else {
			return;
		}
	}
	else {
		warn "could not covert SQL DateTime to offset: $datetime\n";
		return;
	}

}

# poll ops schedule Excel spreadsheet for legacy feed statuses
sub refresh_xls {

	# create parser and parse xls
	my $xlsparser = Spreadsheet::ParseExcel->new();
	my $workbook  = $xlsparser->parse($sched_xls)
		or die "unable to parse spreadsheet: $sched_xls\n",
		$xlsparser->error();
	say 'done';

	# optionally create database and tables
	( create_db() or die "failed to create database\n" ) if $opts{d};

	# populate database from excel file

	# iterate over each weekday (worksheets)
	for my $worksheet ( $workbook->worksheets() ) {
		my $weekday = $worksheet->get_name();
		say "parsing $weekday...";
		my $weekday_code = code_weekday($weekday);

		# skip if this is an unrecognized worksheet
		say "\tunable to parse weekday, skipping" and next
			if $weekday_code eq 'U';

		# find the row and column bounds for iteration
		my ( $col_min, $col_max ) = $worksheet->col_range();
		my ( $row_min, $row_max ) = $worksheet->row_range();

		my $sched_block = '';

		# iterate over each row and store scheduling data
		for ( my $row = $row_min; $row <= $row_max; $row++ ) {
			next if $row <= 1;

			# per-update hash of column values
			my $row_data = {};
			for ( my $col = $col_min; $col <= $col_max; $col++ ) {
				my $cell = $worksheet->get_cell( $row, $col );
				unless ( extract_row( $col, $cell, $row_data ) ) {

					#last;
				}
				else {
					if (    $row_data->{time_block}
						 && $sched_block ne $row_data->{time_block} )
					{
						$sched_block = $row_data->{time_block};
					}
					else {
						$row_data->{time_block} = $sched_block;
					}
				}
			}

			# skip rows that have no values, degenerates (ha)
			next unless $row_data->{update};

			# attempt to store rows that had values
			store_row( $weekday_code, $row_data, 1 )
				or warn "failed to store row $row for $weekday\n";
		}
	}
}

# convert time of day (24hr) into minute offset from 12:00am
sub time2offset {
	my $time_string = shift;
	my ( $hours, $minutes ) = ( $time_string =~ m/(\d+):(\d+)/ );
	unless ( defined $hours && defined $minutes ) {
		warn "parsing error converting time to offset: $time_string\n";
		return;
	}
	return $hours * 60 + $minutes;
}

# get an update's id from name
sub get_update_id {
	my $name         = shift;
	my $select_query = "
		select update_id from [TQASched].dbo.[Updates] 
		where name = '$name'
	";
	my $id = ( ( $dbh_sched->selectall_arrayref($select_query) )->[0] )->[0];
	return $id;
}

# load db info and optional params from config file
sub load_conf {
	my $conf_file = shift;

	# if file doesn't exist, create
	unless ( -f $conf_file ) {
		die "could not load config file: $conf_file, ",
			( init_conf()
			  ? 'a skeleton config has been created'
			  : 'failed to create skeleton config'
			),
			"\n";
	}

	my $cfg = new Config::Simple($conf_file);

	my $sched_db = $cfg->param( -block => 'sched_db' )
		or die
		"could not load sched database info from config file, check config file\n";
	my $auh_db = $cfg->param( -block => 'auh_db' )
		or die
		"could not load auh database info from config file, check config file\n";
	my $prod1_db = $cfg->param( -block => 'prod1_db' )
		or die
		"could not load prod1 database info from config file, check config file\n";

	return ( $sched_db, $auh_db, $prod1_db );
}

# translate weekday string to code
sub code_weekday {
	my $weekday = shift;
	my $rv;
	given ($weekday) {
		when (/monday/i)    { $rv = 'M' }
		when (/tuesday/i)   { $rv = 'T' }
		when (/wednesday/i) { $rv = 'W' }
		when (/thursday/i)  { $rv = 'R' }
		when (/friday/i)    { $rv = 'F' }
		when (/saturday/i)  { $rv = 'S' }
		when (/sunday/i)    { $rv = 'N' }
		default             { $rv = 'U' };
	}
	return $rv;
}

# create a basic config file for the user to fill in
sub init_conf {
	open( my $conf, '>', 'tqa_sched.conf' )
		or warn "could not create config file: $!\n" and return;
	print $conf '# basic config file
# insert database connection info here
[db]
server=
user=
pwd=
# optional configs
[opts]
# update frequency (in seconds) when running as daemon
update_frequency=60
http_port=8080';
	close $conf;
	return 1;
}

# get handle for master on sql server
sub init_handle {
	my $db = shift;

	# connecting to master since database may need to be created
	return
		DBI->connect(
		sprintf(
			"dbi:ODBC:Driver={SQL Server};Database=%s;Server=%s;UID=%s;PWD=%s",
			'master', $db->{server}, $db->{user}, $db->{pwd},
		)
		) or die "failed to initialize database handle\n", $DBI::errstr;
}

# create database if not already present
sub create_db {

	# if already exists, return
	say 'database already exists, skipping create flag' and return 1
		if check_db('TQASched');
	say 'creating TQASched database...';

	# create the database
	$dbh_sched->do("create database [TQASched]")
		or die "could not create TQASched database\n";

	# create the tables
	$dbh_sched->do(
		"create table [TQASched].dbo.[Updates] (
		update_id int not null identity(1,1),
		name varchar(255) not null unique,
		priority tinyint,
		is_legacy bit
	)"
	) or die "could not create Updates table\n", $dbh_sched->errstr;
	$dbh_sched->do(
		"create table [TQASched].dbo.[Update_Schedule] (
		sched_id int not null identity(1,1),
		update_id int not null,
		weekday char(1),
		time int
	)"
		)
		or die "could not create Update_Schedule table\n", $dbh_sched->errstr;
	$dbh_sched->do(
		"create table [TQASched].dbo.[Update_History] (
		hist_id int not null identity(1,1),
		update_id int not null,
		sched_id int not null,
		time int,
		filedate int,
		filenum tinyint,
		timestamp DateTime,
		late char(1)
	)"
		)
		or die "could not create Update_History table\n", $dbh_sched->errstr;

	say 'done creating db';
	return 1;
}

# check that database exists
sub check_db {
	my $check_query = "select db_id('TQASched')";
	return ( ( $dbh_sched->selectall_arrayref($check_query) )->[0] )->[0];
}

# drop the database
sub drop_db {
	return $dbh_sched->do('drop database TQASched')
		or die "could not drop TQASched database\n", $dbh_sched->errstr;
}

# clear all update records in database
sub clear_updates {
	return $dbh_sched->do('delete from [TQASched].dbo.[Updates]')
		or die "error in clearing Updates table\n", $dbh_sched->errstr;
}

# clear all scheduling records in database
sub clear_schedule {
	return $dbh_sched->do('delete from [TQASched].dbo.[Update_Schedule]')
		or die "error in clearing Schedule table\n", $dbh_sched->errstr;
}

# look for a schedule file in local dir
sub find_sched {
	say 'excel schedule file not specified, searching local directory...';
	opendir( my $dir, '.' );
	my @files = readdir($dir);
	closedir $dir;

	for (@files) {
		say "\tfound: $_" and return $_ if /^DailyCheckList.*xls$/i;
	}
	usage() and die "could not find a schedule spreadsheet\n";
}

sub usage {
	pod2usage( -verbose > 1 );
	print '
	usage: perl gen_db.pl
		-c config file specified
		-d create empty database framework
		-f schedule xls file	
		-h this message
';
}

=pod

=head1 NAME

TQASched - a tool for monitoring both legacy and DIS feed timeliness in AUH.

=head1 SYNOPSIS

perl tqa_sched.pl [optional flags]

=head1 DESCRIPTION

AUH content schedule monitoring tool
Creates and populates a database with content scheduling data from content scheduling spreadsheet
run daemon which reads AUH metadata and operator checklists to generate a feed timeliness report

=head1 OPTIONS

=over 6

=item B<-c>=I<configpath>

Specify path for config file in the command line.
Defaults to tqa_sched.pl and is generated automatically if not found.

=item B<-d>

Create a new TQASched database if not already present.

=item B<-h>

Print usage

=item B<-f>=I<schedulepath>

Specify path to Excel scheduling/checklist document 

=back

=head1 FILES

=over 6

=item F<tqa_sched.pl>

This self-documented script.
It is both a tool and a deamon - 
the only executable component of the TQASched application.
Refer to documentation for usage and configuration.

=item F<tqa_sched.conf>

A config file for the database credentials and other options. 
Path can be specified with -c flag.
Defaults to tqa_sched.pl and is generated automatically if not found.

=item F<DailyChecklist.xls>

Schedule checklist Excel spreadsheet.
This is used for either initializing the TQASched database
or for assigning the I<Filedate> and I<Filenum> of legacy content sets.
The syntax of this document is strict, see example: F<ExampleChecklist.xls>

=back

=head1 AUTHOR

Matt Shockley

=head1 COPYRIGHT AND LICENSE

Copyright 2012 Matt Shockley

This program is free software; you can redistribute it and/or modify 
it under the same terms as Perl itself.

=cut
