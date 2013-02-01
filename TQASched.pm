#! perl -w

package TQASched;

use strict;
use feature qw(say switch);
use Spreadsheet::ParseExcel;
use Spreadsheet::ParseExcel::Utility qw(ExcelFmt);
use DBI;
use Date::Manip qw(ParseDate DateCalc Delta_Format UnixDate);
use Pod::Usage qw(pod2usage);
use AppConfig qw(:argcount);
use Exporter 'import';

# opts d,i,e for initialization (drop, create, and populate db)
# opt s for daemon mode (with webserver)
# opt t to run only webserver (for testing)
# no CLI args assumes

# stuff to export to portal and daemon
our @EXPORT
	= qw(load_conf refresh_handles kill_handles write_log usage @dbhs);

# shared db handle variables
our @dbhs = our ( $dbh_sched, $dbh_auh,  $dbh_prod1, $dbh_dis1,
				  $dbh_dis2,  $dbh_dis3, $dbh_dis4,  $dbh_dis5 );

# return if being imported as module rather than run directly
# INV: experimental... does this work in a require? I think so
if (caller) {
	say 'imported TQASched module for your personal enjoyment';
	return 1;
}

################################################################################
# Notice Posted:
# anything beyond this point is the executable portion of this module
# tread lightly -
# do not flagrantly call flags or risk corrupting/losing scheduling data
#################################################################################

say 'TQASched module running in direct control mode, feel the POWER!';

my $cfg;
say 'parsing CLI and file configs (om nom nom)...';
$cfg = load_conf();

usage() if $cfg->help;

say 'gonna initialize a fresh crop of database handles...';

# private variables associated with different db handles
my ( $sched_db, $auh_db,  $prod1_db, $dis1_db,
	 $dis2_db,  $dis3_db, $dis4_db,  $dis5_db );

say '	*dial-up modem screech* (sorry)';

# refresh those handles for the first time
# just to make sure that any and all subs have live handles
refresh_handles();

say 'fnished, all warmed up and revving to go ^_^';

# exit here if this is just a basic module load test - dryrun
if ( $cfg->dryrun ) {
	say 'dryrun complete - run along now';
	exit;
}

# no CLI args provided (unusual), give a cute little warning
elsif ( scalar @ARGV ) {
	say
		"well, you did NOT give me any explicit commands so I hope ${\$cfg->conf_file} tells me what to do";
}

say 'moving on to user request(s)...';

# initialize scheduling database from blank Excel file
init_sched() if $cfg->init_sched;

# test web server mode
server() if $cfg->start_server;

# run in daemon (server) mode
daemon() if $cfg->start_daemon;

say 'finished with all requests - prepare to be returned THE TRUTH';

# THE TRUTH
1;

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
		or warn
		"\tcould not create DIS linking table - Update_DIS, may already exist\n";

	# open mapping file, populate mapping table
	open( MAP, '<', 'mapping.csv' );
	while (<MAP>) {
		chomp;
		my ( $name, $feed_id, $dis );
		if (m/"(.*)",(.*)/) {
			( $name, $feed_id ) = ( $1, $2 );
		}
		else {
			( $name, $feed_id ) = split ',';
		}

		my $update_id = get_update_id($name);

		#	or
		#	warn "could not find update_id for $name\n"
		#	and next;

		# if update_id not found, probably an enumerated feed
		unless ($update_id) {

			# attempt to retrieve all enumerations
			my $rows_aref = $dbh_sched->selectall_arrayref( "
				select update_id, name
				from [TQASched].dbo.[Updates]
				where name like '$name%'
			" )
				or warn "\terror attempting to select enums for $name\n";

			# this is unrecognizable... warn and go to next
			( warn "\tsanity check failed for $name - not an enum\n"
			   and next )
				unless @$rows_aref;

			# iterate over enumerations in feed and link to the same feed_id
			for my $row_aref ( @{$rows_aref} ) {
				my ( $update_id, $name ) = @{$row_aref};
				$dbh_sched->do( "
					insert into [TQASched].dbo.[Update_DIS]
					values ('$feed_id',$update_id)
				" )
					or warn
					"\tcould not insert mapping for : $feed_id / $update_id -> $name\n";
			}
		}

		# otherwise store mapping
		else {
			$dbh_sched->do( "
				insert into [TQASched].dbo.[Update_DIS]
				values ('$feed_id',$update_id)
			" )
				or warn
				"\tcould not insert mapping for : $feed_id / $update_id -> $name\n";
		}
	}
	close MAP;
}

# fill database with initial scheduling data
sub init_sched {

	my $sched_xls = $cfg->sched;

	# create parser and parse xls
	my $xlsparser = Spreadsheet::ParseExcel->new();
	my $workbook  = $xlsparser->parse($sched_xls)
		or die "unable to parse spreadsheet: $sched_xls\n",
		$xlsparser->error();
	say 'done';

	# optionally create database and tables
	( create_db() or die "failed to create database\n" ) if $cfg->create_db;

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
		   # also skip rows that have 'x' priority, not scheduled for that day
			next if !$row_data->{update} || $row_data->{priority} eq 'x';

			# attempt to store rows that had values
			store_row( $weekday_code, $row_data, 0 )
				or warn "\tfailed to store row $row for $weekday\n";
		}
	}

	# import the DIS mapping
	import_dis() if $cfg->import_dis;
}

# intialize new database handles
# should be called often enough to keep them from going stale
# especially for long-running scripts (daemon)
sub refresh_handles {

	(  $dbh_sched, $dbh_auh,  $dbh_prod1, $dbh_dis1,
	   $dbh_dis2,  $dbh_dis3, $dbh_dis4,  $dbh_dis5 )
		= map { init_handle($_) } ( $sched_db, $auh_db,  $prod1_db, $dis1_db,
									$dis2_db,  $dis3_db, $dis4_db,  $dis5_db
		);
}

# close database handles
sub kill_handles {
	my @handles = @_;
	map { $_->disconnect } @handles;
}

# server to be run in another process
# hosts the report webmon
sub server {
	exec;
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
			$row_href->{priority} = $value;
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
				"\tmissing row info update: $update priority: $priority is_legacy: $is_legacy\n"
				unless defined $update
					&& defined $priority
					&& defined $is_legacy;

			# if not, insert it into the database
			my $update_insert = "insert into [TQASched].dbo.[Updates] values 
				('$update','$priority', '$is_legacy')";
			$dbh_sched->do($update_insert)
				or warn
				"\terror inserting update: $update, probably already inserted\n",
				$dbh_sched->errstr
				and return;

			# get the id of the new update
			$update_id = get_update_id($update)
				or warn "\tcould not retrieve last insert id\n",
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
			"\tfailed to insert update schedule info for update: $update\n",
			$dbh_sched->errstr
			and return;

	}
	else {
		unless ( $update_id = get_update_id($update) ) {
			warn "\tcould not find update: $update\n" and return;
		}
		unless ( $sched_id = get_sched_id($update_id) ) {
			warn "\tcould not find sched history ID: $update ID: $update_id\n"
				and return;
		}
		my $time_offset = now_offset();
		my ( $filedate, $filenum ) = my $history_insert = sprintf( "
			insert into [TQASched].dbo.[Update_History] values
			('$update_id', '$sched_id', '$time_offset', %s, %s, GetUTCDate(), )
		", ( $filedate ? "$filedate" : 'NULL' ),
			( $filenum ? "$filenum" : 'NULL' ) );
		$dbh_sched->do($history_insert)
			or warn "\tfailed to insert update history for udpate: $update\n";
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

	my ($res) = $dbh_sched->selectrow_array( "
		select top 1 sched_id from [TQASched].dbo.[Update_Schedule]
		where update_id = '$update_id'
	" );
	warn "\tno schedule id found for $update_id\n" unless $res;
	return $res;
}

# returns code for current weekday
sub now_wd {
	my ( $sec, $min, $hour, $mday, $mon, $year, $wday, $yday, $isdst )
		= gmtime(time);
	my @weekdays = qw(N M T W R F S);
	return $weekdays[$wday];
}

# update all older builds issued in the same update
sub backdate {
	my ( $backdate_updates, $trans_offset, $late, $fd, $fn, $build_num,
		 $orig_sched_id )
		= @_;

	for my $backdate_rowaref ( @{$backdate_updates} ) {
		my ( $sched_id, $name, $update_id, $filedate ) = @{$backdate_rowaref};
		my ($bn) = $name =~ m/#(\d+)/;

# only backdate earlier build numbers which have no history yet and are scheduled for earlier in the day
		next
			unless $bn < $build_num
				&& !$filedate
				&& $orig_sched_id > $sched_id;
		say "backdating $name - $bn";
		update_history( { update_id    => $update_id,
						  sched_id     => $sched_id,
						  trans_offset => $trans_offset,
						  late         => $late,
						  filedate     => $fd,
						  filenum      => $fn
						}
		);
	}
}

# verify that an offset falls earlier in the day (based on spreadsheet days)
sub offset_before {
	my ( $orig, $curr ) = @_;

	if ( 1260 <= $orig && $orig < 1440 ) {
		if ( 1260 <= $curr && $curr < 1440 && $curr <= $orig ) {
			return 1;
		}
	}

	# Afternoon adjust (current day, both CST and GMT)
	# GMT 1080 - 1439
	# CST 0    - 359
	elsif ( $orig >= 0 && $orig < 360 ) {
		if ( $curr >= 0 && $curr < 360 && $curr <= $orig ) {
			return 1;
		}
	}

	# Evening adjust (next day, GMT)
	# GMT 0    - 840
	# CST 360  - 1259
	elsif ( $orig >= 360 && $orig < 1260 ) {
		if ( $orig >= 360 && $orig < 1260 && $curr <= $orig ) {
			return 1;
		}
	}

	return 0;
}

# compares offsets for timezone and day diff
# to GMT
sub comp_offsets {

	# (		GMT		 ,		CST 	)
	my ( $trans_offset_ts, $sched_offset ) = @_;

	#my ( $trans_offset, $date_flag ) = datetime2offset($trans_offset_ts);

	my $parsed_trans = ParseDate($trans_offset_ts);
	my $sched_string = offset2time($sched_offset);

	my $parsed_sched = ParseDate("$sched_string")
		or warn "DM parser error\n";
	my $schedule_adjust = 6;

	# Morning adjust (end of previous day, CST)
	# GMT 900  - 1079
	# CST 1260 - 1439
	# TODO: check that this is the previous day CST
	if ( 1260 <= $sched_offset && $sched_offset < 1440 ) {
		$schedule_adjust -= 24;

	}

	# Afternoon adjust (current day, both CST and GMT)
	# GMT 1080 - 1439
	# CST 0    - 359
	# TODO: verify that this is the current day
	elsif ( $sched_offset >= 0 && $sched_offset < 360 ) {

	}

	# Evening adjust (next day, GMT)
	# GMT 0    - 840
	# CST 360  - 1259
	# TODO: check that this is the next day GMT
	elsif ( $sched_offset >= 360 && $sched_offset < 1260 ) {

	}

	# adjust CST to GMT
	$parsed_sched = DateCalc( "$sched_string", "in $schedule_adjust hours" )
		or warn "parse 4\n";

	my $date_delta = DateCalc( $parsed_sched, $parsed_trans )
		or warn "parse 5\n";

	my $hrs_diff = Delta_Format( $date_delta, 2, '%ht' )
		or warn "parse 6\n";

	$hrs_diff = -$hrs_diff if $trans_offset_ts =~ m/1900-01-01/;
	say $hrs_diff;

	my $late_threshold = $cfg->late_threshold;

	# arrived a period after the late threshold, late
	if ( $hrs_diff >= $late_threshold ) {
		return 1;
	}

	# arrived earlier in the day
	elsif ( $hrs_diff < $late_threshold && $hrs_diff >= -24 ) {
		return 0;
	}

	# assume this is previous transaction, has not arrived yet today
	# check current time against schedule to see if it was late
	else {
		my $now_gmt = DateCalc( 'now', 'in 6 hours' );
		$date_delta = DateCalc( $parsed_sched, $now_gmt );
		$hrs_diff = Delta_Format( $date_delta, 2, '%ht' );
		say "still waiting $hrs_diff";

		# late, but not received yet
		if ( $hrs_diff >= $late_threshold ) {
			return 1;
		}

		# still waiting
		return -1;
	}
}

sub offset2time {
	my $offset = shift;

	my $hours   = int( $offset / 60 );
	my $minutes = $offset - $hours * 60;
	return sprintf '%02u:%02u', $hours, $minutes;
}

# parse the feed sender as specified by TQALic
# return appropriate DIS server db handle
sub sender2dbh {
	my ($sender) = @_;

	my $server = 0;

	# make sure this is from DIS1, as all DIS content should be
	if ( $sender =~ m/NTCP-DIS1-NTCP-(.*)/ ) {

		# DIS 1..3
		if ( $sender =~ m/DIS(\d+)$/ ) {
			$server = $1;
		}

		# TINTRIN 3,5 = DIS 4,5
		elsif ( $sender =~ m/TINTRIN(\d+)$/ ) {
			my $tt_server = $1;
			if ( $tt_server == 3 ) {
				$server = 4;
			}
			elsif ( $tt_server == 5 ) {
				$server = 5;
			}
			else {
				warn "unrecognized TINTRIN server: $sender\n";
			}
		}
		else {
			warn
				"sanity check failed on DIS sender $sender, unable to match server\n";
		}
	}
	else {
		warn "DIS feed sender $sender somehow not sent from DIS1\n";
	}

	for ($server) {
		when (/1/) { return $dbh_dis1 }
		when (/2/) { return $dbh_dis2 }
		when (/3/) { return $dbh_dis3 }
		when (/4/) { return $dbh_dis4 }
		when (/5/) { return $dbh_dis5 }
		default    {return};
	}
}

# store/modify update history entry
sub update_history {
	my $hashref = shift;
	my ( $update_id, $sched_id, $trans_offset, $late_q, $fd_q, $fn_q )
		= ( $hashref->{update_id},    $hashref->{sched_id},
			$hashref->{trans_offset}, $hashref->{late},
			$hashref->{filedate},     $hashref->{filenum}
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
		say "already stored $update_id : $sched_id";
		return;
	}

# already an entry in history (late), update with newly found filedate filenum
	elsif ( defined $hist_id && ( $fd_q && $fn_q ) && ( !$fd || !$fn ) ) {
		say "$update_id updating";
		$dbh_sched->do( "
			update TQASched.dbo.Update_History
			set filedate = $fd_q, filenum = $fn_q 
			where hist_id = $hist_id
		" );

	}

	# not recvd and never seen, insert new record w/ filedate and filenum
	elsif ( !$hist_id && $fd_q && $fn_q ) {
		say "$update_id inserting";

		# retrieve filedate and filenum from TQALic on nprod1
		#my ( $fd, $fn ) = get_fdfn($trans_num);
		my $insert_hist = "
			insert into TQASched.dbo.Update_History 
			values
			($update_id, $sched_id, $trans_offset, $fd_q, $fn_q, GetUTCDate(), '$late_q')
		";

		#say $insert_hist;
		$dbh_sched->do($insert_hist);
	}

	# otherwise, it is late and has no filedate filenum, insert
	else {
		my $insert_hist = "
			insert into TQASched.dbo.Update_History 
			values
			($update_id, $sched_id, $trans_offset, NULL, NULL, GetUTCDate(), '$late_q')
		";
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
	warn "\tcould not find fd/fn for $trans_num\n" unless $fd && $fn;

	return ( $fd, $fn );
}

# convert SQL datetime to offset if it is in the current day
# otherwise return false
# TODO: fix this to handle GM and CST date properly, prev day and next day
sub datetime2offset {
	my ($datetime) = @_;

	$datetime =~ m/ (\d+):(\d+):/;
	return time2offset("$1:$2");

	my $parsed_now = ParseDate( 'epoch ' . gmtime );

	if ( my $parsed_date = ParseDate($datetime) ) {

		my $delta = DateCalc( $parsed_date, $parsed_now );
		my $hrs_diff = Delta_Format( $delta, 2, '%ht' );
		my $hours_mins = UnixDate( $parsed_date, '%H:%M' );
		my $offset = time2offset($hours_mins);

		# exactly the same day
		if ( $hrs_diff < 24 && $hrs_diff > -24 ) {
			return ( $offset, 0 );
		}

		# previous day
		elsif ( $hrs_diff < -24 && $hrs_diff > -48 ) {
			return ( $offset, -1 );
		}

		# next day
		elsif ( $hrs_diff > 24 && $hrs_diff < 48 ) {
			return ( $offset, 1 );
		}

		# no transactions for this feed_id today
		else {
			say
				"\t$datetime diff: $hrs_diff parsed: $hours_mins offset: $offset";
			return;
		}
	}
	else {
		warn "\tcould not parse SQL DateTime: $datetime\n";
		return;
	}

}

# convert time of day (24hr) into minute offset from 12:00am
# TODO: compensate for GMT vs CST times
sub time2offset {
	my $time_string = shift;
	my ( $hours, $minutes ) = ( $time_string =~ m/(\d+):(\d+)/ );
	unless ( defined $hours && defined $minutes ) {
		warn "\tparsing error converting time to offset: $time_string\n";
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

# (re)loads configs
sub load_conf {

	$cfg = AppConfig->new( { CREATE => 1,
							 GLOBAL => { ARGCOUNT => ARGCOUNT_ONE,
										 DEFAULT  => "<undef>",
							 },
						   }
	);

	# $cfg->define() any default values and set their options
	define_defaults();

	# backup CLI args
	my @CLI = @ARGV;

	# first pass at CLI args, mostly checking for config file setting
	$cfg->getopt();

	# parse config file
	$cfg->file( $cfg->config_file() );

	# second pass at CLI args, they take precedence over config file
	$cfg->getopt( \@CLI );
	(  $sched_db, $auh_db,  $prod1_db, $dis1_db,
	   $dis2_db,  $dis3_db, $dis4_db,  $dis5_db )
		= map { get_handle_hash($_) } qw(sched_db auh_db prod1_db 1 2 3 4 5);

	return $cfg;
}

sub define_defaults {
	my %config_vars = ();

	%config_vars = (
		%config_vars,
		(

			# server configs
			# server host port ex: localhost:9191
			server_port => { DEFAULT => 9191,
							 ARGS    => '=s',
							 ALIAS   => 'host_port|port|p',
			},

   # server auto-start, good to set in conf file once everything is running OK
			server_start => { DEFAULT => 0,
							  ARGS    => '!',
							  ALIAS   => 'start_server|s',
			},

			# server logfile path
			server_logfile => { DEFAULT => 'server.log',
								ALIAS   => 'server_log',
			},

			# path to script which prints content
			# this content is hosted through TCP/IP under HTTP
			server_hosted_script => {
						DEFAULT => 'test.pl',
						ALIAS => 'hosted_script|target_script|content_script',
			},

   # daemon configs
   # daemon auto-start, good to set in conf file once everythign is running OK
			daemon_start => { DEFAULT => 0,
							  ARGS    => '!',
							  ALIAS   => 'start_daemon|d'
			},

			# periodicity of the daemon loop (seconds to sleep)
			daemon_update_frequency => { DEFAULT => 60,
										 ALIAS   => 'update_freq',
			},

			# daemon logfile path
			daemon_logfile => { DEFAULT => 'daemon.log',
								ALIAS   => 'daemon_log',
			},

			# scheduling configs
			#
			# path to master schedule spreadsheet
			sched_file => { DEFAULT => find_sched('.'),
							ALIAS   => 'sched',
			},

			# path to the operator legacy update checklist
			sched_checklist_path => { DEFAULT => '.',
									  ALIAS   => 'checklist',
			},

			# initialize scheduling data
			# parse master schedule
			# insert scheduling records and metadata into db
			sched_init => { DEFAULT => 0,
							ARGS    => '!',
							ALIAS   => 'init_sched|i',
			},

	   # create scheduling the scheduling database framework from scratch, yum
			sched_create_db => { DEFAULT => 0,
								 ARGS    => '!',
								 ALIAS   => 'create_db|c',
			},

# report (content gen script) configs
#
# path to css stylesheet file for report gen, hosted statically and only by request!
# all statically hosted files are defined relative to the TQASched/Resources/ directory, where they enjoy living (for now, bwahahaha)
			report_stylesheet => { DEFAULT => 'styles.css',
								   ALIAS   => 'styles|stylesheet',
			},

	  # path to jquery codebase (an image of it taken sometime in... Jan 2013)
			report_jquery => { DEFAULT => 'jquery.js',
							   ALIAS   => 'jquery',
			},

			# path to
			report_user_js => { DEFAULT => 'js.js',
								ALIAS   => 'user_js',
			},

		 # default (misc) configs
		 #
		 # toggle or set verbosity level to turn off annoying, snarky messages
			default_verbosity => { DEFAULT => 0,
								   ARGS    => ':0',
								   ALIAS   => 'verbosity|verbose|v',
			},

			# toggle logging
			default_enable_logging => { DEFAULT => 1,
										ARGS    => '!',
										ALIAS => 'logging|logging_enabled|l',
			},

			# timezone to write log timestamps in
			default_log_tz => { DEFAULT => 'local',
								ALIAS   => 'tz|timezone',
			},

			# helpme / manpage from pod
			default_help => { DEFAULT => 0,
							  ALIAS   => 'help|h|version|usage'
			},

# path to config file
# (optional, I suppose if you wanted to list all database connection info in CLI args)
			default_config_file => {
								  DEFAULT => "TQASched.conf",
								  ARGS    => '=s',
								  ALIAS => "cfg_file|conf_file|config_file|f",
			},

# toggle dryrun mode = non-destructive test of module load and all db connections
			default_dryrun => { DEFAULT => 0,
								ARGS    => '!',
								ALIAS   => 'dryrun|y',
			}
		)
	);

	$cfg->define( $_ => \%{ $config_vars{$_} } ) for keys %config_vars;
}

# build and return hash of db connection info from configs
sub get_handle_hash {
	my ($db_name) = (@_);
	return { name => ( $cfg->get("${db_name}_name") ? $cfg->get("${db_name}_name") : 'master'),
			 user => $cfg->get("${db_name}_user"),
			 server => $cfg->get("${db_name}_server"),
			 pwd    => $cfg->get("${db_name}_pwd"),
	};
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
		or warn "\tcould not create config file: $!\n" and return;
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
			"dbi:ODBC:Database=%s;Driver={SQL Server};Server=%s;UID=%s;PWD=%s",
			$db->{name} || 'master', $db->{server},
			$db->{user}, $db->{pwd}
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

	#say 'excel schedule file not specified, searching local directory...';
	opendir( my $dir, shift );
	my @files = readdir($dir);
	closedir $dir;

	for (@files) {
		say "\tfound: $_" and return $_ if /^DailyCheckList.*xls$/i;
	}
	usage() and die "could not find a schedule spreadsheet\n";
}

sub write_log {
	my $entry_href = shift;

	return unless $cfg->logging();

	( warn "Passed non-href value to write_log\n" and return )
		unless ( ref($entry_href) eq 'HASH' );

	my %entry = %{$entry_href};

	open my $log_fh, '>>', $entry{logfile}
		or warn
		"unable to open/create log $entry{logfile}: [$entry{type}]\t$entry{msg}\n"
		and return;
	printf $log_fh "[%s]\t[%s]\t%s\n", timestamp(), $entry{type}, $entry{msg};
	close $log_fh;
}

sub usage {
	my ($exit_val) = @_;
	pod2usage( { -verbose => 1,
				 -exit    => $exit_val || 0
			   }
	);
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
