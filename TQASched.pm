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
	= qw(load_conf refresh_handles kill_handles write_log usage redirect_stderr exec_time @db_hrefs @CLI);

# for saving @ARGV values for later consumption
our @CLI = @ARGV;

our @db_hrefs = my ( $sched_db, $auh_db,  $prod1_db, $dis1_db,
					 $dis2_db,  $dis3_db, $dis4_db,  $dis5_db );

# return if being imported as module rather than run directly - also snarky import messages are fun
# INV: experimental... does this work in a use/require? I think so!
if ( my @subscript = caller ) {

	# shut up if this is loaded by the report, you'll screw with the protocol!
	# otherwise - loud and proud
	say
		'imported TQASched module for your very own personal amusement! enjoy, pretty boy.'
		unless $subscript[1] =~ m/report/;
	return 1;
}

################################################################################
# Notice Posted:
# anything beyond this point is the executable portion of this module
# tread lightly -
# do not flagrantly call flags or risk corrupting/losing scheduling data, RTFM!
#################################################################################

say
	'TQASched module running in direct control mode, can you feel the POWER?!';

say 'parsing CLI and file configs (om nom nom)...';

# the ever-powerful and needlessly vigilant config variable - seriously
my $cfg = load_conf();

# no verbosity check! too bad i can't unsay what's been say'd
# send all these annoying remarks to dev/null, or close as we can get
disable_say() unless $cfg->verbose;

# user has requested some help. or wants to read the manpage. fine.
usage() if $cfg->help;

say 'initializing and nurturing a fresh crop of database handles...';

say '	*dial-up modem screech* (apologies, running old tech)';

# refresh those handles for the first time
# just to make sure that any and all subs have live handles
my ( $dbh_sched, $dbh_auh,  $dbh_prod1, $dbh_dis1,
	 $dbh_dis2,  $dbh_dis3, $dbh_dis4,  $dbh_dis5
) = refresh_handles();

say 'finished. TQASched all warmed up and revving to go go go ^_^';

# exit here if this is just a basic module load test - dryrun
my $num_args = scalar @CLI;
if ( $cfg->dryrun ) {
	if ( $num_args > 1 ) {
		say
			'detected possible unconsumed commandline arguments and nolonger hungry';
	}
	say sprintf
		'dryrun completed in %u seconds. run along now little technomancer',
		exec_time();
	exit;
}

# no CLI args provided (unusual), give a cute little warning
elsif ($num_args) {
	say
		"no explicit arguments? sure hope ${\$cfg->conf_file} tells me what to do, oh silent one";
}

# let them know we're watching (if only barely)
say sprintf "knocking out user request%s, if any...",
	( $num_args > 1 ? 's' : '' );

# initialize scheduling database from master schedule Excel file
init_sched() if $cfg->init_sched;

# start web server and begin hosting web application
server() if $cfg->start_server;

# start daemon keeping track of scheduling
daemon() if $cfg->start_daemon;
say 'finished with all requests - prepare to be returned THE TRUTH';

# THE TRUTH (oughta be 42, but that's 41 too many for perlwarn's liking)
1;

####################################################################################
#	subs - currently in no particular order
#		with only mild attempts at grouping similar code
####################################################################################

# quick sub for getting current execution time
sub exec_time {
	return time - $^T;
}

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
	return ( $dbh_sched, $dbh_auh,  $dbh_prod1, $dbh_dis1,
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

# current timestamp SQL DateTime format for GMT or machine time (local)
sub timestamp {
	my @now
		= $cfg->tz() =~ m/(?:GM[T]?|UT[C]?)/i
		? gmtime(time)
		: localtime(time);
	return
		sprintf "%4d-%02d-%02d %02d:%02d:%02d",
		$now[5] + 1900,
		$now[4] + 1,
		@now[ 3, 2, 1, 0 ];
}

# (re)loads configs from an optional relative path for sub-script callers
sub load_conf {
	my ($relative_path) = (@_);

	$cfg = AppConfig->new( { CREATE => 1,
							 ERROR  => \&appconfig_error,
							 GLOBAL => { ARGCOUNT => ARGCOUNT_ONE,
										 DEFAULT  => "<undef>",
							 },
						   }
	);

	# $cfg->define() any default values and set their options
	define_defaults();

# first pass at CLI args, mostly checking for config file setting (note - consumes @ARGV)
	$cfg->getopt();

# parse config file for those vivacious variables and their rock steady, dependable values
	$cfg->file( ( defined $relative_path ? "$relative_path/" : '' )
				. $cfg->config_file() );

	# second pass at CLI args, they take precedence over config file
	$cfg->getopt( \@CLI );
	(  $sched_db, $auh_db,  $prod1_db, $dis1_db,
	   $dis2_db,  $dis3_db, $dis4_db,  $dis5_db )
		= map { get_handle_hash($_) } qw(sched_db auh_db prod1_db 1 2 3 4 5);

	return $cfg;
}

# handle any errors in AppConfig parsing - namely log them
sub appconfig_error {

	# hacky way to force always writing this log to top-level dir
	# despite the calling script's location
	my $top_log
		= ( __PACKAGE__ ne 'TQASched'
			? $INC{'TQASched.pm'} =~ s!\w+\.pm!!gr
			: '' )
		. $cfg->log;

	write_log(
		{ logfile => $top_log,
		  type    => 'WARN',
		  msg     => join( "\t", @_ ),
		}
	);
}

sub define_defaults {
	my %config_vars = (

		# server configs
		# server host port ex: localhost:9191
		server_port => { DEFAULT => 9191,
						 ARGS    => '=i',
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
		sched_file => { DEFAULT => 'TQA_Update_Schedule.xls',
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
		# link update ids to feed ids in DIS 
		sched_import_dis => {
			DEFAULT => 0,
			ARGS => '!',
			ALIAS => 'import_dis|m'
		},
		
		# report (content gen script) configs
		# report script's logfile
		report_logfile => {
			DEFAULT => 'report.log',
			ALIAS   => 'report_log',

		},

# path to css stylesheet file for report gen, hosted statically and only by request!
# all statically hosted files are defined relative to the TQASched/Resources/ directory, where they enjoy living (for now, bwahahaha)
		report_stylesheet => { DEFAULT => 'styles.css',
							   ALIAS   => 'styles|stylesheet',
		},

# path to jquery codebase (an image of it taken sometime in... Jan 2013) - not in use yet
		report_jquery => { DEFAULT => 'jquery.js',
						   ALIAS   => 'jquery',
		},

	# path to user created javascript libraries and functions - not in use yet
		report_user_js => { DEFAULT => 'js.js',
							ALIAS   => 'user_js',
		},

		# refresh rate for report page
		report_refresh => { DEFAULT => '300',
							ALIAS   => 'refresh',
		},

		# report date CGI variable
		report_date => { DEFAULT => '',
						 ARGS    => '=i',
						 ALIAS   => 'date',
		},

# refresh rate for the report page - can't be less than 10, and 0 means never.
# (in seconds)

		# default (misc) configs
		#
		# toggle or set verbosity level to turn off annoying, snarky messages
		default_verbosity => { DEFAULT => 1,
							   ARGS    => ':i',
							   ALIAS   => 'verbosity|verbose|v',
		},

		# toggle logging
		default_enable_logging => { DEFAULT => 1,
									ARGS    => '!',
									ALIAS   => 'logging|logging_enabled|l',
		},

		# timezone to write log timestamps in
		default_log_tz => { DEFAULT => 'local',
							ALIAS   => 'tz|timezone',
		},

		# helpme / manpage from pod
		default_help => { DEFAULT => 0,
						  ARGS    => '!',
						  ALIAS   => 'help|version|usage|h'
		},

# path to config file
# (optional, I suppose if you wanted to list all database connection info in CLI args)
		default_config_file => { DEFAULT => "TQASched.conf",
								 ARGS    => '=s',
								 ALIAS => "cfg_file|conf_file|config_file|f",
		},

# toggle dryrun mode = non-destructive test of module load and all db connections
		default_dryrun => { DEFAULT => 0,
							ARGS    => '!',
							ALIAS   => 'dryrun|y',
		},
		default_logfile => { DEFAULT => 'TQASched.log',
							 ALIAS   => 'log',
		}
	);

	$cfg->define( $_ => \%{ $config_vars{$_} } ) for keys %config_vars;
}

# build and return hashref of db connection info from configs
sub get_handle_hash {
	my ($db_name) = (@_);
	return { name   => $cfg->get("${db_name}_name"),
			 user   => $cfg->get("${db_name}_user"),
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
		sched_epoch DateTime not null
	)"
		)
		or die "could not create Update_Schedule table\n", $dbh_sched->errstr;
	$dbh_sched->do(
		"create table [TQASched].dbo.[Update_History] (
		hist_id int not null identity(1,1),
		update_id int not null,
		sched_id int not null,
		hist_epoch DateTime,
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

# get latest schedule checklist
sub find_sched {
	opendir( my $dir_fh, $cfg->checklist );
	my @files = readdir($dir_fh);
	closedir $dir_fh;

	# TODO: find latest, create new (copy & rename blank checklist)
	for (@files) {
		say "\tfound: $_" and return $_ if /^DailyCheckList.*xls$/i;
	}
	write_log( { logfile => $cfg->log, type => 'ERROR', msg => } );
}

# write a severity/type tagged message to target logfile
sub write_log {
	my $entry_href = shift;

	# bounce for logging toggle
	return unless $cfg->logging;

	# bounce for badly formed argument
	( warn "Passed non-href value to write_log\n" and return )
		unless ( ref($entry_href) eq 'HASH' );

	# let's just make sure we're all lower case keys here and save a headache
	my %entry = map { ( lc $_ => ${$entry_href}{$_} ) } keys %{$entry_href};

	# verbosity bounce for INFO tags
	return if !$cfg->verbose && uc $entry{type} eq 'INFO';

	open my $log_fh, '>>', $entry{logfile}
		or warn
		"unable to open/create log $entry{logfile}: [$entry{type}]\t$entry{msg}\n"
		and return;
	printf $log_fh "[%s]\t[%s]\t%s\n", timestamp(), $entry{type}, $entry{msg};
	close $log_fh;
}

# STDERR redirects to file if being run from the module
sub redirect_stderr {
	use IO::Handle;
	my ($error_log) = (@_);
	open my $err_fh, '>>', $error_log;
	STDERR->fdopen( $err_fh, 'a' )
		or warn "failed to pipe errors to logfile:$!\n";

	#return $err_fh;
}

sub usage {
	my ($exit_val) = @_;
	pod2usage( { -verbose => $cfg->verbosity,
				 -exit    => $exit_val || 0
			   }
	);
}

=pod

=head1 NAME

TQASched - a module for monitoring both legacy and DIS feed timeliness using AUH metadata

=head1 SYNOPSIS

perl TQASched.pm [optional flags]

=head1 DESCRIPTION

AUH content schedule monitoring module
the module itself contains all utility functions for this application
however, only really needs to be called directly to initialize app database and do testing

capable of running all or in part the sub-scripts which support the application:

=over 4

=item F<Server/server.pl>

HTTP server script which serves the report and any other files

=item F<Daemon/daemon.pl>

daemon which cyclicly compares AUH metadata against scheduling rules and updates TQASched db accordingly

=item F<Server/report.pl>

script which dynamically generates the web application interface HTML 

=back

=head3 COMPONENTS:

=over 4 

=item B<server>

start/debug http server (and by extension, the report)
	
=item B<daemon>

start/debug scheduling daemon
	
=item B<report>

generate report snapshot without running the server
  
=back

=head1 OPTIONS

=over 6

=item B<-c --create-db>

create database from scratch

=item B<-d --start_daemon>

fork the scheduling monitoring daemon script after startup

=item B<-f --config-file>=I<configpath>

specify path for config file in the command line
defaults to TQASched.conf in current dir

=item B<-h --help --version>

print this manpage and exit

=item B<-i --init_sched> 

initialize schedule from master spreadsheet

=item B<-l --logging>

logging toggle, on/off

=item B<-p --port>=I<portnumber>

specify port the server hosts the web application on

=item B<-s --start_server>

fork the http server script to begin hosting the report script

=back

=head1 FILES

=over 6

=item F<TQASched.pm>

this self-documented module, you're reading the manpage for it right now! 
refer to the rest of the documentation for usage and configuration details

=item F<TQASched.conf>

C<.ini> style config file primarily for the database credentials but is capable of setting any other configuration variables as well

=item F<Daemon/daemon.pl>

daemon which cyclicly compares AUH metadata against scheduling rules and updates TQASched db accordingly
daemon logs can be found in this subdirectory

=item F<Server/server.pl>

server script (also a daemon in its own right)
hosts the output of the report file - the HTML webapp frontend
also hosts various static files (css, js, generated xls files, etc.)
server logs can be found in this subdirectory

=item F<Server/report.pl>

report script which dynamically generates HTML web application content based on the TQASched db
report logs can be found in this subdirectory

=item F<TQA_Update_Schedule.xls>

master schedule checklist Excel spreadsheet
this is used for either initializing the TQASched database
or for adding new scheduling content
parsing requires that the syntax of this document is strict so leave no trace 
unless you know what you're doing - adding content row(s)
removing content rows is not implemented yet and will have no effect on the db

=item F<//E<lt>network.pathE<gt>/DailyChecklist_E<lt>daterangeE<gt>.xls>

the operator checklist Excel spreadsheet for legacy content
new sheets automatically generated in the network path by the daemon on weekly basis
network path is generally set in configs
date range in the filename is calculated
strict formatting must be maintained in this file so that it may be parsed properly by the daemon

=back

=head1 AUTHOR

Matt Shockley

=head1 COPYRIGHT AND LICENSE
Copyright 2012 Matt Shockley

This program is free software; you can redistribute it and/or modify 
it under the same terms as Perl itself.

=cut
