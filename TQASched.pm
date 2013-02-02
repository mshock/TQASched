#! perl

package TQASched;

use strict;
no warnings "all";
use feature qw(say switch);
use Spreadsheet::ParseExcel;
use Spreadsheet::ParseExcel::Utility qw(ExcelFmt);
use DBI;
use Date::Manip qw(ParseDate DateCalc Delta_Format UnixDate Date_SetTime);
use Pod::Usage qw(pod2usage);
use AppConfig qw(:argcount);
use Exporter 'import';

# -i -c to initialize

# stuff to export to portal and daemon
our @EXPORT =
  qw(load_conf refresh_handles kill_handles write_log usage redirect_stderr exec_time find_sched @db_hrefs @CLI);

our @EXPORT_OK = qw(refresh_legacy refresh_dis);

# add the :all tag to Exporter
our %EXPORT_TAGS = ( all => [ ( @EXPORT, @EXPORT_OK ) ] );

# for saving @ARGV values for later consumption
our @CLI = @ARGV;

our @db_hrefs = my (
	$sched_db, $auh_db,  $prod1_db, $dis1_db,
	$dis2_db,  $dis3_db, $dis4_db,  $dis5_db
);

# require/use bounce
# return if being imported as module rather than run directly - also snarky import messages are fun
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

say 'TQASched module running in direct control mode, can you feel the POWER?!';

say 'parsing CLI args and config file (om nom nom)...';

# run all the configuration routines
# returns a reference to a ref to an AppConfig
my $cfg = ${ init() };

# get how many CLI args there were/are
my $num_args = scalar @CLI;

say 'initializing and nurturing a fresh crop of database handles...';

say '	*dial-up modem screech* (apologies, running old tech)';

# refresh those global handles for the first time
my (
	$dbh_sched, $dbh_auh,  $dbh_prod1, $dbh_dis1,
	$dbh_dis2,  $dbh_dis3, $dbh_dis4,  $dbh_dis5
) = refresh_handles();

say 'finished. TQASched all warmed up and revving to go go go ^_^';

# end of the line if a basic module load/connection test - dryrun
exit( dryrun($num_args) ) if $cfg->dryrun;

# warn that the module was run with no args, which is prettymuch a dryrun
# unless the config file tells it to do otherwise (nothing by default)
if ( $num_args <= 1 ) {
	say
"no explicit arguments? sure hope ${\$cfg->conf_file} tells me what to do, oh silent one";
}

# let them know we're watching (if only barely)
say sprintf "knocking out user request%s%s...",
  ( $num_args > 1 ? 's' : '' ), ( $num_args ? '' : 'if any' );

# initialize scheduling database from master schedule Excel file
init_sched() if $cfg->init_sched;

# start web server and begin hosting web application
my $server_pid = server() || 0 if $cfg->start_server;

# start daemon keeping track of scheduling
my $daemon_pid = daemon() || 0 if $cfg->start_daemon;

# if no children were forked, we're done - say goodbye!
unless ( $server_pid || $daemon_pid ) {
	say 'finished with all requests - prepare to be returned THE TRUTH';
}
else {

	# TODO: set the USR1 signal handler - for cleanly exiting
	# print out some nice info
	if ($server_pid) {
		write_log(
			{
				logfile => $cfg->log,
				msg =>
"the server was started with PID: $server_pid on port ${\$cfg->port}",
				type => 'INFO'
			}
		);
	}
	if ($daemon_pid) {
		write_log(
			{
				logfile => $cfg->log,
				msg =>
"the daemon was started with PID: $daemon_pid with freq. ${\$cfg->freq}",
				type => 'INFO'
			}
		);
	}

 # wait for the children to mess up or get killed (they should run indefinitely)
	my $dead_pid = wait();

	# determine at least one of the culprits and complain
	my $dead_child = ( $dead_pid == $server_pid ? 'server' : 'daemon' );

	write_log(
		{
			logfile => $cfg->log,
			msg =>
			  "well, it looks like $dead_child died on us (or all children)\n",
			type => 'ERROR'
		}
	);

	exit(1);
}

# THE TRUTH (oughta be 42, but that's 41 too many for perlwarn's liking)
1;

####################################################################################
#	subs - currently in no particular order
#		with only mild attempts at grouping similar code
####################################################################################

# glob all the direct-execution initialization and config routines
# returns ref to the global AppConfig
sub init {

	# the ever-powerful and needlessly vigilant config variable - seriously
	my $cfg = load_conf();

# no verbosity check! too bad i can't unsay what's been say'd, without more effort than it's worth
# send all these annoying remarks to dev/null, or close as we can get in M$
# TODO: neither of these methods actually do anything, despite some trying
	disable_say() unless $cfg->verbose;

	# run in really quiet, super-stealth mode (disable any warnings at all)
	disable_warn() if !$cfg->enable_warn || !$cfg->verbose;

	# user has requested some help. or wants to read the manpage. fine.
	usage() if $cfg->help;

	return \$cfg;
}

# dryrun exit routine
# takes optional exit value
sub dryrun {
	my ( $num_args, $exit_val ) = @_;

	# assume all is well
	$exit_val = 0 unless defined $exit_val;

	# insert various tests that all is well here
	if ( $num_args > 1 ) {

		# if it looks like the user is trying to do anything else
		# warn and exit(1)
		warn
"detected possible unconsumed commandline arguments and nolonger hungry\n";
		$exit_val++;
	}
	say sprintf
	  'dryrun completed in %u seconds. run along now little technomancer',
	  exec_time();

	# I prefer to return the exit value to the exit routine at toplevel
	# it enforces that the script will exit no matter what if it is a dryrun
	return $exit_val;
}

# somehow redefine the say feature to shut up
sub disable_say { }

# somehow turn off warnings... $SIG{WARN} redefine maybe?
sub disable_warn { }

# quick sub for getting current execution time
sub exec_time {
	return time - $^T;
}

# fill database with initial scheduling data
sub init_sched {

	my $sched_xls = $cfg->sched;

	# create parser and parse xls
	my $xlsparser = Spreadsheet::ParseExcel->new();
	my $workbook  = $xlsparser->parse($sched_xls)
	  or die "unable to parse spreadsheet: $sched_xls\n",
	  $xlsparser->error();
	say 'done loading master spreadsheet Excel file';

	# optionally create database and tables
	( create_db() or die "failed to create database\n" ) if $cfg->create_db;

	# populate database from excel file

	# iterate over each weekday (worksheets)
	for my $worksheet ( $workbook->worksheets() ) {
		my $sheet_name = $worksheet->get_name();

		# guess we're only doing daily updates for now
		next unless $sheet_name =~ m/daily/i;
		say "parsing $sheet_name...";

		#my $weekday_code = code_weekday($weekday);

		# skip if this is an unrecognized worksheet
		#say "\tunable to parse weekday, skipping" and next
		#		if $weekday_code eq 'U';

		# find the row and column bounds for iteration
		my ( $col_min, $col_max ) = $worksheet->col_range();
		my ( $row_min, $row_max ) = $worksheet->row_range();

		#my $sched_block = '';

		# iterate over each row and store scheduling data
		for ( my $row = $row_min ; $row <= $row_max ; $row++ ) {

			# skip header rows
			next if $row <= 1;

			# per-update hash of column values
			my $row_data = {};
			for ( my $col = $col_min ; $col <= $col_max ; $col++ ) {
				my $cell = $worksheet->get_cell( $row, $col );
				extract_row_init( $col, $cell, $row_data );
			}

			# skip rows that have no values, degenerates (ha)
			# also skip rows that have 'x' priority, not scheduled for that day
			#next if !$row_data->{update} || $row_data->{priority} eq 'x';

			# attempt to store rows that had values
			store_row($row_data)
			  or warn "\tfailed to store row $row for $sheet_name\n";
		}
	}

	# import the DIS mapping
	# DISABLED - new master sheet provides a mapping column
	#import_dis() if $cfg->import_dis;
}

# intialize new database handles
# should be called often enough to keep them from going stale
# especially for long-running scripts (daemon)
sub refresh_handles {
	return (
		$dbh_sched, $dbh_auh,  $dbh_prod1, $dbh_dis1,
		$dbh_dis2,  $dbh_dis3, $dbh_dis4,  $dbh_dis5
	  )
	  = map { init_handle($_) } (
		$sched_db, $auh_db,  $prod1_db, $dis1_db,
		$dis2_db,  $dis3_db, $dis4_db,  $dis5_db
	  );
}

# close database handles
sub kill_handles {
	my @handles = @_;
	map { $_->disconnect } @handles;
}

# kills any child processes
sub slay_children {
	kill( 9, $_ ) for @_;
}

# daemon to be run in another process
# polls the AUH and DIS metadata SQL and updates TQASched db
# see server() for detailed daemonization comments
sub daemon {
	my $daemon_pid;
	unless ( $daemon_pid = fork ) {
		exec( 'Daemon/daemon.pl', @CLI );
	}
	return $daemon_pid;
}

# server to be run in another process
# hosts the report webmon
sub server {

	# fork and return process id for shutdown
	my $server_pid;
	unless ( $server_pid = fork ) {

		# let's allow the modules CLI args to transfer down
		exec( 'Server/server.pl', @CLI );
	}
	return $server_pid;
}

# extract row into hash based on column number
# for master spreadsheet ingestion
sub extract_row_init {
	my ( $col, $cell, $row_href ) = @_;

	# get formatted excel value for most columns
	my $value = $cell ? $cell->value() : undef;

	given ($col) {

		# CT scheduled time
		when (/^0$/) {
			if ($value) {
				$row_href->{cst_clock} = $value;
			}
			else {

				#warn "no value found in column 0 (CST)\n";
				return;
			}
		}

		# GMT scheduled time
		when (/^1$/) {
			if ($value) {
				$row_href->{sched_epoch} = time2offset($value);
			}
			else {

				#warn "no value found in column 1 (GMT)\n";
				return;
			}
		}

		# update name
		when (/^2$/) {
			if ($value) {
				$row_href->{update} = $value;
			}
			else {

				#warn "no value found for column 2 (update)\n";
				return;
			}
		}

		# feed id
		when (/^3$/) {
			if ($value) {
				$row_href->{feed_id} = $value;
			}
			else {

				#warn "no value found for column 3 (feed id)\n";
				return;
			}
		}

		# DIS/Legacy flag
		when (/^4$/) {
			if ($value) {
				if ( $value =~ m/legacy/i ) {
					$row_href->{is_legacy} = 1;
				}
				elsif ( $value =~ m/dis/i ) {
					$row_href->{is_legacy} = 0;
				}
				else {

					#warn "unrecognized value in column 4 (legacy flag)\n";
					return;
				}
			}
			else {

				#warn "no value found for column 4 (legacy flag)\n";
				return;
			}
		}

		# priority
		when (/^5$/) {
			if ( defined $value ) {
				$row_href->{priority} = $value;
			}
			else {

				#warn "no value found for column 5 (priority)\n";
				return;
			}

		}

		# day of week range scheduled
		when (/^6$/) {
			if ($value) {
				$row_href->{days} = $value;
			}
			else {

				#warn "no value found for column 6 (day/s of week)\n";
				return;
			}
		}

		# outside of parsing scope
		# return and go to next row
		default { return };
	}
	return 1;
}

# extract row into hash based on column number
# for loading checklist entries
sub extract_row_daemon {
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
		default { return };
	}
	return 1;
}

# analyze and store a row from scheduling spreadsheet in database
sub store_row {
	my $row_href = shift;
	my ( $cst_clock, $sched_epoch, $update, $feed_id, $is_legacy, $priority,
		$days )
	  = map { $row_href->{$_} }
	  qw(cst_clock sched_epoch update feed_id is_legacy priority days);

	# cut any whitespace from feed_id
	$feed_id =~ s/\s//g;

	# trim whitespace from update full name
	$update =~ s/^\s*//;
	$update =~ s/\s*$//;

	# don't store row if not scheduled for today
	# or row is blank
	# but not an error so return true
	return 1
	  unless $update =~ m/\w+/
		  && defined $priority;

	# check if this update name has been seen before
	my $update_id;
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

	# okay, now there should be an entry in Updates, and the update_id is stored

	# let's link this to DIS feed id, also taken from the sheet (thank God)
	$dbh_sched->do(
		"insert into [TQASched].dbo.[Update_DIS] values
		('$feed_id', '$update_id')"
	) or warn "\tfailed to insert $update : $update_id into DIS linking\n";

	# insert scheduling info for each weekday
	my @time_offsets = offset_weekdays( $sched_epoch, $days );
	for my $pair_aref (@time_offsets) {
		my ( $this_offset, $weekday_code ) = @$pair_aref;
		$dbh_sched->do( "
			insert into [TQASched].dbo.[Update_Schedule] values 
				('$update_id','$weekday_code','$this_offset')
		" )
		  or warn
"\tfailed to insert update schedule info for update: $update id = $update_id & offset = $this_offset\n",
		  $dbh_sched->errstr;
	}

	# I guess double check that the Update made it in the table?
	unless ( $update_id = get_update_id($update) ) {
		warn "\tcould not find update: $update\n";
	}
	my $scheds_aref;

	# verify that all days made it into the scheduling table
	unless ( $scheds_aref = get_sched_id($update_id) ) {
		warn "\tcould not find sched history ID: $update ID: $update_id\n";
	}
	return $scheds_aref;
}

# generate time offset for the current time GMT
sub now_offset {

	# calculate GM Time
	my ( $sec, $min, $hour, $mday, $mon, $year, $wday, $yday, $isdst ) =
	  gmtime(time);
	my $offset = time2offset("$hour:$min");
	return 86400 * $wday + $offset + $sec;
}

# retrieve schedule IDs (aref of aref) from database for an update ID
sub get_sched_id {
	my ($update_id) = @_;

	my $scheds_aref = $dbh_sched->selectall_arrayref( "
		select sched_id, sched_epoch from [TQASched].dbo.[Update_Schedule]
		where update_id = '$update_id'" );

	my @scheds = ();
	for my $sched_aref (@$scheds_aref) {
		my ( $sched_id, $sched_epoch ) = @$sched_aref;
		push @scheds, [ $sched_id, $sched_epoch ];
	}

	#warn "\tno. of schedule records does not match no. of days parsed\n"
	#unless scalar @scheds == scalar ;

	return \@scheds;
}

# returns code for current weekday
sub now_wd {
	my ( $sec, $min, $hour, $mday, $mon, $year, $wday, $yday, $isdst ) =
	  gmtime(time);
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
		update_history(
			{
				update_id    => $update_id,
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
		default    { return };
	}
}

# store/modify update history entry
sub update_history {
	my $hashref = shift;
	my ( $update_id, $sched_id, $trans_offset, $late_q, $fd_q, $fn_q ) = (
		$hashref->{update_id},    $hashref->{sched_id},
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

# convert 24hr time to seconds offset from beginning of the day
# next it will have a day offset in seconds added to it where Sunday = 0
sub time2offset {
	my $time_string = shift;
	my ( $hours, $minutes ) = ( $time_string =~ m/(\d+):(\d+)/ );
	unless ( defined $hours && defined $minutes ) {
		warn "\tparsing error converting time to offset: $time_string\n";
		return;
	}
	return $hours * 3600 + $minutes * 60;
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
	my @now =
	  $cfg->tz() =~ m/(?:GM[T]?|UT[C]?)/i
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

	$cfg = AppConfig->new(
		{
			CREATE => 1,
			ERROR  => \&appconfig_error,
			GLOBAL => {
				ARGCOUNT => ARGCOUNT_ONE,
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
	(
		$sched_db, $auh_db,  $prod1_db, $dis1_db,
		$dis2_db,  $dis3_db, $dis4_db,  $dis5_db
	) = map { get_handle_hash($_) } qw(sched_db auh_db prod1_db 1 2 3 4 5);

	return $cfg;
}

# handle any errors in AppConfig parsing - namely log them
sub appconfig_error {

	# hacky way to force always writing this log to top-level dir
	# despite the calling script's location
	my $top_log =
	  ( __PACKAGE__ ne 'TQASched' ? $INC{'TQASched.pm'} =~ s!\w+\.pm!!gr : '' )
	  . $cfg->log();

	write_log(
		{
			logfile => $top_log,
			type    => 'WARN',
			msg     => join( "\t", @_ ),
		}
	);
}

sub define_defaults {
	my %config_vars = (

		# server configs
		# server host port ex: localhost:9191
		server_port => {
			DEFAULT => 9191,
			ARGS    => '=i',
			ALIAS   => 'host_port|port|p',
		},

	 # server auto-start, good to set in conf file once everything is running OK
		server_start => {
			DEFAULT => 0,
			ARGS    => '!',
			ALIAS   => 'start_server|s',
		},

		# server logfile path
		server_logfile => {
			DEFAULT => 'server.log',
			ALIAS   => 'server_log',
		},

		# path to script which prints content
		# this content is hosted through TCP/IP under HTTP
		server_hosted_script => {
			DEFAULT => 'test.pl',
			ALIAS   => 'hosted_script|target_script|content_script',
		},

	 # daemon configs
	 # daemon auto-start, good to set in conf file once everythign is running OK
		daemon_start => {
			DEFAULT => 0,
			ARGS    => '!',
			ALIAS   => 'start_daemon|d'
		},

		# periodicity of the daemon loop (seconds to sleep)
		daemon_update_frequency => {
			DEFAULT => 60,
			ALIAS   => 'update_freq',
		},

		# daemon logfile path
		daemon_logfile => {
			DEFAULT => 'daemon.log',
			ALIAS   => 'daemon_log',
		},

		# scheduling configs
		#
		# path to master schedule spreadsheet
		sched_file => {
			DEFAULT => 'TQA_Update_Schedule.xls',
			ALIAS   => 'sched',
		},

		# path to the operator legacy update checklist
		sched_checklist_path => {
			DEFAULT => '.',
			ALIAS   => 'checklist',
		},

		# initialize scheduling data
		# parse master schedule
		# insert scheduling records and metadata into db
		sched_init => {
			DEFAULT => 0,
			ARGS    => '!',
			ALIAS   => 'init_sched|i',
		},

		# create scheduling the scheduling database framework from scratch, yum
		sched_create_db => {
			DEFAULT => 0,
			ARGS    => '!',
			ALIAS   => 'create_db|c',
		},

		# link update ids to feed ids in DIS
		sched_import_dis => {
			DEFAULT => 0,
			ARGS    => '!',
			ALIAS   => 'import_dis|m'
		},

		# report (content gen script) configs
		# report script's logfile
		report_logfile => {
			DEFAULT => 'report.log',
			ALIAS   => 'report_log',

		},

# path to css stylesheet file for report gen, hosted statically and only by request!
# all statically hosted files are defined relative to the TQASched/Resources/ directory, where they enjoy living (for now, bwahahaha)
		report_stylesheet => {
			DEFAULT => 'styles.css',
			ALIAS   => 'styles|stylesheet',
		},

# path to jquery codebase (an image of it taken sometime in... Jan 2013) - not in use yet
		report_jquery => {
			DEFAULT => 'jquery.js',
			ALIAS   => 'jquery',
		},

	  # path to user created javascript libraries and functions - not in use yet
		report_user_js => {
			DEFAULT => 'js.js',
			ALIAS   => 'user_js',
		},

		# refresh rate for report page
		report_refresh => {
			DEFAULT => '300',
			ALIAS   => 'refresh',
		},

		# report date CGI variable
		report_date => {
			DEFAULT => '',
			ARGS    => '=i',
			ALIAS   => 'date',
		},

  # refresh rate for the report page - can't be less than 10, and 0 means never.
  # (in seconds)

		# default (misc) configs
		#
		# toggle or set verbosity level to turn off annoying, snarky messages
		default_verbosity => {
			DEFAULT => 1,
			ARGS    => ':i',
			ALIAS   => 'verbosity|verbose|v',
		},

		# toggle logging
		default_enable_logging => {
			DEFAULT => 1,
			ARGS    => '!',
			ALIAS   => 'logging|logging_enabled|l',
		},

		# timezone to write log timestamps in
		default_log_tz => {
			DEFAULT => 'local',
			ALIAS   => 'tz|timezone',
		},

		# helpme / manpage from pod
		default_help => {
			DEFAULT => 0,
			ARGS    => '!',
			ALIAS   => 'help|version|usage|h'
		},

# path to config file
# (optional, I suppose if you wanted to list all database connection info in CLI args)
		default_config_file => {
			DEFAULT => "TQASched.conf",
			ARGS    => '=s',
			ALIAS   => "cfg_file|conf_file|config_file|f",
		},

# toggle dryrun mode = non-destructive test of module load and all db connections
		default_dryrun => {
			DEFAULT => 0,
			ARGS    => '!',
			ALIAS   => 'dryrun|y',
		},
		default_logfile => {
			DEFAULT => 'TQASched.log',
			ALIAS   => 'log',
		},
		default_enable_warn => {
			DEFAULT => 1,
			ALIAS   => 'enable_warn',
		}
	);

	$cfg->define( $_ => \%{ $config_vars{$_} } ) for keys %config_vars;
}

# build and return hashref of db connection info from configs
sub get_handle_hash {
	my ($db_name) = (@_);
	return {
		name   => $cfg->get("${db_name}_name"),
		user   => $cfg->get("${db_name}_user"),
		server => $cfg->get("${db_name}_server"),
		pwd    => $cfg->get("${db_name}_pwd"),
	};
}

# translate weekday string to array of corresponding offsets
sub offset_weekdays {
	my ( $sched_offset, $days ) = @_;

	# hardcoded hash of day to weekday integer (as from localtime - sunday is 0)
	my %wd_lookup = (
		Su => 0,
		M  => 1,
		T  => 2,
		W  => 3,
		Th => 4,
		F  => 5,
		Sa => 6,
	);

	my $day_increment = 86400;

	# capture date range case
	my @offsets = ();
	if ( $days =~ m/(\w+)-(\w+)/ ) {
		my ( $first_date, $second_date ) = ( $1, $2 );
		my ( $first_int, $second_int ) =
		  ( $wd_lookup{$first_date}, $wd_lookup{$second_date} );

		# this should be the case most of the time

		if ( $first_int < $second_int ) {

		   # iterate over each day and push to return array - easy case, no wrap
			while ( $first_int <= $second_int ) {
				push @offsets,
				  [ $first_int * $day_increment + $sched_offset, $first_int ];
				$first_int++;
			}

		}
		elsif ( $first_int > $second_int ) {
			while (1) {
				push @offsets,
				  [ $first_int * $day_increment + $sched_offset, $first_int ];
				last if $first_int == $second_int;

				# wrap back around to sunday after friday
				if ( ++$first_int > 6 ) {
					$first_int = 0;
				}
			}

		}
		else {
			warn
"failed sanity check: $first_date:$first_int $second_date:$second_int\n";
			return;
		}

	}

	# capture single day case
	elsif ( $days =~ m/^\s*(\w+)\s*$/ ) {
		my $day     = $1;
		my $day_int = $wd_lookup{$day};
		$sched_offset += $day_int * $day_increment;
		push @offsets, [ $sched_offset, $day_int ];
	}

	# match range plus a day case
	elsif ( $days =~ m/(\w+)-(\w+), (\w+)/ ) {
		my ( $first_date, $second_date, $last_date ) = ( $1, $2, $3 );

		# lookup the corresponding ints for these days
		my ( $first_int, $second_int, $last_int ) = (
			$wd_lookup{$first_date}, $wd_lookup{$second_date},
			$wd_lookup{$last_date}
		);

		# push last day as a single, it's easy
		push @offsets,
		  [ $last_int * $day_increment + $sched_offset, $last_int ];

		# handle other date range the same as the last one
		if ( $first_int < $second_int ) {

		   # iterate over each day and push to return array - easy case, no wrap
			while ( $first_int <= $second_int ) {
				push @offsets,
				  [ $first_int * $day_increment + $sched_offset, $first_int ];
				$first_int++;
			}
		}
		elsif ( $first_int > $second_int ) {
			while (1) {
				push @offsets,
				  [ $first_int * $day_increment + $sched_offset, $first_int ];
				last if $first_int == $second_int;

				# wrap back around to sunday after friday
				if ( ++$first_int > 6 ) {
					$first_int = 0;
				}
			}
		}
	}
	else {
		warn "unable to parse $days date range\n";
		return;
	}
	return @offsets;
}

# translate weekday string to code
sub code_weekday {
	my $weekday = shift;
	my $rv;
	given ($weekday) {
		when (/monday/i)    { $rv = 1 }
		when (/tuesday/i)   { $rv = 2 }
		when (/wednesday/i) { $rv = 3 }
		when (/thursday/i)  { $rv = 4 }
		when (/friday/i)    { $rv = 5 }
		when (/saturday/i)  { $rv = 6 }
		when (/sunday/i)    { $rv = 0 }
		default             { $rv = -1 };
	}
	return $rv;
}

# get handle for master on sql server
sub init_handle {
	my $db = shift;

	# connecting to master since database may need to be created
	return DBI->connect(
		sprintf(
			"dbi:ODBC:Database=%s;Driver={SQL Server};Server=%s;UID=%s;PWD=%s",
			$db->{name} || 'master',
			$db->{server}, $db->{user}, $db->{pwd}
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

	# create update table
	$dbh_sched->do(
		"create table [TQASched].dbo.[Updates] (
		update_id int not null identity(1,1),
		name varchar(255) not null unique,
		priority tinyint,
		is_legacy bit
	)"
	) or die "could not create Updates table\n", $dbh_sched->errstr;

	# create update/schedule linking table
	$dbh_sched->do(
		"create table [TQASched].dbo.[Update_Schedule] (
		sched_id int not null identity(1,1),
		update_id int not null,
		weekday tinyint not null,
		sched_epoch int not null
		
	)"
	) or die "could not create Update_Schedule table\n", $dbh_sched->errstr;

	# create history tracking table
	$dbh_sched->do(
		"create table [TQASched].dbo.[Update_History] (
		hist_id int not null identity(1,1),
		update_id int not null,
		sched_id int not null,
		hist_epoch int,
		filedate int,
		filenum tinyint,
		timestamp DateTime,
		late char(1)
	)"
	) or die "could not create Update_History table\n", $dbh_sched->errstr;

	# create linking table from DIS feed_ids to update_ids
	$dbh_sched->do( "
		create table [TQASched].dbo.[Update_DIS] (
		update_dis_id int not null identity(1,1),
		feed_id varchar(20) not null,
		update_id int not null
		)
	" )
	  or warn
	  "\tcould not create DIS linking table - Update_DIS, may already exist\n";

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
	say 'accessing checklist directory: ' . $cfg->checklist;
	opendir( my $dir_fh, $cfg->checklist )
	  or warn "could open/find checklist dir" . $cfg->checklist . "$!\n";
	my @files = readdir($dir_fh);
	closedir $dir_fh;
	say 'success. searching for latest checklist';

	# TODO: find latest, create new (copy & rename blank checklist)
	my $low           = 9**9**9;
	my $new_list_path = '';
	say scalar @files . ' files to look at';
	my $checklist_path = $cfg->checklist . '/';
	for my $file (@files) {
		next unless -f "$checklist_path$file";
		my $score = -M "$checklist_path$file";
		if (   $score < $low
			&& $file =~ m/DailyChecklist/i
			&& $file !~ m/Shortcut/i )
		{
			$low           = $score;
			$new_list_path = "$checklist_path$file";
		}
	}
	say "best candidate: $new_list_path w/ $low";
	my ( $startdate, $enddate );
	if ($new_list_path) {

		# capture dates and months from name
		if ( $new_list_path =~ m/(\d+)(\D*)-(\d+)(\D*)\./ ) {
			( $startdate, $enddate ) = parse_months( $1, $2, $3, $4 );
		}
		else {
			warn
"could not find a valid checklist file (almost, though) - $new_list_path\n";
		}

	}
	else {
		write_log(
			{
				logfile => $cfg->log,
				type    => 'ERROR',
				msg     => "could not find the ops checklist!"
			}
		);
	}
	return ( $new_list_path, $startdate, $enddate );
}

# convert text month to number indexed starting at 0 in Jan
sub parse_months {
	my ( $startdate, $startmonth, $enddate, $endmonth ) = @_;

	# some filtering for crap
	$startmonth =~ s/(th|st|nd|rd)//;
	$endmonth   =~ s/(th|st|nd|rd)//;

	# let's try to divine what the date range is from regex extractions
	my ( $firstdate, $seconddate );

	if ( $firstdate = ParseDate("$startdate $startmonth") ) {

		# yay it recognizes it as a date
		say "first pass $firstdate";
	}
	else {

		# crap it is probably just a preceding date in the same month
		if ( $firstdate = ParseDate("$endmonth $startdate") ) {

			# yay, it was
			say "second pass $firstdate";
		}
		else {
			warn
"could not parse a date out of $startdate / $startmonth / $endmonth from checklist filename\n";
			return;
		}
	}

	# this one should work every time
	if ( $seconddate = ParseDate("$endmonth $enddate") ) {
		say "seconddate $seconddate";
	}
	else {
		warn
		  "unable to parse an enddate $enddate $endmonth for checklist file\n";
	}
	return ( $firstdate, $seconddate );
}

# poll auh metadata for DIS feed statuses
sub refresh_dis {
	my $current_wd     = now_wd();
	my $current_offset = now_offset();

	# get all updates expected for the current day
	my $expected = "
		select ud.feed_id, u.name, us.time, us.sched_id, us.update_id
		from 
			TQASched.dbo.Update_Schedule us,
			TQASched.dbo.Update_DIS ud,
			TQASched.dbo.Updates u
		where ud.update_id = us.update_id
		and us.weekday = '$current_wd'
		and u.update_id = ud.update_id
		";
	my $sth_expected = $dbh_sched->prepare($expected);
	$sth_expected->execute();
	my $updates_aref = $sth_expected->fetchall_arrayref();

	# iterate over each of them and determine if they are completed
	for my $update_aref ( @{$updates_aref} ) {

		# extract update info
		my ( $feed_id, $name, $offset, $sched_id, $update_id ) =
		  @{$update_aref};

		# get build number (optional) from feed name
		my ( $stripped_name, $build_num ) = $name =~ m/(.*)#(\d+)/;

# double duty query
# gets all needed info for non-enumerated feeds
# gets DIS server (sender) for enumerated feeds to hit for build-specific details
		my $transactions = "
			select top 1 Status, ProcessTime, FileDate, FileNum, Sender, TransactionNumber, BuildTime 
			from [TQALic].dbo.[PackageQueue] 
			with (NOLOCK)
			where TaskReference LIKE '%$feed_id%'
			order by ProcessTime desc
		";

		my ( $status, $exec_end, $fd, $fn, $sender, $trans_num, $build_time ) =
		  $dbh_prod1->selectrow_array($transactions);

		# if this is an enumerated feed
		# check the last execution time of that build
		# in the correct DIS server
		my $backdate_updates;
		if ($build_num) {

			# backdate builds packaged in the same UPD
			my $backdate_query = "
				select us.sched_id, u.name, u.update_id, uh.filedate
				from tqasched.dbo.update_schedule us
				join tqasched.dbo.updates u
					on u.update_id = us.update_id
				left join tqasched.dbo.update_history uh
					on uh.sched_id = us.sched_id
					and DateDiff(dd, [timestamp], GETUTCDATE()) < 1
				where us.weekday = '$current_wd'
				and u.name LIKE '$stripped_name%'
			";

			#say $backdate_query;

			$backdate_updates = $dbh_sched->selectall_arrayref($backdate_query);

			my $dbh_dis = sender2dbh($sender);

			# retrieve last transaction number for this build number
			my $dis_trans = "
				select top 1 DISTransactionNumber
				from DataIngestionInfrastructure.dbo.MakeUpdateInfo
				with (NOLOCK)
				where BuildNumber = $build_num
				and DataFeedId = '$feed_id'
				
				order by ExecutionDateTime desc
			";

			my ($trans_num) = $dbh_dis->selectrow_array($dis_trans)
			  or warn "\tno transaction # found for enum feed $name, skipping\n"
			  and next;

			# select this transaction from TQALic
			# to get AUH process time, along with filenum and filedate
			my $transactions = "
				select top 1 Status, BuildTime, FileDate, FileNum, Sender, TransactionNumber, BuildTime 
				from [TQALic].dbo.[PackageQueue] 
				with (NOLOCK)
				where TaskReference LIKE '%$feed_id%'
				and TransactionNumber = $trans_num
				and DateDiff(dd, [BuildTime], GETUTCDATE()) < 1
				order by ProcessTime desc
			";
			( $status, $exec_end, $fd, $fn, $sender, $trans_num, $build_time ) =
			  $dbh_prod1->selectrow_array($transactions)
			  or warn
			  "\tcould not find metadata for $name from trans #: $trans_num\n"
			  and next;

		}

		if ( defined $fd ) {
			$fd =~ s/(\d+)-(\d+)-(\d+).*/$1$2$3/;
		}

		# check last feed execution endtime value to verify schedule data
		# convert DateTime to offset and compare against current time
		if ($exec_end) {

			##say "found transaction for $name";
			#my $trans_offset = datetime2offset($exec_end);

			# no transaction offset means that the last one was a previous day
			#if ( !$trans_offset ) {
			#	say "\tmust be previous day $name";
			#	next;
			#}

			# compare transaction execution time to schedule offset
			my $cmp_result = comp_offsets( $exec_end, $offset );
			my $trans_offset = datetime2offset($exec_end);

			# if it's within an hour of the scheduled time, mark as on time
			# could also be early
			if ( $cmp_result == 0 ) {
				say "ontime $name $exec_end offset: $offset";
				update_history(
					{
						update_id    => $update_id,
						sched_id     => $sched_id,
						trans_offset => $trans_offset,
						late         => 'N',
						filedate     => $fd,
						filenum      => $fn
					}
				);
				backdate( $backdate_updates, $trans_offset, 'N', $fd, $fn,
					$build_num, $sched_id );
			}

			# otherwise it either has not come in or it is late
			# late
			elsif ( $cmp_result == 1 ) {
				say "late $name $exec_end to offset: $offset";
				update_history(
					{
						update_id    => $update_id,
						sched_id     => $sched_id,
						trans_offset => $trans_offset,
						late         => 'Y',
						filedate     => $fd,
						filenum      => $fn
					}
				);
				backdate( $backdate_updates, $trans_offset, 'Y', $fd, $fn,
					$build_num, $sched_id );
			}

			# possibly just not recvd yet
			elsif ( $cmp_result == -1 ) {
				say "waiting on $name, last trans: $exec_end";
			}
			else {
				warn
				  "\tFAILED transaction offset sanity check: $name $$offset\n";
				next;
			}
		}
		else {
			warn "\tno transactions found for $name : feed_id = $feed_id\n";
			next;
		}

	}
}

# poll ops schedule Excel spreadsheet for legacy feed statuses
sub refresh_legacy {

	# attempt to find & download the latest spreadsheet from OpsDocs server
	my ( $sched_xls, $startdate, $enddate ) = find_sched();

	# create parser and parse xls
	my $xlsparser = Spreadsheet::ParseExcel->new();
	my $workbook  = $xlsparser->parse($sched_xls)
	  or die "unable to parse spreadsheet: $sched_xls\n",
	  $xlsparser->error();
	say 'done';

	# iterate over each weekday (worksheets)
	for my $worksheet ( $workbook->worksheets() ) {
		my $weekday = $worksheet->get_name();
		say "parsing $weekday...";
		my $weekday_code = code_weekday($weekday);

		# skip if this is an unrecognized worksheet
		say "\tunable to parse weekday, skipping" and next
		  if $weekday_code == -1;

		# find the row and column bounds for iteration
		my ( $col_min, $col_max ) = $worksheet->col_range();
		my ( $row_min, $row_max ) = $worksheet->row_range();

		my $sched_block = '';

		# iterate over each row and store scheduling data
		for ( my $row = $row_min ; $row <= $row_max ; $row++ ) {
			next if $row <= 1;

			# per-update hash of column values
			my $row_data = {};
			for ( my $col = $col_min ; $col <= $col_max ; $col++ ) {
				my $cell = $worksheet->get_cell( $row, $col );
				unless ( extract_row_daemon( $col, $cell, $row_data ) ) {
				}
				else {
					if (   $row_data->{time_block}
						&& $sched_block ne $row_data->{time_block} )
					{
						$sched_block = $row_data->{time_block};
					}
					else {
						$row_data->{time_block} = $sched_block;
					}
				}
			}

			# skip unless filled in
			next
			  unless $row_data->{update}
				  && $row_data->{filedate}
				  && $row_data->{filenum};

			my $name        = $row_data->{update};
			my $update_id   = get_update_id($name);
			my $sched_query = "
				select sched_epoch, sched_id 
				from TQASched.dbo.Update_Schedule us
				where update_id = $update_id
				and weekday = $weekday_code
			";

			#say $sched_query and die;
			my ( $sched_offset, $sched_id ) =
			  $dbh_sched->selectrow_array($sched_query);

			unless ($sched_offset) {
				warn "no schedule entry for $name : $update_id : $sched_id\n";
				next;
			}

			#my $exec_end     = gmtime(time);
			my $trans_offset = now_offset();
			my $ontime;

			# compare transaction execution time to schedule offset
			my $cmp_result = comp_offsets( $trans_offset, $sched_offset );

			# if it's within an hour of the scheduled time, mark as on time
			# could also be early
			if ( $cmp_result == 0 ) {
				say "ontime $name $trans_offset offset: $sched_offset";
				update_history(
					{
						update_id    => $update_id,
						sched_id     => $sched_id,
						trans_offset => $trans_offset,
						late         => 'N',
						filedate     => $row_data->{filedate},
						filenum      => $row_data->{filenum}
					}
				);
			}

			# otherwise it either has not come in or it is late
			# late
			elsif ( $cmp_result == 1 ) {
				say "late $name $trans_offset to offset: $sched_offset";
				update_history(
					{
						update_id    => $update_id,
						sched_id     => $sched_id,
						trans_offset => $trans_offset,
						late         => 'Y',
						filedate     => $row_data->{filedate},
						filenum      => $row_data->{filenum}
					}
				);
			}

			# possibly just not recvd yet
			elsif ( $cmp_result == -1 ) {
				say "waiting on $name, last trans: $trans_offset";
			}
			else {
				warn
"\tFAILED transaction offset sanity check: $name $sched_offset\n";
				next;
			}
		}
	}
}

# write a severity/type tagged message to target logfile
sub write_log {
	my $entry_href = shift;

	# bounce for logging toggle
	return unless $cfg->logging;

	# bounce for badly formed argument, cmon give us a darn hash reference
	( warn "Passed non-href value to write_log\n" and return )
	  unless ( ref($entry_href) eq 'HASH' );

	# let's just make sure we're all lower case keys here and save a headache
	my %entry = map { ( lc $_ => ${$entry_href}{$_} ) } keys %{$entry_href};

	# log message individual type handling
	# hopefully reducing the number of warn and say calls needed
	given ( uc $entry{type} ) {
		when (m'INFO') {
			return unless $cfg->verbose;
			say $entry{msg};
		}
		when (m'WARN') {
			return unless $cfg->enable_warn;
			warn $entry{msg};
		}
		when (m'ERROR') {
			warn $entry{msg};
		}

		# warn about unusual entry types, but still log them
		default {
			warn "unrecognized log entry type: $entry{type}\n";
			$entry{msg} = 'UNKN';
		}
	}

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
	pod2usage(
		{
			-verbose => $cfg->verbosity,
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
