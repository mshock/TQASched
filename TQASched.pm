#! perl -w

package TQASched;

# TODO switch all debugging outputs from write_log to more helpful dsay
# TODO fix feed date handling in refresh and report

use strict;
use Net::SMTP;
use feature qw(say switch);
use Spreadsheet::ParseExcel;
use Spreadsheet::ParseExcel::Utility qw(ExcelFmt);
use DBI;
use Carp;
use Term::ReadKey;
use File::Copy;

#use Date::Manip
#	qw(ParseDate DateCalc Delta_Format UnixDate Date_DayOfWeek Date_GetPrev Date_ConvTZ Date_PrevWorkDay Date_NextWorkDay);
use Time::Local;
use Pod::Usage qw(pod2usage);
use AppConfig qw(:argcount);
use Exporter 'import';
use constant REGEX_TRUE => qr/^\s*(?:true|(?:t)|(?:y)|yes|(?:1))\s*$/i;

# options crash course:
# -i -c to initialize database and populate with master scheduling
# -s -d to start in server + daemon mode (normal execution)

# stuff to export to all subscripts
our @EXPORT = qw(
	dsay
	load_conf
	legacy_feed_date
	time2offset
	now_offset
	refresh_handles
	kill_handles
	write_log
	usage
	redirect_stderr
	exec_time
	find_sched
	check_handles
	date_math
	parse_filedate
	@db_hrefs
	@CLI
	REGEX_TRUE
	$cfg);

# anything used only in a single subscript goes here
our @EXPORT_OK = qw(refresh_legacy refresh_dis);

# add the :all tag to Exporter
our %EXPORT_TAGS = (
	all => [ ( @EXPORT, @EXPORT_OK ) ],
	min => [qw(write_log redirect_stderr load_conf $cfg)] );

# for saving @ARGV values for later consumption
our @CLI = @ARGV;

#our $debug_mode;

# shared database info loaded from configs
# so that importers can create their own handles
# INV: may be completely unecessary! kind of a scoping grey area - test when time
our @db_hrefs = my ( $sched_db, $auh_db,  $prod1_db, $dis1_db,
					 $dis2_db,  $dis3_db, $dis4_db,  $dis5_db );

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

say
	'TQASched module running in direct control mode, can you feel the POWER?!';

say 'parsing CLI args and config file (om nom nom)...';

# run all the configuration routines
# returns a reference to a ref to an AppConfig
our $cfg = ${ init() };

# get how many CLI args there were/are
my $num_args = scalar @CLI;

say 'initializing and nurturing a fresh crop of database handles...';

say '	*dial-up modem screech* (apologies, running old tech)';

# refresh those global handles for the first time
my ( $dbh_sched, $dbh_auh,  $dbh_prod1, $dbh_dis1,
	 $dbh_dis2,  $dbh_dis3, $dbh_dis4,  $dbh_dis5
) = refresh_handles();

say 'finished. TQASched all warmed up and revving to go go go ^_^';

# end of the line if a basic module load/connection test - dryrun
exit( dryrun($num_args) ) if $cfg->dryrun;

# warn that the module was run with no args, which is prettymuch a dryrun
# unless the config file tells it to do otherwise (nothing by default)
unless ($num_args) {
	say
		"no explicit arguments? sure hope ${\$cfg->conf_file} tells me what to do, oh silent one";
}

# let them know we're watching (if only barely)
say sprintf "knocking out user request%s%s...",
	( $num_args > 1 ? 's' : '' ), ( $num_args ? '' : 'if any' );

# do all the various tasks requested in the config file and CLI args, if any
execute_tasks();

# THE TRUTH (oughta be 42, but that's 41 too many for perlwarn's liking)
1;

####################################################################################
#	subs - currently in no particular order
#		with only mild attempts at grouping similar code
####################################################################################

# same as say but toggles output with debug mode config
# must be defined first for Perl one-pass syntax
# TODO pull into another library, load up top
sub dsay {
	return unless my $debug_mode = $cfg->debug;

	# avoid undef warnings and be more clear in output
	my @dumped_args;
	# TODO this is setting variables out of scope...
#	for my $var (@_) {
#		$var = '<undef>' unless defined $var;
#		push @dumped_args, $var;
#	}

	my $output
		= "\nDEBUG"
		. join( "\n\t", " caller:\t@{[caller]}", @_ )
		. "\n\\DEBUG\n";

	# values > 1 correspond to different debug modes, wtb switch for numerics
	if ( $debug_mode == 1 ) { say $output; }
	elsif ( $debug_mode == 2 ) {
		open my $db_fh, '>>', 'debug.txt';
		say $db_fh "[@{[timestamp()]}]\t", $output;
		close $db_fh;
	}
	elsif ( $debug_mode == 3 ) {
		open my $db_fh, '>>', 'debug.txt';
		say $db_fh "[@{[timestamp()]}]\t", $output;
		close $db_fh;
		say $output;
	}
	else {
		die "[DEBUG] debug mode not recognized, ending execution: $output";
	}
}

# glob all the direct-execution initialization and config routines
# returns ref to the global AppConfig
sub init {

	# the ever-powerful and needlessly vigilant config variable - seriously
	$cfg = load_conf();

# no verbosity check! too bad i can't unsay what's been say'd, without more effort than it's worth
# send all these annoying remarks to dev/null, or close as we can get in M$
# TODO neither of these methods actually do anything, despite some trying
#disable_say() unless $cfg->verbose;

	# run in really quiet, super-stealth mode (disable any warnings at all)
	#disable_warn() if !$cfg->enable_warn || !$cfg->verbose;

	# user has requested some help. or wants to read the manpage. fine.
	usage() if $cfg->help;

	return \$cfg;
}

# run all user requested tasks when module is executed directly (rather than imported)
sub execute_tasks {

	# initialize scheduling database from master schedule Excel file
	init_sched() if $cfg->init_sched;

	# start web server and begin hosting web application
	my $server_pid = server() || 0 if $cfg->start_server;

	# start daemon keeping track of scheduling
	my $daemon_pid = daemon() || 0 if $cfg->start_daemon;

	# if no children were forked, we're done - say goodbye!
	unless ( $server_pid || $daemon_pid ) {
		say 'finished with all requests - prepare to be returned THE TRUTH';
		write_log( { type => 'INFO',
					 msg  => sprintf( 'TQASched run completed in %u seconds',
									 exec_time() ),
					 logfile => $cfg->log
				   }
		);
	}
	else {

		# TODO set the USR1 signal handler - for cleanly exiting
		# print out some nice info
		if ($server_pid) {
			write_log(
				{  logfile => $cfg->log,
				   msg =>
					   "the server was started with PID: $server_pid on port ${\$cfg->port}",
				   type => 'INFO'
				}
			);
		}
		if ($daemon_pid) {
			write_log(
				{  logfile => $cfg->log,
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
			{  logfile => $cfg->log,
			   msg =>
				   "well, it looks like $dead_child died on us (or all children)\n",
			   type => 'ERROR'
			}
		);

		exit(1);
	}
}

# dryrun exit routine
# takes optional exit value
sub dryrun {
	my ( $num_args, $exit_val ) = @_;

	# assume all is well
	$exit_val = 0 unless defined $exit_val;
	my $msg  = '';
	my $type = 'INFO';

	# insert various tests that all is well here
	if ( $num_args > 1 ) {
		$msg
			= "detected possible unconsumed commandline arguments and no longer hungry\n";

		# if it looks like the user is trying to do anything else
		# warn and exit(1)
		$type = 'WARN';
		$exit_val++;
	}
	$msg .= sprintf
		'dryrun completed in %u seconds. run along now little technomancer',
		exec_time();

	write_log( { logfile => $cfg->log,
				 msg     => $msg,
				 type    => $type,
			   }
	);

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

	#say 'yes' and die if -f $sched_xls;

	# create parser and parse xls
	my $xlsparser = Spreadsheet::ParseExcel->new();
	my $workbook  = $xlsparser->parse($sched_xls)
		or say "unable to parse spreadsheet: $sched_xls\n",
		$xlsparser->error()
		and return;
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
		for ( my $row = $row_min; $row <= $row_max; $row++ ) {

			# skip header rows
			next if $row <= 1;

			# per-update hash of column values
			my $row_data = {};
			for ( my $col = $col_min; $col <= $col_max; $col++ ) {
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

	# allow caller to specify handles
	my @selected = @_;

	my @refresh_list = ();

	# if caller passed args, refresh only those handles
	if ( scalar @selected ) {
		for (@selected) {
			when (m/sched/i)    { push @refresh_list, $sched_db }
			when (m/auh/i)      { push @refresh_list, $auh_db }
			when (m/prod1/i)    { push @refresh_list, $prod1_db }
			when (m/dis1/i)     { push @refresh_list, $dis1_db }
			when (m/dis2/i)     { push @refresh_list, $dis2_db }
			when (m/dis3/i)     { push @refresh_list, $dis3_db }
			when (m/dis4|tt3/i) { push @refresh_list, $dis4_db }
			when (m/dis5|tt5/i) { push @refresh_list, $dis5_db }
			default {
				say "trying to refresh unrecognized handle: $_ from "
					. (caller)[1];

				#				write_log(
				#					{
				#						logfile  => $cfg->log,
				#							type => 'ERROR',
				#							msg =>
				#							"trying to refresh unrecognized handle: $_ from "
				#							. (caller)[1]
				#
				#					}
				#				);
			}
		}
	}

	# otherwise refresh all handles
	else {
		@refresh_list = ( $sched_db, $auh_db,  $prod1_db, $dis1_db,
						  $dis2_db,  $dis3_db, $dis4_db,  $dis5_db );
	}

	return ( $dbh_sched, $dbh_auh,  $dbh_prod1, $dbh_dis1,
			 $dbh_dis2,  $dbh_dis3, $dbh_dis4,  $dbh_dis5
	) = map { init_handle($_) } @refresh_list;

}

# check that all handles are defined
sub check_handles {
	my $self_check = shift;
	my $undefs     = 0;
	for ( $dbh_sched, $dbh_auh,  $dbh_prod1, $dbh_dis1,
		  $dbh_dis2,  $dbh_dis3, $dbh_dis4,  $dbh_dis5 )
	{
		$undefs++ unless defined;
	}

	# try to initialize them one more time for the hell of it
	if ( !$self_check && $undefs ) {
		refresh_handles();
		$undefs = check_handles(1);
	}
	return $undefs;
}

# close database handles
sub kill_handles {
	map { $_->disconnect if defined $_ } @_;
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
		default {return};
	}
	return 1;
}

# extract row into hash based on column number
# for loading checklist entries
sub extract_row_daemon {
	my ( $col, $cell, $row_href, $skipping ) = @_;
	$skipping ||= 0;

	# get formatted excel value for most columns
	my $value = $cell ? $cell->value() : '';

	#say $value;
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

		# data source (previously legacy flag)
		when (/^3$/) {
			$row_href->{feed_id} = $value;
		}

		# file date
		when (/^4$/) {
			unless ($value) {

				dsay "\terror parsing date field" unless $skipping;
				return;
			}

			# extract unformatted datetime and convert to filedate integer
			my $time_excel = $cell->unformatted();
			my $formatted_value = ExcelFmt( 'yyyymmdd', $time_excel );

			# skip if not scheduled for this day
			$row_href->{filedate}
				= $formatted_value ? $formatted_value : $value;
		}

		# file num
		when (/^5$/) {
			$row_href->{filenum} = $value ? $value : return;
		}

		# ID
		when (/^6$/) {
			$row_href->{id} = $value;
		}

		# comment
		when (/^7$/) {
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
		my $update_insert = "insert into [Updates] values 
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
		"insert into [Update_DIS] values
		('$feed_id', '$update_id')"
		)
		or warn "\tfailed to insert $update : $update_id into DIS linking\n";

	# insert scheduling info for each weekday
	my @time_offsets = offset_weekdays( $sched_epoch, $days );
	for my $pair_aref (@time_offsets) {
		my ( $this_offset, $weekday_code ) = @$pair_aref;
		$dbh_sched->do( "
			insert into [Update_Schedule] values 
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

# generate time offset for the current time GMT (epoch seconds)
sub now_offset {

	# calculate GM Time
	my ( $sec, $min, $hour, $mday, $mon, $year, $wday, $yday, $isdst )
		= gmtime(time);

	#my $offset = time2offset("$hour:$min");
	return $wday * 86400 + $hour * 3600 + $min * 60 + $sec;
}

# returns wd code for passed date (YYYY?MM?DD)
sub get_wd {
	my ($date) = @_;
	my ( $year, $month, $day ) = ( $date =~ m!(\d{4})\D?(\d{2})\D?(\d{2})! );
	dsay( $year, $month, $day );

	if ( !defined $year || !defined $month || !defined $day ) {
		$date ||= '<undef>';
		warn "unable to parse date for weekday: $date";
		return -1;
	}
	my $time = timegm( 0, 0, 0, $day, $month - 1, $year - 1900 );
	my ( $sec, $min, $hour, $mday, $mon, $y, $wday, $yday, $isdst )
		= gmtime($time);
	dsay $wday;
	return $wday;
}

# gets the sched_id of the next day given sched_id of current
# TODO make this work for odd schedules
sub next_sched_id {
	my $sched_id = shift;
	my $offset;
	my $next_sched_query = "
		select b.sched_id, b.sched_epoch
		from [Update_Schedule] a
		join
		update_schedule c
		on a.update_id = c.update_id
		join
		[Update_Schedule] b
		on a.update_id = b.update_id
		and ((b.weekday = a.weekday + 1) or (a.weekday = 6 and b.weekday = 0) or (a.weekday = 5 and b.weekday = 1 and c.weekday != 6 and c.weekday != 0))  
		where a.sched_id = $sched_id
		and b.sched_id = c.sched_id
	";
	dsay $next_sched_query;
	my $old_sched_id = $sched_id;
	( $sched_id, $offset ) = $dbh_sched->selectrow_array($next_sched_query);
	dsay($old_sched_id, $sched_id, $offset );
	
	
	
	return ( $sched_id, $offset );
}

# gets the sched_id of the next day given sched_id of current
# TODO make this work for odd schedules
sub prev_sched_offset {
	my $sched_id = shift;
	my $offset;
	my $next_sched_query = "
		select b.sched_epoch
		from [Update_Schedule] a
		join
		update_schedule c
		on a.update_id = c.update_id
		join
		[Update_Schedule] b
		on a.update_id = b.update_id
		and ((b.weekday = a.weekday - 1) or (a.weekday = 0 and b.weekday = 6) or (a.weekday = 1 and b.weekday = 5 and c.weekday != 6 and c.weekday != 0))  
		where a.sched_id = $sched_id
		and b.sched_id = c.sched_id
	";
	dsay $next_sched_query;
	(  $offset ) = $dbh_sched->selectrow_array($next_sched_query);
	dsay( $offset );
	
	
	
	return ( $sched_id, $offset );
}

# retrieve schedule IDs (aref of aref) from database for an update ID
sub get_sched_id {
	my ($update_id) = @_;

	my $scheds_aref = $dbh_sched->selectall_arrayref( "
		select sched_id, sched_epoch from [Update_Schedule]
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

# returns the wday for tomorrow - refresh_dis rollover fix
sub get_tomorrow_wday {
	my ( $tyear, $tmonth, $tday ) = @_;
	my $time = timegm( 0, 0, 0, $tday, $tmonth - 1, $tyear - 1900 );
	my ( $sec, $min, $hour, $mday, $mon, $year, $wday, $yday, $isdst )
		= gmtime( $time + 86400 );
	return $wday;
}

# returns code for current weekday
sub now_wd {
	my ( $sec, $min, $hour, $mday, $mon, $year, $wday, $yday, $isdst )
		= gmtime(time);

	#my @weekdays = qw(N M T W R F S);
	return $wday;
}

# update all older builds issued in the same update
sub backdate {
	my ( $backdate_updates, $trans_offset, $late,
		 $fd,               $fn,           $build_num,
		 $orig_sched_id,    $trans_num,    $feed_date
	) = @_;

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
						  filenum      => $fn,
						  transnum     => $trans_num,
						  feed_date    => $feed_date
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

# compares two GMT offsets (perceived trans time vs scheduled time)
# TODO intelligently handle week boundary
sub comp_offsets {
	my ( $trans_offset, $sched_offset, $weekend_prev_day ) = @_;

	#	# properly handle monday's early feeds
	#	if ($weekend_prev_day) {
	#
	#	}
	#

	# get seconds difference
	my $offset_diff = $trans_offset - $sched_offset;
	dsay "offset diff: $offset_diff";

	# if the difference is greater than the allowed lateness... mark as late
	# do an extra check for week rollovers
	if ( $offset_diff > $cfg->late_threshold ) {

		# this is a weekly rollover, do comparison differently
		# subtract 4 or 5 days
		#		if ($offset_diff > 400000 ) {
		#			74899
		#			$offset_diff -= 345600;
		#		}
		# return late
		dsay 'c_o: late';
		return 1;
	}

	# early but not more than a day ago or within acceptable late threshold
	elsif ( $offset_diff < $cfg->late_threshold )

		# no need for this now that filtered by feed_date from sched_id
		#&& ( $offset_diff > -86400 || $is_weekend ) )
	{

		#		if ($is_weekend) {
		#			dsay 'weekend case, early?';
		#		}
		#		else {
		#			dsay 'non-weekend, early?';
		#		}
		return 0;
	}

	# otherwise we're still waiting
	else {
		dsay 'c_o: wait';
		return -1;
	}
}

# OLD - works in minutes not seconds
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
	my $server   = 0;
	my $special  = 0;

	dsay $sender;

	# make sure this is from DIS1, as all DIS content should be
	if ( $sender =~ m/NTCP-DIS1-NTCP-(.*)/ ) {

		# DIS 1..3
		if ( $sender =~ m/DIS(\d+)(\D*)$/ ) {
			$server = $1;

			# TODO there will be a flag here {P} for special UPDs
			# handle accordingly
			$special = $2;
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

		# PROD01 - special updates, I guess
		elsif ( $sender =~ m/TQAPROD01/ ) {
			return $dbh_prod1;
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

# return UPD date based on current date
sub upd_date {
	my ($now_date) = @_;
	unless ( defined $now_date ) {
		$now_date = now_date();
	}
	my ( $year, $month, $mday ) = parse_filedate($now_date);
	my $time_arg = timegm( 0, 0, 0, $mday, $month - 1, $year - 1900 )
		or say "[1]\tupd_date() failed for: $now_date\n";

	# get DOW
	my ($wday) = ( gmtime($time_arg) )[6];

	# weekends use Friday's UPD
	# fastest way to get
	#	sat
	if ( $wday == 6 ) {
		$time_arg -= 86400;
	}

	#	sun
	elsif ( $wday == 0 ) {
		$time_arg -= 172800;
	}

	#	mon
	elsif ( $wday == 1 ) {
		$time_arg -= 259200;
	}

	# all other days use previous date
	else {
		$time_arg -= 86400;
	}

	# convert back to YYYYMMDD format
	( $year, $month, $mday ) = gmtime($time_arg)
		or say "[2]\tupd_date() failed for: $time_arg\n";

	# zero pad month and day
	return sprintf( '%u%02u%02u', $year, $month, $mday );
}

sub is_prev_date {
	my ($update_id) = @_;
	
	return ($dbh_sched->selectrow_array("
		select prev_date from updates where update_id = $update_id
	"));
}

# store/modify update history entry
sub update_history {
	my $hashref = shift;
	my ( $update_id, $sched_id, $trans_offset, $late_q,
		 $fd_q,      $fn_q,     $trans_num,    $is_legacy,
		 $feed_date, $seq_num
		)
		= ( $hashref->{update_id},    $hashref->{sched_id},
			$hashref->{trans_offset}, $hashref->{late},
			$hashref->{filedate},     $hashref->{filenum},
			$hashref->{transnum},     $hashref->{is_legacy},
			$hashref->{feed_date},    $hashref->{seq_num}
		);
	$is_legacy ||= 0;

	($fd_q, $fn_q) = map {!defined $_ || $_ =~ /undef/i ? 'NULL' : $_} ($fd_q, $fn_q); 

	dsay( $update_id, $sched_id, $trans_offset, $late_q,
		  $fd_q,      $fn_q,     $trans_num,    $is_legacy,
		  $feed_date, $seq_num
	);
	my $weekday = get_wd($feed_date);

	# first, get the last UPD (or just seq_num) this update_id had
	# if current is identical, skip storing again
	if ( defined $seq_num ) {
		my $select_last_upd = "
			select top 1 [timestamp]
			from update_history
			where
			update_id = $update_id
			and seq_num = $seq_num
			and datediff(dd, [timestamp], '$feed_date') < 7
			order by [timestamp] desc
		";
		dsay $select_last_upd;
		my ($ts_prev) = $dbh_sched->selectrow_array($select_last_upd);
		if ( defined $ts_prev ) {
			say "\tnot storing dup seq_num ($seq_num)";
			return;

			# check that filedate and filenum haven't been updated
			# this is where update occurs for new 'recvd but processing' flag

			#			if (defined $fd_prev && defined $fn_prev) {
			#				say "$fd_prev $fn_prev";
			#			}
			#			elsif () {
			#				say "UPD not defined";
			#			}
		}

	}

	# K = skipped
	if ( $is_legacy && $late_q eq 'K' ) {
		dsay "\tchecking legacy skip...";
		my $select_skip_query = "
			select hist_id, late from update_history
			where feed_date = '$feed_date'
			and sched_id = '$sched_id'
		";

		my ( $hist_id, $status )
			= $dbh_sched->selectrow_array($select_skip_query);
		dsay( $hist_id, $status );

		# the last record for this sched_id, feed_date wasn't a skip?
		# could update...
		if ( defined $status && $status ne 'K' ) {
			say "\tattempted to skip previously stored update";
		}
		elsif ( !defined $hist_id ) {
			$trans_num ||= 'NULL';
			$seq_num   ||= 'NULL';
			my $insert_skip_query = "
				insert into update_history
				values 
				($update_id, $sched_id, 
				$trans_offset, $fd_q, $fn_q, 
				GETUTCDATE(), 'K', $trans_num, 
				'$feed_date', $seq_num) 
		";

			#say $insert_skip_query;

			$dbh_sched->do($insert_skip_query);
		}
		return;
	}

	#say "$update_id $sched_id $trans_offset";
	#say "( $update_id, $sched_id, $trans_offset, $late_q, $fd_q, $fn_q )";
	# if there was a trasnum some things need to be done:
	my $select_filter = "
	and feed_date = '$feed_date'
	and sched_id = $sched_id";
	if ( $trans_num && $late_q ne 'E' ) {

		# only filter by transaction number if one was passed in
		$select_filter .= "
		and transnum = $trans_num
		";
	}
	elsif ( $late_q eq 'E' ) {
		$select_filter .= "
		and transnum = $trans_num
		";
	}

	# otherwise set it to impossible value
	# (for updates that have fd/fn but are not yet done processing)
	else {
		$trans_num = -1;
	}

	my $get_hist_query = "
		select hist_id, late, filedate, filenum, transnum
		from Update_History
		where feed_date = '$feed_date'
		$select_filter
	";

	dsay $get_hist_query;

	# update if late and not yet recvd
	# or skip if it was already recvd
	# otherwise, insert
	my ( $hist_id, $late, $fd, $fn, $hist_trans_num )
		= $dbh_sched->selectrow_array($get_hist_query);

	# if no history record found (most likely for this transaction number)
	# check if just the transaction number needs to be updated
	# TODO no trans num updates are needed ever
	my $update_trans_flag = 0;
	if ( !$hist_id && $fd_q && $fn_q ) {

#		# rewrite query to handle nulls as well
#		my $upd_filter = "and seq_num = $seq_num";
##		if ( $fd_q eq 'NULL' ) {
##
##			# this is an empty update, need to add feed date
##			$upd_filter = "
##			and filedate is NULL
##			and filenum is NULL
##			and feed_date = '$feed_date' 
##			";
##		}
##		else {
##			$upd_filter = "
##			and filedate = $fd_q
##			and filenum = $fn_q
##			"
##		}
#		my ( $old_trans_num, $old_feed_date );
#		my $lookbehind_query = "
#		select hist_id, transnum, feed_date
#		from Update_History
#		where  update_id = $update_id
#		$upd_filter
#		";
#		dsay $lookbehind_query;
#		( $hist_id, $old_trans_num, $old_feed_date )
#			= $dbh_sched->selectrow_array($lookbehind_query);
#
#		#		say "select hist_id, transnum, feed_date
#		#		from Update_History
#		#		where  sched_id = $sched_id
#		#		and filedate = $fd_q
#		#		and filenum = $fn_q";
#		#say "$old_trans_num $trans_num";
#		if ( defined $old_trans_num && $old_trans_num != $trans_num ) {
#			say "\tnew transaction number";
#			$update_trans_flag = 1;
#		}
#		elsif ( !defined $old_trans_num ) {
#			if ( ( $late_q eq 'Y' || $late_q eq 'E' ) && !$is_legacy ) {
#
#				say "\tnext trans number";
#				my $sched_offset;
#				my $old_sched_id = $sched_id;
#				my $old_sched_offset = $sched_offset;
#				($sched_id, $sched_offset) = next_sched_id($sched_id);
#				dsay "$sched_id $sched_offset";
#				say "$sched_id $sched_offset";
#				if (defined $sched_offset && $sched_offset && $trans_offset && $trans_offset != -1 && is_prev_date($update_id)) {
#					say "\tseqnum found";
#					my $comp_result = comp_offsets($trans_offset, $sched_offset);
#					# late
#					if ($comp_result == 1) {
#						$late_q = 'Y';
#					}
#					# wait
#					elsif ($comp_result == 0) {
#						$late_q = 'N';
#					}
#					# early
#					elsif ($comp_result == -1) {
#						$late_q = 'N';
#					}
#					else {
#						say "\terror $sched_id";
#						return;
#					}
#				}
#				else {
#					say "\tseq_num not found";
#					#say  use original $sched_id vs $old_sched_id";
#					#my ($oy, $om, $od) = parse_filedate($old_feed_date);
#					$old_feed_date ||= 0;
#					$sched_id = $old_sched_id; 
#					#$sched_id ||= $old_sched_id;
#					#$sched_offset ||= $old_sched_offset;
#					($sched_id, $sched_offset) = next_sched_id($sched_id);
#					$sched_id = $old_sched_id; 
#					#$sched_offset ||= $old_sched_offset;
#					$old_feed_date =~ s/(\d+)\D+(\d+)\D+(\d+)\D.*/$1$2$3/;
#					$feed_date =~ s/(\d+)\D+(\d+)\D+(\d+)\D.*/$1$2$3/;
#					
#					#my ($ny, $nm, $nd) = parse_filedate($feed_date);
#					# this is a newer feed date, store in next sched_id
#					
#					if ($old_feed_date < $feed_date) {
#						# recalc for new sched_id
#						my $comp_result = comp_offsets($trans_offset, $sched_offset);
#						# late
#						if ($comp_result == 1) {
#							$late_q = 'Y';
#						}
#						# wait
#						elsif ($comp_result == 0) {
#							$late_q = 'N';
#						}
#						# early
#						elsif ($comp_result == -1) {
#							$late_q = 'N';
#						}
#						else {
#							say "\terror $sched_id";
#							return;
#						}
#					}
#					# otherwise revert sched_id
#					else {
#						$sched_id = $old_sched_id;
#					}
#				}
#				
#			}
#		}
#		elsif ( $old_trans_num == $trans_num && $old_feed_date eq $feed_date )
#		{
#			say "\tnon-enum from prev day, skip";
#			return;
#		}
	}

	# recvd already, return
	if (    ( $fd && $fn || ( defined $late && $late eq 'E' ) )
		 && !$update_trans_flag
		 && $trans_offset != -1
		 && $trans_num != -1 )
	{
		say "\talready stored $update_id";
		return;
	}

# TODO verify using feed_date rather than history entry for sched_id
# already an entry in history (late), update with newly found filedate filenum
	elsif ( defined $hist_id
			&& (    ( ( $fd_q && $fn_q ) && ( !$fd || !$fn ) )
				 || $trans_offset != -1
				 || $update_trans_flag )
		)
	{
		if ( defined $late && ( $late eq 'E' || $late eq 'N' ) ) {
			say "\talready stored $update_id ",
				( $late eq 'E' ? '(empty)' : '(recvd)' );
			return;
		}

		$seq_num ||= 'NULL';

		say "\t$update_id updating: "
			. ( $update_trans_flag
				? '(latest transnum)'
				: '(wait/late -> recvd)'
			);
		$dbh_sched->do( "
			update Update_History
			set filedate = $fd_q, filenum = $fn_q, transnum = $trans_num, seq_num = $seq_num
			where hist_id = $hist_id
		" ) or say "update failed";

	}

	# not recvd and never seen, insert new record w/ filedate and filenum
	elsif ( ( !$hist_id && $fd_q && $fn_q )
			|| $trans_offset == -1 && $trans_num != -1 )
	{

		#		if ($update_trans_flag) {
		#			say "\t\trecvd new transnum (next), skipping";
		#			return;
		#		}

  # handle legacy miss, mark as recvd but without details due to parsing error
		if ( $trans_offset == -1 && !$is_legacy ) {
			say "\t$update_id DIS not done today yet";

			#return;
		}
		elsif ($is_legacy) {
			say "\t$update_id legacy recvd";
			if ($trans_offset == -1) {
				say "\tbut parsing error";
			}
			#return;
		}

		say "\t$update_id inserting (found)";

		# if skipped, change status for insert
		if ( $fd_q =~ m/skip|hold/i || $fn_q =~ m/skip|hold/i ) {
			( $fd_q, $fn_q ) = ( 'NULL', 'NULL' );
			$late_q = 'K';
		}
		$fd_q    ||= 0;
		$fn_q    ||= 0;
		$seq_num ||= 'NULL';

		# retrieve filedate and filenum from TQALic on nprod1
		#my ( $fd, $fn ) = get_fdfn($trans_num);
		my $insert_hist = "
			insert into Update_History 
			values
			($update_id, $sched_id, $trans_offset, $fd_q, $fn_q, GetUTCDate(), '$late_q', $trans_num, '$feed_date', $seq_num )
		";
		dsay $insert_hist;
		$dbh_sched->do($insert_hist) or warn "could not insert!!!\n";
	}

	# otherwise, it is late/wait and has no filedate filenum, insert
	else {
		say "\t$update_id no history found";
		$seq_num ||= 'NULL';
		$trans_num = 'NULL' if $trans_num != -1;
		my $insert_hist = "
			insert into Update_History 
			values
			($update_id, $sched_id, $trans_offset, NULL, NULL, GetUTCDate(), '$late_q', $trans_num, '$feed_date', $seq_num)
		";
		$dbh_sched->do($insert_hist) or say "\t\talready waiting...";

		#say "\t$update_id not found";
	}

}

sub get_sched_wd {
	my ($sched_id) = @_;
	my $select_sched_wd = "
		select weekday from update_schedule 
		where 
		sched_id = $sched_id
		";
	my ($wd) = $dbh_sched->selectrow_array($select_sched_wd)
		or warn "could not find wd for $sched_id\n";
	return $wd;
}

# convert SQL datetime to offset
# returns total offset (including day of week)
sub datetime2offset {
	my ($datetime) = @_;

	# bounce empty datetimes
	( dsay 'no datetime value passed to offset conversion routine'
	   and return )
		unless $datetime;
	my ( $year, $month, $date, $hour, $minute, $second )
		= ( $datetime =~ m/(\d+)-(\d+)-(\d+) (\d+):(\d+):(\d+)/ );

	#my $dow = Date_DayOfWeek( $month, $date, $year );
	my $dow = get_wd($datetime);
	return $dow * 86400 + $hour * 3600 + $minute * 60 + $second;
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
		select top 1 update_id from [Updates] 
		where name = '$name'
	";
	my $id = ( ( $dbh_sched->selectall_arrayref($select_query) )->[0] )->[0];
	return $id;
}

# take update_id return feed_id, if any
sub get_feed_id {
	my ($update_id) = @_;
	return unless $update_id;
	my $select_query = "
		select top 1 feed_id from [Update_DIS]
		where update_id = $update_id
	";
	return ($dbh_sched->selectrow_array($select_query))[0];
}

# current timestamp SQL DateTime format for GMT or default to machine time (local)
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
										 DEFAULT  => '',
							 },
						   }
	);

# bring in configuration code from external file
# separate AppConfig hash and possible future configs for ease of use
# INV look into best practices for calling sub-packages (another module is preventing me from using Config.pm)
	require 'Config/config.pl';
	TQASched::Config::define_defaults( \$cfg );

# first pass at CLI args, mostly checking for config file setting (note - consumes @ARGV)
	$cfg->getopt();
	my $cfg_path = ( defined $relative_path ? "$relative_path/" : '' )
		. $cfg->config_file();

# parse config file for those vivacious variables and their rock steady, dependable values
	$cfg->file($cfg_path);

	# second pass at CLI args, they take precedence over config file
	$cfg->getopt( \@CLI );

	# set the debug mode on load
	#$debug_mode = $cfg->debug;

	# check that usage is availabe and/or defined
	$cfg->pod( find_pod_usage($cfg) );

	# get hashes of database connection information
	(  $sched_db, $auh_db,  $prod1_db, $dis1_db,
	   $dis2_db,  $dis3_db, $dis4_db,  $dis5_db )
		= map { get_handle_hash($_) } qw(sched_db auh_db prod1_db 1 2 3 4 5);

	return $cfg;
}

# verify that usage POD file exists
# returns path if found, else returns undef
# TODO use pod_find and break doc into components
sub find_pod_usage {
	my ($cfg) = @_;
	my ( $pod_msg, $pod_error );
	if ( !defined $cfg->pod ) {
		$pod_msg   = "POD documentation file not configured.";
		$pod_error = 1;
		dsay "[Usage not available]";
	}
	elsif ( !-f $cfg->pod ) {
		$pod_msg   = "POD documentation file not found: $cfg->pod.";
		$pod_error = 1;
	}
	else {
		$pod_msg = "POD docs found in $cfg->pod";
	}
	say $pod_msg if defined $cfg->verbosity && $cfg->verbosity > 1;
	dsay '[Usage not available]' if $pod_error;
	return $pod_error ? 0 : $cfg->pod;
}

# parse YYYYMMDD into (y,m,d)
sub parse_filedate {
	my ($filedate) = @_;
	return unless $filedate;
	if ( my ( $year, $month, $mday )
		 = ( $filedate =~ m/(\d{4})\D*(\d{2})\D*(\d{2})/ ) )
	{
		return ( $year, $month, $mday );
	}
	return;
}

# handle any errors in AppConfig parsing - namely log them
sub appconfig_error {
	my $msg = sprintf(@_);

	dsay $msg;

	# hacky way to force always writing this log to top-level dir
	# despite the calling script's location
	#	my $top_log = ( __PACKAGE__ ne 'TQASched'
	#					? $INC{'TQASched.pm'} =~ s!\w+\.pm!!gr
	#					: ''
	#	) . $cfg->log();

	#	write_log( { logfile => $top_log,
	#				 type    => 'WARN',
	#				 msg     => $msg, #join( "\t", @_ )
	#			   }
	#	);
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

# translate weekday string to array of corresponding offsets
sub offset_weekdays {
	my ( $sched_offset, $days ) = @_;

  # hardcoded hash of day to weekday integer (as from localtime - sunday is 0)
	my %wd_lookup = ( Su => 0,
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
		my ( $first_int, $second_int )
			= ( $wd_lookup{$first_date}, $wd_lookup{$second_date} );

		# this should be the case most of the time

		if ( $first_int < $second_int ) {

		 # iterate over each day and push to return array - easy case, no wrap
			while ( $first_int <= $second_int ) {
				push @offsets,
					[ $first_int * $day_increment + $sched_offset, $first_int
					];
				$first_int++;
			}

		}
		elsif ( $first_int > $second_int ) {
			while (1) {
				push @offsets,
					[ $first_int * $day_increment + $sched_offset, $first_int
					];
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
		my ( $first_int, $second_int, $last_int )
			= ( $wd_lookup{$first_date}, $wd_lookup{$second_date},
				$wd_lookup{$last_date} );

		# push last day as a single, it's easy
		push @offsets,
			[ $last_int * $day_increment + $sched_offset, $last_int ];

		# handle other date range the same as the last one
		if ( $first_int < $second_int ) {

		 # iterate over each day and push to return array - easy case, no wrap
			while ( $first_int <= $second_int ) {
				push @offsets,
					[ $first_int * $day_increment + $sched_offset, $first_int
					];
				$first_int++;
			}
		}
		elsif ( $first_int > $second_int ) {
			while (1) {
				push @offsets,
					[ $first_int * $day_increment + $sched_offset, $first_int
					];
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

	my ( $dbh, $success, $tries ) = ( undef, 0, 0 );

	my $db_name = 'master';
	if (exists $db->{name} && defined $db->{name} && $db->{name} !~ m/master/i) {
		$db_name = $db->{name};
	}
	#say "$db_name $db->{server}";
	# force connection, for server reboots/not responsive
	while ( !$success ) {
		$tries++;
		$dbh = DBI->connect(

			# connecting to master since database may need to be created

			sprintf(
				"dbi:ODBC:Database=%s;Driver={SQL Server};Server=%s;UID=%s;PWD=%s",
				$db_name, $db->{server},
				$db->{user}, $db->{pwd}
			),
			{ RaiseError => 0, PrintError => 1 }
		);

		if ( defined $dbh && !$DBI::err ) {
			$success = 1;
		}

	}

	if ( $tries > 1 ) {
		dsay "took $tries tries to connect to $db->{name}";
	}

	return $dbh;

	# or die "failed to initialize database handle\n", $DBI::errstr;
}

# create database if not already present
sub create_db {
	say 'checking if TQASched database already exists';

	# if already exists, return
	if ( check_db('TQASched') && !$cfg->force_create ) {
		write_log(
			{  logfile => $cfg->log,
			   type    => 'ERROR',
			   msg =>
				   'TQASched database already exists, but create was requested'
			}
		);
		return 1;
	}
	elsif ( !$cfg->force_create ) {
		say 'not found,';
		write_log( { logfile => $cfg->log,
					 type    => 'INFO',
					 msg => 'creating TQASched database and table framework'
				   }
		);
		$dbh_sched->do('create database TQASched');
	}

	# slurp and execute sql create file
	$dbh_sched->do( ${ slurp_file( $cfg->sql_file ) } )
		or warn "failed to populate TQASched db with tables\n";

  #	# create update table
  #	$dbh_sched->do(
  #		"create table [Updates] (
  #	update_id int not null identity(1,1),
  #	name varchar(255) not null unique,
  #	priority tinyint,
  #	is_legacy bit
  #	)"
  #	) or die "could not create Updates table\n", $dbh_sched->errstr;
  #
  #	# create update/schedule linking table
  #	$dbh_sched->do(
  #		"create table [Update_Schedule] (
  #	sched_id int not null identity(1,1),
  #	update_id int not null,
  #	weekday tinyint not null,
  #	sched_epoch int not null
  #	)"
  #		)
  #		or die "could not create Update_Schedule table\n", $dbh_sched->errstr;
  #
  #	# create history tracking table
  #	$dbh_sched->do(
  #		"create table [Update_History] (
  #	hist_id int not null identity(1,1),
  #	update_id int not null,
  #	sched_id int not null,
  #	hist_epoch int,
  #	filedate int,
  #	filenum tinyint,
  #	timestamp DateTime,
  #	late char(1),
  #	transnum int
  #	)"
  #		)
  #		or die "could not create Update_History table\n", $dbh_sched->errstr;
  #
  #	# create linking table from DIS feed_ids to update_ids
  #	$dbh_sched->do( "
  #	create table [Update_DIS] (
  #	update_dis_id int not null identity(1,1),
  #	feed_id varchar(20) not null,
  #	update_id int not null
  #	)
  #	" )
  #		or warn
  #		"\tcould not create DIS linking table - Update_DIS, may already exist\n";

	say 'done creating db';
	return 1;

}

# slurps specified file into a string aref
sub slurp_file {
	local $/ = undef;
	open my $fh, '<', shift;
	my $contents = <$fh>;
	close $fh;
	return \$contents;
}

# check that database exists
sub check_db {
	my $db_name     = shift;
	my $check_query = "select db_id('$db_name')";
	return ( ( $dbh_sched->selectall_arrayref($check_query) )->[0] )->[0];
}

# get latest schedule checklist full path
# optionally pass in target date components
sub find_sched {

	# optional argument to refresh specific spreadsheet file
	my ( $tyear, $tmonth, $tmday );
	if ( ( $tyear, $tmonth, $tmday ) = @_ ) {
		dsay
			"spreadsheet target params = year: $tyear month: $tmonth day: $tmday";
	}

	# otherwise get file for current execution date
	else {
		dsay 'no target spreadsheet, using gmtime';
		( $tmday, $tmonth, $tyear ) = ( gmtime(time) )[ 3 .. 5 ];
		$tmonth++;
		$tyear += 1900;
	}

	my $tdate = sprintf( '%u%02u%02u', $tyear, $tmonth, $tmday );

	say "finding checklist for $tdate...";

	my $checklist_path = $cfg->checklist_path;

	print "connecting to network directory: $checklist_path...";
	opendir( my $dir_fh, $checklist_path )
		or say "could open/find checklist dir: $checklist_path\n$!\n" and return;
	say ' connected' if $dir_fh;
	my @files = grep m/^dailychecklist_\d+\.xls$/i, readdir($dir_fh);
	closedir $dir_fh;
	dsay "\tfound ", scalar(@files), 'checklist files:';
	my @checklist_files = reverse sort @files;
	#dsay @checklist_files;

	my $tfile = '';
	# use config args checklist if specified
	if ( $cfg->checklist ) {
		$tfile = $cfg->checklist;
	}
	# otherwise match filename from target date
	else {
		my $last_monday = last_monday( $tyear, $tmonth, $tmday );
		$tfile = "DailyCheckList_${last_monday}.xls"		
	}
	my $checklist_target = "$checklist_path/$tfile";
	
	# checklist has been found
	if (-f $checklist_target) {
		dsay "checklist found: $checklist_target", 'existing files:', @checklist_files;
		return $checklist_target;
	}
	# checklist not found, create from master
	# TODO timing on this? will it be generated in time?
	else {
		dsay "checklist not found for $tdate:", $checklist_target, 'existing files:', @checklist_files;
		say "$tfile not found, generating new checklist...";	
		if (create_checklist($checklist_target)) {
			return $checklist_target;
		}
		else {
			say 'no checklist, failed to generate';
			dsay "could not create checklist $checklist_target";
			return;
		}
	}
	
	
}

# takes date parts and returns last monday
sub last_monday {
	( dsay 'nothing passed to last_monday' and return ) unless @_;
	
	my ( $year, $month, $mday, $wday )
		= ( $_[0] - 1900, $_[1] - 1, @_[ 2 .. 3 ] );
	dsay "$year $month $mday";
	my $time = timegm( (0) x 3, $mday, $month, $year )
		or ( dsay "failed to timegm @_" and return );

	# if weekday is already known it is optional and will speed it up
	$wday ||= -1;

	dsay "calculating date of last monday for: @_";
	( $mday, $month, $year, $wday ) = ( gmtime($time) )[ 3 .. 6 ];
	my $monday_date = '';

	# already mon (1)
	if ( $wday == 1 ) {
		dsay 'last_monday - date is monday';
	}
	else {

		# tues - sat (2 - 6)
		if ( $wday > 1 ) {
			$time -= 86400 * ( $wday - 1 );
		}

		# sun (0)
		else {

			# decrement 6 days
			$time -= 518400;
		}
		( $mday, $month, $year, $wday ) = ( gmtime($time) )[ 3 .. 6 ];
	}

	$monday_date = sprintf( '%u%02u%02u', $year + 1900, $month + 1, $mday );
	( say "last_monday sanity check failed - $wday for $monday_date"
	   and return )
		if $wday != 1;
	
	dsay "\twday: $wday on $monday_date is/was/will be last monday";
	return $monday_date;
}

# add ordinal component to numeric values (-st,-nd,-rd,-th)
sub ordinate {
	my ($number) = (@_);
	my $ord = '';
	given ($number) {
		when (/1[123]$/) { $ord = 'th' }
		when (/1$/)      { $ord = 'st' }
		when (/2$/)      { $ord = 'nd' }
		when (/3$/)      { $ord = 'rd' }
		default          { $ord = 'th' };
	}
	return $number . $ord;
}

# returns whether a day code is a weekend (6,0)
sub is_weekend {
	my ($code) = @_;
	if ( defined $code && $code == 6 || $code == 0 ) {
		return 1;
	}
	return;
}

# create renamed copy of master checklist
sub create_checklist {
	my ($new_checklist_path) = @_;
	my $mastersheet
		= $cfg->master_checklist;
	if (! -f $mastersheet) {
		say "could not find master checklist: $mastersheet";
		return;
	}
	unless (copy( $mastersheet, $new_checklist_path )) {
		say "failed to create new spreadsheet: $!";
		dsay 'create_checklist copy failed: ',$mastersheet, $new_checklist_path;
	}
	else {
		say "created new checklist: $new_checklist_path";
		return 1;
	}
	
}

# utility for changing the scheduled time for an update
sub update_scheduling {
	my ($update_id) = @_;

	my $update_sched_query = "
		update Update_Schedule
		set sched_epoch = ? 
		where
		update_id = ?
	";

	my $sth_update = $dbh_sched->prepare($update_sched_query);
}

# poll auh metadata for DIS feed statuses
sub refresh_dis {

	#	my ( $tyear, $tmonth, $tday, $tsched_id ) = @_;

	my $opts_href = shift;

	my ( $tyear, $tmonth, $tday, $tsched_id, $pause_mode, $tupdate_id )
		= map { exists $opts_href->{$_} ? $opts_href->{$_} : undef }
		qw(year month day sched_id pause_mode update_id);

	my $target_date_string = sprintf '%u%02u%02u', $tyear, $tmonth, $tday;

	#say $target_date_string;

	# make sure that all the handles are defined;
	check_handles();

	# argument = weekday to scan
	my $current_wd = $tyear ? get_wd($target_date_string) : now_wd();

	#my $current_offset = now_offset();
	for my $current_wd ($current_wd) {
		dsay "DIS scanning weekday: $current_wd";

		# targetted schedule ID update run
		my $filter_sched
			= is_weekend($current_wd)
			? "and ((weekday = 0 or weekday = 6) or (prev_date = 1 and weekday = 1))"
			: "and us.weekday = $current_wd";

		if ( defined $tsched_id ) {

			# TODO fix for weekend target sched_id
			if ( $current_wd == 0 ) {

			}
			$filter_sched = "and us.sched_id = $tsched_id";
		}
		elsif ( defined $tupdate_id ) {
			$filter_sched .= "\nand us.update_id = $tupdate_id";
		}

		# get all updates expected for the current day
		my $expected = "
		select ud.feed_id, u.name, us.sched_epoch, us.sched_id, us.update_id, u.prev_date
		from 
			Update_Schedule us,
			Update_DIS ud,
			Updates u
		where ud.update_id = us.update_id
		and u.update_id = ud.update_id
		and u.is_legacy = 0
		and us.enabled = 1
		$filter_sched
		";
		dsay $expected;
		my $sth_expected = $dbh_sched->prepare($expected);
		$sth_expected->execute();
		my $updates_aref = $sth_expected->fetchall_arrayref();

		my $first_run = 1;

		# iterate over each of them and determine if they are completed
		for my $update_aref ( @{$updates_aref} ) {

			my $trans_offset;
			my ( $feed_year, $feed_mon, $feed_day );

			# extract update info
			my ( $feed_id,  $name,      $offset,
				 $sched_id, $update_id, $prev_date
			) = @{$update_aref};
			dsay( $feed_id,  $name,      $offset,
				  $sched_id, $update_id, $prev_date );

			# handle annoying FIEJV feeds not being enumerated
			# TODO fix special case for FIEJV

			if ( $feed_id =~ m/fiejv|rdc/i ) {
				dsay "$name: skipping";

	# get earliest FIEJV feed that hasn't been received, assume it is this one
	# skip for debugging purposes now
				next;
			}
			say "$name - $update_id - $sched_id";
			$first_run = pause_mode($first_run) if $pause_mode;

			# get build number (optional) from feed name
			my ( $stripped_name, $build_num ) = ( $name =~ m/(.*)#(\d+)/ );
			# some special cases for build numbers
			($build_num) = ($name =~ m/\((\d+)\)/) unless $build_num;  
			$build_num = 0 if defined $stripped_name && $stripped_name =~ m/first call/i;
			
#
#		# this could be associated with a different weekday, verify feed date
#		my $feed_date_rewind = '';
#		if (defined $prev_date && $prev_date == 1)  {
#			say "\tusing yesterday's feed date";
#			$feed_date_rewind = "and FeedDate = DateAdd(dd, $feed_date_rewind, '$target_date_string')";
#		}
			#my $yfeed_date = date_math(-1, $target_date_string);
			#my $feed_date_filter = "and feeddate = '$yfeed_date'";
			my $feed_date_filter = '';
			#if ( $current_wd != 1 && defined $build_num ) {
			#if ( defined $build_num ) {

				# weekend case, get next sched_id and its corresponding offset
#				if (    $current_wd == 0
#					 && defined $prev_date
#					 && $prev_date == 1 )
#				{
					
				
				if (defined $prev_date && $prev_date == 1 && $build_num) {
					say "\tdoing some schedule_id/feed_date magic";
					if (my ($psched_id, $poffset) = prev_sched_offset($sched_id)) {
						#$offset = $poffset;
						$sched_id = $psched_id; 
					}
#					my	( $nsched_id, $noffset ) = next_sched_id($sched_id);
#					if (defined $nsched_id) {
#						#$sched_id = $nsched_id if $nsched_id > $sched_id;
#						$offset = $noffset;
#					}
				}
				my $sched_feed_date
					= sched_id2feed_date( $sched_id, $target_date_string, (($build_num && $prev_date) || !$build_num ? 0 : -1) );
				dsay $sched_feed_date;
				$feed_date_filter = "and feeddate = '$sched_feed_date'";
				# rewind Data Explorers (DXL_Daily) an extra time
				# TODO figure out DXL
				if ($update_id == 156) {
					$sched_feed_date = date_math(-1, $sched_feed_date);
					$feed_date_filter = "and feeddate = '$sched_feed_date'";	
				}
#				}
				
			#}

# double duty query
# gets all needed info for non-enumerated feeds
# gets DIS server (sender) for enumerated feeds to hit for build-specific details
# TODO cannot simply take newest, need to check feed date
			my $transactions = "
			select top 1 Status, BuildTime, FileDate, FileNum, Sender, TransactionNumber, ProcessTime, FeedDate, SeqNum, filesize 
			from [TQALic].dbo.[PackageQueue] 
			with (NOLOCK)
			where TaskReference LIKE '%$feed_id%'
			$feed_date_filter
			order by SeqNum desc
		";
			dsay $transactions;

 #say $transactions;
 #--	and feeddate = DateAdd(dd, $feed_date_rewind, '${tyear}${tmonth}${tday}')
			my ( $status,  $exec_end,  $fd,         $fn,
				 $sender,  $trans_num, $build_time, $feed_date,
				 $seq_num, $filesize
			) = $dbh_prod1->selectrow_array($transactions);

			dsay( $status,     $exec_end,  $fd,
				  $fn,         $sender,    $trans_num,
				  $build_time, $feed_date, $seq_num, $filesize
			);

			# handle daily (non-enum) empty feeds now and go to next
			if ( !$filesize && $status && !$build_num) {
					say "\tthis was an empty non-enum update";
					update_history(
								  { update_id => $update_id,
									sched_id  => $sched_id,
									trans_offset =>
										( datetime2offset($exec_end) || -1 ),
									late      => 'E',
									filedate  => 'NULL',
									filenum   => 'NULL',
									transnum  => $trans_num,
									feed_date => $feed_date,
									seq_num   => $seq_num,
								  }
					);
					next;
			}

			#my $backdate_updates;

			# if this is an enumerated feed
			# check the last execution time of that build
			# in the correct DIS server
			# First Call #? is not really an enumerated feed
			if ( $build_num && $name !~ m/first call/i ) {
				say "\thandling enum: $build_num";

				# RDC TR Business Classifications has no build numbers
				if ( $name =~ m/RDC Daily-Thomson Reuters Business/i ) {

					#say "\tRDC, build = 0 instead";
					$build_num = 0;
				}
				unless ($sender) {
					say "\tno DIS sender found for $name, skipping";
					next;
				}

	   # backdate builds packaged in the same UPD
	   #				my $backdate_query = "
	   #				select us.sched_id, u.name, u.update_id, uh.filedate, uh.feed_date
	   #				from update_schedule us
	   #				join updates u
	   #					on u.update_id = us.update_id
	   #				left join update_history uh
	   #					on uh.sched_id = us.sched_id
	   #				--	and DateDiff(dd, [timestamp], GETUTCDATE()) < 1
	   #				where us.weekday = $current_wd
	   #				and uh.transnum = $trans_num
	   #				and u.name LIKE '$stripped_name%'
	   #			";

				#say $backdate_query;
				#
				#				$backdate_updates
				#					= $dbh_sched->selectall_arrayref($backdate_query);
				my $dbh_dis = sender2dbh($sender);

				my $sched_feed_date = $target_date_string;
				if ( $current_wd != 1 ) {
					$sched_feed_date = sched_id2feed_date( $sched_id,
														$target_date_string );
					$feed_date_filter = "and FeedDate = '$sched_feed_date'";
				}

# retrieve last transaction number for this build number
#TODO calculate the feed date for the sched_id and filter (take into account feeds that have prev_date)
				my $dis_trans = "
				select top 1 DISTransactionNumber, FeedDate, Status, ExecutionDateTime
				from DataIngestionInfrastructure.dbo.MakeUpdateInfo
				with (NOLOCK)
				where BuildNumber = $build_num
				and DataFeedId = '$feed_id'
				$feed_date_filter
				order by ExecutionDateTime desc
				--order by FeedDate desc
			";
				dsay $dis_trans;
				my ( $dis_feed_date, $dis_feed_status );
				until ( ( $trans_num, $dis_feed_date, $dis_feed_status )
						= $dbh_dis->selectrow_array($dis_trans) )
				{
					say
						"\tno trans num found for DIS trans num, rewinding again";
					$sched_feed_date =
						sched_id2feed_date( $sched_id,
										  date_math( -1, $sched_feed_date ) );
					$feed_date_filter = "and FeedDate = '$sched_feed_date'";
					$dis_trans        = "
					select top 1 DISTransactionNumber, FeedDate, Status, ExecutionDateTime
					from DataIngestionInfrastructure.dbo.MakeUpdateInfo
					with (NOLOCK)
					where BuildNumber = $build_num
					and DataFeedId = '$feed_id'
					$feed_date_filter
					order by ExecutionDateTime desc";

					#					( $trans_num, $dis_feed_date, $dis_feed_status )
					#					= $dbh_dis->selectrow_array($dis_trans) or
				}
				dsay $sched_feed_date;
				# select this transaction from TQALic
				# to get AUH process time, along with filenum and filedate
				my $transactions = "
				select top 1 Status, BuildTime, FileDate, FileNum, Sender, 
					TransactionNumber, DateDiff(dd, [BuildTime], GETUTCDATE()), FeedDate, seqnum,
					filesize 
				from [TQALic].dbo.[PackageQueue] 
				with (NOLOCK)
				where TaskReference LIKE '%$feed_id%'
				and TransactionNumber = $trans_num

				--and DateDiff(dd, [BuildTime], GETUTCDATE()) < 1.1
				order by BuildTime desc
			";
				dsay $transactions;
				(  $status,  $exec_end,  $fd,         $fn,
				   $sender,  $trans_num, $build_time, $feed_date,
				   $seq_num, $filesize
				) = $dbh_prod1->selectrow_array($transactions);
					#say "fd: $fd" and exit;
#					or warn
#					"\t[2] no transaction # found for enum feed $name, $sender skipping\n$transactions\n"
#					and next;
				( $feed_year, $feed_mon, $feed_day )
					= ( $feed_date =~ m/(\d+)-(\d+)-(\d+)/ );
				my ( $dfeed_year, $dfeed_mon, $dfeed_day )
					= ( $dis_feed_date =~ m/(\d+)-(\d+)-(\d+)/ );
				dsay( $feed_year,  $feed_mon,  $feed_day );
				dsay( $dfeed_year, $dfeed_mon, $dfeed_day );

				# this is an empty update, should be marked as such
				if ( !$filesize && $status ) {

		   # TODO two sched_id inserts are happening here, need to investigate
					say "\tthis was an empty enum update";
					update_history(
								  { update_id => $update_id,
									sched_id  => $sched_id,
									trans_offset =>
										( datetime2offset($exec_end) || -1 ),
									late      => 'E',
									filedate  => 'NULL',
									filenum   => 'NULL',
									transnum  => $trans_num,
									feed_date => $feed_date,
									seq_num   => $seq_num,
								  }
					);
					next;
				}

		  # not done processing, mark as wait
		  # TODO status is binary, how is this represented in Perl out of DBI?
				elsif ( !$status ) {
					say "\tno status, AUH not finished";
					$trans_offset = -1;
				}

			  # monday is special
			  # TODO fix how weekends are handled using upd_date and feed_date
			  #				elsif ( $current_wd == 1 && $offset < 129600 ) {
			  #
			  #					say "\tfixing for monday";
			  #					$trans_offset = datetime2offset($exec_end);
			  #					my $fut_flag = int( $trans_offset / 86400 );
			  #					$trans_offset -= $fut_flag * 86400;
			  #				}

			}

			if ( defined $fd ) {
				say "\tfiledate defined: $fd";
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
				$trans_offset ||= datetime2offset($exec_end);
				dsay $trans_offset;
				my $cmp_result;
				if ( $trans_offset == -1 ) {
					say "\tforcing cmp_result";
					$cmp_result = -1;

					#$cmp_result = comp_offsets( $trans_offset, $offset );
				}
				else {
					$cmp_result = comp_offsets( $trans_offset, $offset,
										 ( $current_wd == 0 && $prev_date ) );
					dsay $cmp_result;
				}

			#say "result: $cmp_result";
			# filter earlier/later feed dates out, still waiting on them
			#				if ($feed_date =~ m/(\d+)-(\d+)-(\d+)/) {
			#					my $parsed_fd = ParseDate($feed_date);
			#					my $cur_day = ParseDate("$tyear-$tmonth-$tday");
			#					my $prev_day = Date_PrevWorkDay($cur_day, 1);
			#					#my $next_day = Date_NextWorkDay($cur_day, 1);
			#					#say "$parsed_fd, $cur_day, $prev_day";
			#					 #|| $parsed_fd eq $next_day
			#					unless ($parsed_fd eq $prev_day || $parsed_fd eq $cur_day) {
			#						$cmp_result = -1;
			#					}
			#				}

			   # if it's within an hour of the scheduled time, mark as on time
			   # could also be early
				if ( $cmp_result == 0 ) {
					say "\tontime $trans_offset offset: $offset";
					update_history( { update_id    => $update_id,
									  sched_id     => $sched_id,
									  trans_offset => $trans_offset,
									  late         => 'N',
									  filedate     => $fd,
									  filenum      => $fn,
									  transnum     => $trans_num,
									  feed_date    => $feed_date,
									  seq_num      => $seq_num,
									}
					);

					#					backdate( $backdate_updates, $trans_offset,
					#							  'N',               $fd,
					#							  $fn,               $build_num,
					#							  $sched_id,         $trans_num,
					#							  $feed_date
					#					);
				}

				# otherwise it either has not come in or it is late
				# late
				elsif ( $cmp_result == 1 ) {
					say "\tlate $trans_offset to offset: $offset";
					update_history( { update_id    => $update_id,
									  sched_id     => $sched_id,
									  trans_offset => $trans_offset,
									  late         => 'Y',
									  filedate     => $fd,
									  filenum      => $fn,
									  transnum     => $trans_num,
									  feed_date    => $feed_date,
									  seq_num      => $seq_num,
									}
					);

					#					backdate( $backdate_updates, $trans_offset,
					#							  'Y',               $fd,
					#							  $fn,               $build_num,
					#							  $sched_id,         $trans_num,
					#							  $feed_date
					#					);
				}

				# possibly just not recvd yet
				elsif ( $cmp_result == -1 ) {
					say "\twaiting/passing, last trans: $exec_end";
				}
				else {
					say "\tFAILED transaction offset sanity check: $offset";
					next;
				}
			}
			else {
				say "\tno transactions found";
				next;
			}

		}
	}
}

# poll ops schedule Excel spreadsheet for legacy feed statuses
sub refresh_legacy {
	my $opts_href = shift;

	my ( $tyear, $tmonth, $tday, $tsched_id, $pause_mode )
		= map { exists $opts_href->{$_} ? $opts_href->{$_} : undef }
		qw(year month day sched_id pause_mode);

	#my ( $tyear, $tmonth, $tday ) = @_;

	check_handles();

	# attempt to find & download the latest spreadsheet from OpsDocs server
	my $sched_xls = $tyear ? find_sched($tyear, $tmonth, $tday) : find_sched(); 
		# '\\\\10.16.40.216/dataops/Operations_Update_Summary/Checklist_2013/DailyCheckList_20130422.xls';

	my $feed_date = sprintf( '%u%02u%02u', $tyear, $tmonth, $tday );

	# create parser and parse xls
	my $xlsparser = Spreadsheet::ParseExcel->new();
	my $workbook  = $xlsparser->parse($sched_xls)
		or say "unable to parse spreadsheet: $sched_xls\n",
		$xlsparser->error()
		and return;
	say "done parsing checklist: $sched_xls";

	my $first_run = 1;

	# iterate over each weekday (worksheets)
	for my $worksheet ( $workbook->worksheets() ) {
		my $weekday = $worksheet->get_name();
		if ( $weekday =~ m/special|issue/i ) {
			dsay "skipping unsupported legacy checklist page: $weekday";
			next;
		}
		say "parsing $weekday...";
		my $weekday_code = code_weekday($weekday);

		# skip if this is an unrecognized worksheet
		say "\tunable to parse weekday, skipping" and next
			if $weekday_code == -1;

		# find the row and column bounds for iteration
		my ( $col_min, $col_max ) = $worksheet->col_range();
		my ( $row_min, $row_max ) = $worksheet->row_range();

		my $sched_block = '';
		my $blank_flag  = 0;

		# iterate over each row and store scheduling data
		for ( my $row = $row_min; $row <= $row_max; $row++ ) {

			next if $row <= 1;
			if ($blank_flag) {
				$blank_flag = 0;
			}
			else {
				$first_run = pause_mode($first_run) if $pause_mode;
			}

			# per-update hash of column values
			my $row_data = {};
			for ( my $col = $col_min; $col <= $col_max; $col++ ) {
				my $cell = $worksheet->get_cell( $row, $col );
				unless ( extract_row_daemon(
									$col, $cell, $row_data, defined $tsched_id
						 )
					)
				{
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

			# skip unless update name filled in

			unless ( exists $row_data->{update} ) {
				$blank_flag = 1;

				dsay "blank row, skip";
				next;
			}

			my $name = $row_data->{update};

			my $update_id = get_update_id($name);
			
			unless ($update_id) {
				dsay "\tcould not find update ID for $name" unless $tsched_id;
				next;
			}
			
			my $feed_id = get_feed_id($update_id);
			
			unless ($feed_id) {
				dsay "\tcould not find feed ID for $name" unless $tsched_id;
			}
			
			# TODO implement better way of handling legacy CT TZ border feeds
			# correct weekday for border cases
			my $border_flag = 0;
			my $tmp_weekday_code;
			if ($update_id == 406

				# || $update_id == 405
				|| $update_id == 403 
				|| $update_id == 407 || $update_id == 408
				)
			{
				dsay "\tfixing border weekday from $weekday_code"
					unless $tsched_id;
				$tmp_weekday_code = $weekday_code;
				$weekday_code++;
				$weekday_code = 0 if $weekday_code == 7;
				$border_flag = 1;
			}

			my $sched_query = "
				select sched_epoch, sched_id 
				from Update_Schedule us
				where update_id = $update_id
				and weekday = $weekday_code
			";
			my ( $sched_offset, $sched_id )
				= $dbh_sched->selectrow_array($sched_query);

			unless ( defined $sched_offset ) {
# TODO handle this error by finding the correct schedule entry to update rather than failing
				dsay "\toffset not defined $update_id";
				next;
			}

			if ( defined $tsched_id && $tsched_id != $sched_id ) {
				dsay 'skipping to target';
				next;
			}

			#			if ($border_flag) {
			#				$sched_offset -= 86400;
			#			}
			if ($border_flag) {
				$weekday_code = $tmp_weekday_code;
			}

			say "\t$name\t$update_id";
			my $status;
			my ( $trans_ts, $trans_offset, $trans_num, $seq_num )
				= ( 0, -1, -1, 0 );
			if ( !defined $row_data->{filedate} || !defined $row_data->{filenum}) {
				say "\t\tno UPD entry";
			}
			elsif (    $row_data->{filedate}
					&& $row_data->{filenum}
					&& $row_data->{filedate} !~ m/skip|hold/i
					&& $row_data->{filenum} !~ m/skip|hold/i )
			{
				#dsay "$row_data->{filedate} $row_data->{filenum} $feed_id";
				( $trans_ts, $trans_num, $seq_num )
					= lookup_update( $row_data->{filedate},
									 $row_data->{filenum}, 
									 $feed_id );
				#dsay "ts: $trans_ts tn: $trans_num sn: $seq_num";
				$trans_offset = $trans_ts ? datetime2offset($trans_ts) : -1;
				#dsay "to: $trans_offset";
			}

			# this was marked for skip or on hold
			elsif (    $row_data->{filedate} =~ m/skip|hold/i
					|| $row_data->{filenum} =~ m/skip|hold/i )
			{
				dsay "\tfound skip or hold";
				$status               = 'K';
				$row_data->{filedate} = 'NULL';
				$row_data->{filenum}  = 'NULL';
			}

			# compare transaction execution time to schedule offset
			# GMT now		# GMT sched
			my $cmp_result;
			if ( $trans_offset == -1 ) {
				$cmp_result = -1;
			}
			else {
				$cmp_result = comp_offsets( $trans_offset, $sched_offset );
			}

			#say $row_data->{update}, 			$row_data->{filedate};

			#my $cmp_result = comp_offsets( $trans_offset, $sched_offset );

			# adjust feed date according to weekday
			$feed_date = legacy_feed_date( $weekday_code, $feed_date );

			# if it's within an hour of the scheduled time, mark as on time
			# could also be early
			if ( $cmp_result == 0 ) {
				$status ||= 'N';

#	say "$update_id $name $trans_ts $trans_offset $sched_offset $cmp_result" if $trans_ts;
#say "ontime $name $trans_offset offset: $sched_offset";
				update_history( { update_id    => $update_id,
								  sched_id     => $sched_id,
								  trans_offset => $trans_offset,
								  late         => $status,
								  filedate     => $row_data->{filedate},
								  filenum      => $row_data->{filenum},
								  transnum     => $trans_num,
								  is_legacy    => 1,
								  feed_date    => $feed_date,
								  seq_num      => $seq_num,
								}
				);
			}

			# otherwise it either has not come in or it is late
			# late
			elsif ( $cmp_result == 1 ) {
				$status ||= 'Y';

				#say "late $name $trans_offset to offset: $sched_offset";
				update_history( { update_id    => $update_id,
								  sched_id     => $sched_id,
								  trans_offset => $trans_offset,
								  late         => $status,
								  filedate     => $row_data->{filedate},
								  filenum      => $row_data->{filenum},
								  is_legacy    => 1,
								  feed_date    => $feed_date,
								  seq_num      => $seq_num,
								}
				);
			}

			# possibly just not recvd yet
			elsif ( $cmp_result == -1 ) {

				#say "waiting on $name, last trans: $trans_offset";
				#say "late $name $trans_offset to offset: $sched_offset";
				next if !$status;
				update_history( { update_id    => $update_id,
								  sched_id     => $sched_id,
								  trans_offset => -1,
								  late         => $status,
								  filedate     => 'NULL',
								  filenum      => 'NULL',
								  is_legacy    => 1,
								  feed_date    => $feed_date,
								}
				);
			}
			else {
				say
					"\tFAILED transaction offset sanity check: $name $sched_offset\n";
				next;
			}
		}
	}
}

# insert a user input pause with options for stepping through loops
sub pause_mode {
	my ($first_run) = @_;

	# pause mode after first run
	if ( !$first_run ) {

		my $user_input = '';
		say '[PAUSE]';

		# Term::ReadKey to avoid having to hit return
		# raw mode
		ReadMode 4;
		while ( not defined( $user_input = ReadKey(-1) ) ) { }

		# original mode
		ReadMode 0;
		if ( $user_input =~ m/Q/i ) {
			say 'quitting by user request';
			exit;
		}
	}
	else {
		say 'pause mode enabled, press any key to step through updates';
		say 'see docs for special commands in pause mode';
	}
	return 0;
}

# calculate a particular DoW's feed date
sub legacy_feed_date {
	my ( $excel_wd, $curr_date ) = @_;
	my $curr_wd = get_wd($curr_date);

	#say "curr wd: $curr_wd excel wd: $excel_wd";
	my $delta = 0;

	# this is today (supposedly)
	if ( $curr_wd == $excel_wd ) {

		#say "same day";
		return $curr_date;
	}

	# sunday is actually at the 'end' of the week
	elsif ( $curr_wd == 0 ) {
		$delta = -7 + $excel_wd;
	}
	elsif ( $excel_wd == 0 ) {
		$delta = $curr_wd - 7;
	}
	else {
		$delta = $excel_wd - $curr_wd;
	}

	# looking at past day in sheet
	#	elsif ( $curr_wd > $excel_wd ) {
	#		$delta = $excel_wd - $curr_wd;
	#	}

	#	# looking at future day in sheet
	#	elsif ( $curr_wd < $excel_wd ) {
	#		$delta = $curr_wd - $excel_wd;
	#	}

	#say "delta: $delta";

	return date_math( $delta, $curr_date );
}

# do date math in day increments
sub date_math {
	my ( $delta_days, $date ) = @_;
	dsay @_;
	my ( $year, $month, $day ) = ( $date =~ m!(\d{4}).?(\d{2}).?(\d{2})! )
		or ( say "could not do date math!\n" and return );
	my $time = timegm( 0, 0, 0, $day, $month - 1, $year - 1900 );
	$time += $delta_days * 86400;
	my ( $sec, $min, $hour, $mday, $mon, $y, $wday, $yday, $isdst )
		= gmtime($time);
	return sprintf '%u-%02u-%02u', $y + 1900, $mon + 1, $mday;
}

# look up an update's completion timestamp from AUH db
sub lookup_update {
	my ( $filedate, $filenum, $feed_id ) = @_;
	$feed_id = $feed_id ? "and taskreference like '%$feed_id%'" : ''; 
	my $select_fdfn_query = "
		select ProcessTime, TransactionNumber, seqnum
		from [TQALic].[dbo].[PackageQueue]
		where FileDate = '$filedate'
		and FileNum= '$filenum'
		$feed_id
	";
	my ( $fdfn_ts, $trans_num, $seq_num )
		= ( $dbh_prod1->selectrow_array($select_fdfn_query) );

	return ( $fdfn_ts, $trans_num, $seq_num ) if defined $fdfn_ts;
	return;

}

# works backwards from a schedule ID to a feed date
# needs to go back to either prod1 or dis box to rewind to the associated feed date
# TODO sched_id does not map to feed_date properly - not sure if this method is save-able
sub sched_id2feed_date {
	my ( $sched_id, $feed_date_current, $date_shift ) = @_;
	$date_shift ||= 0;
	# lookup weekday from schedule
	my $date_lookup_query = "
		select a.update_id, weekday, prev_date, is_legacy
		from Update_Schedule a
		join 
		updates b
		on a.update_id = b.update_id
		where 
		sched_id = $sched_id		
	";
	dsay $date_lookup_query, $feed_date_current;
	my ( $update_id, $wd, $prev_date, $is_legacy )
		= $dbh_sched->selectrow_array($date_lookup_query)
		or dsay "sched_id $sched_id lookup failed" and return -1;

	my ( $found, $rewinds );

	# handle weekend case DIS, needs to fast forward to Monday's date
	if ( $wd == 0 && defined $prev_date && $prev_date == 1 && !$is_legacy ) {
		$found = 1;
		return date_math( 1, $feed_date_current );
	}

	# prev day, need to go back to friday (usually)
	# not for legacy
	elsif ( ( defined $prev_date && $prev_date == 1 ) || $is_legacy ) {
		dsay "rewinding";
		until ( defined $found && $found > 0 ) {

			# rollback to saturday properly
			$wd = 6 if --$wd == -1;
			$rewinds++;
			my $weekday_sched_query = "
			select sched_id
			from Update_Schedule
			where 
			weekday = $wd
			and update_id = $update_id
		";
			dsay $weekday_sched_query;
			my ($sched_id)
				= $dbh_sched->selectrow_array($weekday_sched_query);

			# stop rewinding for dis prev dates or legacy non-prev dates
			# legacy prev dates require 1 more rewind
			if ( ( defined $sched_id && !( $is_legacy && $prev_date ) ) )

				# || ( !$is_legacy && $prev_date && !defined $sched_id ) )
			{
				$found++;
				# extra rewind for RKD early feeds
				if ( $update_id == 432 || $update_id == 433) {
					if ($found == 0) {
						$found = 1;
					}
					else {
						$found = -1;	
					}
				}
			}

			# legacy prev date is defined, but 0 for 1 more rewind run
			elsif (    defined $sched_id
					&& $is_legacy
					&& $prev_date
					&& !defined $found )
			{
				$found = 0;
			}

			# did the extra rewind for legacy w/ prev date
			elsif (    defined $sched_id
					&& $is_legacy
					&& $prev_date
					&& defined $found
					&& $found == 0 )
			{
				$found = 1;
			}
		}

		# rewind number of days from current feed date
		my $rewinded_feed_date = date_math( -$rewinds, $feed_date_current );
		dsay "rewinded to:" . $rewinded_feed_date;
		return $rewinded_feed_date;
	}
	else {
		return date_math($date_shift,$feed_date_current);
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

	   # don't put anything to STDOUT if this is the report, screws with proto
	   #say $entry{msg} unless (caller)[1] =~ m/${\$cfg->hosted_script}/i;
		}
		when (m'WARN') {
			return unless $cfg->enable_warn;
			carp $entry{msg};
		}
		when (m'ERROR') {
			carp $entry{msg};
		}

		# warn about unusual entry types, but still log them
		default {
			carp "unrecognized log entry type: $entry{type}\n";
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

# send e-mail notification
# returns truth upon success
sub send_email {
	my $opts_href = shift;

	my $smtp = Net::SMTP->new( $opts_href->{smtp_server} )
		or warn
		"failed to connect to SMTP server: ${\$opts_href->{smtp_server}}\n"
		and return;

	$smtp->mail( $opts_href->{user} || $ENV{USER} )
		or warn "server auth failed\n" and return;

	my @add_list = @{ $opts_href->{send_to} };

	foreach my $add (@add_list) {
		$smtp->to($add) or warn "failed to add recip: $add\n";
	}

	$smtp->data();

	foreach my $add (@add_list) {
		$smtp->datasend("To: $add\n") or warn "failed to add recip: $add\n";
	}

	$smtp->datasend("From: ${\$opts_href->{sender}}\n")
		or warn "could not use sender: ${\$opts_href->{sender}}\n" and return;
	$smtp->datasend("Subject: ${\$opts_href->{msg_subject}}\n")
		or warn "could not write subject: ${\$opts_href->{msg_subject}}\n"
		and return;
	$smtp->datasend("\n");
	$smtp->datasend( ref( $opts_href->{msg_body} ) eq 'SCALAR'
					 ? ${ \$opts_href->{msg_body} }
					 : $opts_href->{msg_body}
		)
		or warn "could not write body: ${\$opts_href->{msg_body}}\n"
		and return;

	$smtp->dataend();

	$smtp->quit;

	return 1;
}

# STDERR redirects to file if being run from the module
sub redirect_stderr {
	use IO::Handle;
	my ($error_log) = (@_);
	open my $err_fh, '>>', $error_log;
	STDERR->fdopen( $err_fh, 'a' )
		or warn "failed to pipe errors to logfile:$!\n";
}

sub usage {
	my ($exit_val) = @_;

	pod2usage( { -verbose => $cfg->verbosity,
				 -input   => $cfg->pod,
				 -exit    => $exit_val || 0
			   }
	);
}

#################################################
# the following subs are not yet called anywhere
# because of paranoia, will bring in later
#################################################

# drop the database
sub drop_db {
	return $dbh_sched->do('drop database TQASched')
		or die "could not drop TQASched database\n", $dbh_sched->errstr;
}

# clear all update records in database
sub clear_updates {
	return $dbh_sched->do('delete from [Updates]')
		or die "error in clearing Updates table\n", $dbh_sched->errstr;
}

# clear all scheduling records in database
sub clear_schedule {
	return $dbh_sched->do('delete from [Update_Schedule]')
		or die "error in clearing Schedule table\n", $dbh_sched->errstr;
}
