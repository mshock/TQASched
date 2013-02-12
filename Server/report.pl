#! perl -w

# called by the http server
# generates HTML response containing report

use strict;
use feature qw(say switch);
use Time::Local qw(timegm);

use lib '..';
use TQASched;

my $cfg     = load_conf('..');
my $refresh = $cfg->refresh;

# redirect that STDERR if it's not going to the term
redirect_stderr( $cfg->report_log ) if caller;

# we only need the handle for TQASched db in the report! that's all, folks!
my ($dbh_sched) = refresh_handles( ('sched') );

# all possible POST parameters
my ( $headerdate, $headertime, $dbdate ) = calc_datetime();

# get POST params
# parsed from CLI arg key/value pairs
my $post_date     = $cfg->date;
my $legacy_filter = $cfg->legacy =~ REGEX_TRUE ? 'checked' : '';
my $dis_filter    = $cfg->dis =~ REGEX_TRUE ? 'checked' : '';
if ( $legacy_filter && $dis_filter ) {
	( $legacy_filter, $dis_filter ) = ( '', '' );
}

######################################################
#	NOTICE POSTED:
#		anything to STDOUT before this point will
#		botch HTTP communication protocol
######################################################

print "HTTP/1.0 200 OK\r\n";
print "Content-type: text/html\n\n";

######################################################
#	STDOUT = html content
#	anything else will be printed to the page
#	(useful for debug)
######################################################

# if date passed through POST, update header variables
unless ( $post_date =~ m/(\d{4})(\d{2})(\d{2})/ ) {
	if ( defined $post_date ) {
		write_log( { logfile => $cfg->report_log,
					 type    => 'INFO',
					 msg     => "bad POST value: $post_date\n"
				   }
		);
	}
}
else {
	$headerdate = "$2/$3/$1";

	# no time when rewinding, obviously
	$headertime = '';
	$dbdate     = $post_date;
}

print_header();

print_thead(
	qw(
		Feed
		Feed_ID
		Scheduled_Time
		Received_Time
		Update(UPD)
		Timestamp
		)
);

print_table();

print_footer();

# write execution time to log when done generating content (resolution: seconds, unfortunately)
write_log(
		{ logfile => $cfg->report_log, type => 'INFO', msg => exec_time() } );

######################################################
#	Subs
#
######################################################

sub print_header {

	# enable report refresh for times over 15 seconds, no faster
	# a value of less than 15 is treated as no refresh
	my $header_refresh
		= $refresh >= 15
		? "<meta http-equiv='refresh' content='$refresh' >"
		: '<!-- auto refresh not enabled ($refresh) -->';

	say "
<html>
	<head>
	$header_refresh
	<title>Monitor :: TQASched</title>
	<link rel='stylesheet' type='text/css' href='styles.css' />
	</head>
	<body>
";
}

sub print_thead {
	my @headers     = @_;
	my $num_headers = scalar @headers;
	my ( $prevdate, $nextdate ) = calc_adjacent();

	say "
<form method='GET'>
	<table cellspacing='0' width='100%' border=0>
		<thead>
			<tr>
				<th colspan='$num_headers' >
					<h2>Market Date $headerdate&nbsp&nbsp&nbsp|&nbsp&nbsp&nbsp$headertime </h2>
				</th>
			</tr>
			<tr>
				<th colspan='$num_headers'>
					<input type='checkbox' name='filter_legacy' value='true' onclick='this.form.submit();' $legacy_filter/> Only Legacy
					<input type='checkbox' name='filter_dis' value='true' onclick='this.form.submit();' $dis_filter/> Only DIS
				</th>
			</tr>
			<tr>
				<th colspan='2'><a href='?date=$prevdate'><<</a> previous ($prevdate)</th>
				<th colspan='4'>($nextdate) next <a href='?date=$nextdate'>>></a></th>
			</tr>
			<tr>";
	for my $header (@headers) {
		say "
			<th>
				$header
			</th>";
	}
	say "
			<tr>
		</thead>";
}

# returns the UPD YMD format for previous and next day navigation
sub calc_adjacent {

	# really, really don't want to pull Date::Manip into this script
	my ( $month, $day, $year ) = $headerdate =~ m!(\d+)/(\d+)/(\d+)!;
	my $time = timegm( 0, 0, 0, $day, $month - 1, $year - 1900 );

	my @y = gmtime( $time - 86400 );
	my @t = gmtime( $time + 86400 );

	# skip weekends, there are no new UPDs on weekends
	# if yesterday was sunday, skip back to last friday
	#if ($y[6] == 0) {
	#	@y = gmtime($time - 259200);
	#}

	# if tomorrow is saturday, skip forward to next monday
	#elsif ($t[6] == 6) {
	#	@t = gmtime($time + 259200);
	#}

	return (

		# yesterday
		sprintf( "%u%02u%02u", $y[5] + 1900, $y[4] + 1, $y[3] ),

		# tomorrow
		sprintf( "%u%02u%02u", $t[5] + 1900, $t[4] + 1, $t[3] )
	);
}

sub print_table {

	say "<tbody>";

	# get current weekday
	my $wd = get_wd();

	my $filter = '';
	if ($legacy_filter) {
		$filter = 'and u.is_legacy = 1';
	}
	elsif ($dis_filter) {
		$filter = 'and u.is_legacy = 0';
	}

	my $select_schedule = "
	  select distinct us.sched_id, us.update_id, us.sched_epoch, u.name, u.is_legacy, d.feed_id
	  from [TQASched].[dbo].[Update_Schedule] us,
	  [TQASched].[dbo].[Updates] u
	  left join
	  [TQASched].[dbo].[Update_DIS] d
	  on d.update_id = u.update_id
	  where weekday = $wd
      and u.update_id = us.update_id
      $filter
      order by sched_epoch, name asc
	";

	my $select_history = "
		select hist_id, hist_epoch, filedate, filenum, timestamp, late
		from [TQASched].[dbo].[Update_History]
		where
		sched_id = ?
		and cast( floor( cast([timestamp] as float) ) as datetime) = '$headerdate'
	";

	#say 'executing sched query';
	my $sched_aref = $dbh_sched->selectall_arrayref($select_schedule);

	#say 'preparing history query';
	my $hist_query = $dbh_sched->prepare($select_history);

	#say 'iterating...';
	my $row_count = 0;
	for my $row_aref ( @{$sched_aref} ) {
		$row_count++;
		my ( $sched_id, $update_id, $sched_offset, $name, $is_legacy,
			 $feed_id )
			= @{$row_aref};
		$hist_query->execute($sched_id);
		my ( $hist_id, $hist_offset, $filedate, $filenum, $hist_ts, $late )
			= $hist_query->fetchrow_array();

		#say "fetched row for $sched_id";
		# this has been seen for today, has history record

		my ( $row_class,  $status,    $sched_time,
			 $recvd_time, $daemon_ts, $update )
			= row_info( $row_count, $late, $hist_id, $sched_offset,
						$hist_offset, $hist_ts, $filedate, $filenum );

		#say "found result: $hist_id";
		say "
		<tr class='$row_class'>
			<td>$name</td>
			<td>$feed_id</td>
			<td>$sched_time</td>
			<td>$recvd_time</td>
			<td>$update</td>
			<td>$daemon_ts</td>
		</tr>";
	}

}

# assign a style to row based on count
sub row_info {
	my ( $row_count, $late, $hist_id, $sched_offset, $hist_offset, $hist_ts,
		 $filedate, $filenum )
		= @_;
	my $row_parity = $row_count % 2;

	my $sched_time = offset2time($sched_offset);

	# if there is a history record, it can be ontime or late
	my ( $status, $daemon_ts, $recvd_time, $update );
	if ($hist_id) {
		$status     = $late eq 'N' ? 'recv' : 'late';
		$recvd_time = offset2time($hist_offset);
		$daemon_ts  = $hist_ts;
		$update     = "$filedate-$filenum";
	}

	# no history record, still waiting
	else {
		$recvd_time = 'N/A';
		$status     = 'wait';
		$daemon_ts  = 'N/A';
		$update     = 'N/A';
	}

	my $row_class = $status . ( $row_parity ? '_even' : '_odd' );

	return ( $row_class, $status, $sched_time, $recvd_time, $daemon_ts,
			 $update );
}

sub print_footer {
	say '
		</tbody>
		</table>
	</form>
</body>
</html>';
}

# calculate and format today's date and time GMT
sub calc_datetime {
	my ( $sec, $min, $hour, $mday, $mon, $year, $wday, $yday, $isdst )
		= gmtime(time);
	return ( sprintf( "%02u/%02u/%u",   $mon + 1, $mday, $year + 1900 ),
			 sprintf( "%02u:%02u:%02u", $hour,    $min,  $sec ),
			 sprintf( "%u%02u%02u", $year + 1900, $mon + 1, $mday )
	);
}

# returns code for passed date
sub get_wd {
	my ( $month, $day, $year ) = ( $headerdate =~ m!(\d+)/(\d+)/(\d+)! );
	my $time = timegm( 0, 0, 0, $day, $month - 1, $year - 1900 );
	my ( $sec, $min, $hour, $mday, $mon, $y, $wday, $yday, $isdst )
		= gmtime($time);
	return $wday;
}

sub offset2time {
	my $offset = shift;

	# first drop the day portion of the offset
	my $wd         = get_wd();
	my $day_offset = $wd * 86400;
	$offset -= $day_offset;

	# this is for border cases - happened next day
	my $fut_flag = 0;
	if ( $offset > 86400 ) {
		$fut_flag = int( $offset / 86400 );
		$offset %= 86400;

	}

	# then extract hours and minutes for human readable
	my $hours = int( $offset / 3600 );
	$offset -= $hours * 3600;
	my $minutes = int( $offset / 60 );

	# these are the seconds remaining, if anyone cares
	$offset -= $minutes * 60;

	# this is for border cases - happened previous day
	# TODO: what about dates further in the past, weekends especially?
	# keep rewinding until just the time is left to determine actual day
	my $past_flag = 0;

# if offset is negative at this point, then it happened before the scheduled/current day
	if ( $offset < 0 ) {
		$past_flag++;

		# rewind into previous day
		my $into_previous = 86400 + $offset;
		$hours = int( $into_previous / 3600 );
		$into_previous -= $hours * 3600;
		$minutes = int( $into_previous / 60 );
	}

	# TODO: change this to display actual date
	my $post_script = '';
	if ($past_flag) {
		$post_script = ' prev day';
	}
	elsif ($fut_flag) {
		$post_script = " $fut_flag future?";
	}

	return sprintf( '%02u:%02u GMT%s', $hours, $minutes, $post_script );
}
