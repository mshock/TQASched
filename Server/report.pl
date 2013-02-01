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
my ($dbh_sched) = refresh_handles();

# all possible POST parameters
my ( $headerdate, $headertime, $dbdate ) = calc_datetime();

# get POST params
# parsed from CLI arg key/value pairs
my $post_date = $cfg->date;

print "HTTP/1.0 200 OK\r\n";
print "Content-type: text/html\n\n";

# if date passed through POST, update header variables
unless ( $post_date =~ m/(\d{4})(\d{2})(\d{2})/ ) {
	write_log( { logfile => $cfg->report_log,
				 type    => 'WARN',
				 msg     => "bad POST value: $post_date\n"
			   }
	);
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
		SchedTime
		RecvdTime
		Update
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
				<th colspan='2'><a href='?date=$prevdate'><<</a> previous ($prevdate)</th>
				<th colspan='3'>($nextdate) next <a href='?date=$nextdate'>>></a></th>
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

	my $select_schedule = "
	  select us.sched_id, us.update_id, us.sched_epoch, u.name
	  from [TQASched].[dbo].[Update_Schedule] us,
	  [TQASched].[dbo].[Updates] u
	  where weekday = '$wd'
      and u.update_id = us.update_id
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
		my ( $sched_id, $update_id, $sched_offset, $name ) = @{$row_aref};
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

	my $sched_time = offset2time($sched_offset) . ' CST';

	# if there is a history record, it can be ontime or late
	my ( $status, $daemon_ts, $recvd_time, $update );
	if ($hist_id) {
		$status     = $late eq 'N' ? 'recv' : 'late';
		$recvd_time = offset2time($hist_offset) . ' GMT';
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
	my @weekdays = qw(0 1 2 3 4 5 6);
	my ( $month, $day, $year ) = ( $headerdate =~ m!(\d+)/(\d+)/(\d+)! );
	my $time = timegm( 0, 0, 0, $day, $month - 1, $year - 1900 );
	my ( $sec, $min, $hour, $mday, $mon, $y, $wday, $yday, $isdst )
		= gmtime($time);
	return $weekdays[$wday];
}

sub offset2time {
	my $offset = shift;
	my $day_offset = get_wd() * 86400;
	$offset -= $day_offset;
	my $hours   = int( $offset / 60 );
	my $minutes = $offset - $hours * 60;
	return sprintf '%02u:%02u', $hours, $minutes;
}
