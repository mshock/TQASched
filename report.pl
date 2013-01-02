#! perl -w

# called by the http server
# generates HTML response containing report

use strict;
use feature qw(say switch);
use DBI;
use Time::Local qw(timegm);
use Getopt::Long qw(GetOptions);
use Config::Simple;

my $refresh = 0;

my $cfg       = new Config::Simple('tqa_sched.conf');
my $sched_db  = $cfg->param( -block => 'sched_db' );
my $dbh_sched = init_handle($sched_db);

# all possible POST parameters
my ( $headerdate, $headertime ) = calc_datetime();

# get POST params
# parsed from CLI arg key/value pairs
GetOptions( 'date=s' => \$headerdate );

print "HTTP/1.0 200 OK\r\n";
print "Content-type: text/html\n\n";

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

######################################################
#	Subs
#
######################################################

sub print_header {
	my $header_refresh
		= $refresh ? "<meta http-equiv='refresh' content='300' >" : '<!-- auto refresh not enabled -->';

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
	say "
<form method='GET'>
	<table cellspacing='0' width='100%' border=0>
		<thead>
			<tr>
				<th colspan='$num_headers' >
					<h2>Market Date $headerdate&nbsp&nbsp&nbsp|&nbsp&nbsp&nbsp$headertime </h2>
				</th>
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

sub print_table {

	say "<tbody>";

	# get current weekday
	my $wd = get_wd();

	my $select_schedule = "
	  select us.sched_id, us.update_id, us.time, u.name
	  from [TQASched].[dbo].[Update_Schedule] us,
	  [TQASched].[dbo].[Updates] u
	  where weekday = '$wd'
      and u.update_id = us.update_id
	";

	my $select_history = "
		select hist_id, time, filedate, filenum, timestamp, late
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
	return ( sprintf( "%02u/%02u/%u", $mon + 1, $mday, $year + 1900 ),
			 sprintf( "%02u:%02u:%02u", $hour, $min, $sec ) );
}

# returns code for passed date
sub get_wd {
	my @weekdays = qw(N M T W R F S);
	my ( $month, $day, $year ) = ( $headerdate =~ m!(\d+)/(\d+)/(\d+)! );
	my $time = timegm( 0, 0, 0, $day, $month - 1, $year - 1900 );
	my ( $sec, $min, $hour, $mday, $mon, $y, $wday, $yday, $isdst )
		= gmtime($time);
	return $weekdays[$wday];
}

sub offset2time {
	my $offset = shift;

	my $hours   = int( $offset / 60 );
	my $minutes = $offset - $hours * 60;
	return sprintf '%02u:%02u', $hours, $minutes;
}

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
