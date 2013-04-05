#! perl -w

# called by the http server
# generates HTML response containing report

use strict;
use feature qw(say switch);
use Time::Local qw(timegm);
use Data::Dumper;
use lib '..';
use TQASched;

my $cfg     = load_conf('..');
my $refresh = $cfg->refresh;

# redirect that STDERR if it's not going to the term
#redirect_stderr( $cfg->report_log ) if caller;

# we only need the handle for TQASched db in the report! that's all, folks!
my ($dbh_sched) = refresh_handles( ('sched') );

# all possible POST parameters
my ( $headerdate, $headertime, $dbdate ) = calc_datetime();

# get POST params
# parsed from CLI arg key/value pairs
my ( $post_date,   $legacy_filter, $dis_filter,   $prev_search,
	 $search_type, $upd_checked,   $float_status, $report_title
) = ('') x 8;

my $upd_num = 0;

$float_status  = $cfg->float_status =~ REGEX_TRUE ? 'checked' : '';
$post_date     = $cfg->date;
$legacy_filter = $cfg->legacy =~ REGEX_TRUE ? 'checked' : '';
$dis_filter    = $cfg->dis =~ REGEX_TRUE ? 'checked' : '';
$prev_search   = $cfg->search;
$search_type   = $cfg->search_type;
$upd_checked   = $cfg->search_upd =~ REGEX_TRUE ? 'checked' : '';
$report_title  = $cfg->title || 'Monitor :: TQASched';

my $debug_mode = $cfg->debug;

if ( $legacy_filter && $dis_filter ) {
	( $legacy_filter, $dis_filter ) = ( '', '' );
}

# TODO use switch or hash
my ( $id_selected, $feed_selected, $time_selected, $feed_date_selected )
	= ('') x 4;
if ( uc $search_type eq 'FEED' ) {
	$feed_selected = 'selected';
}
elsif ( uc $search_type eq 'FEED_ID' ) {
	$id_selected = 'selected';
}
elsif ( uc $search_type eq 'SCHEDULE_TIME' ) {
	$time_selected = 'selected';
}
elsif ( uc $search_type eq 'FEED_DATE' ) {
	$feed_date_selected = 'selected';
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

	# header time shows current when in a different date

	$headertime = "[$headerdate $headertime]";
	$headerdate = "$2/$3/$1";

	( $dbdate, $upd_num ) = ( $post_date =~ m/(\d+)(?:-(\d+))?/ );
}

print_header();

print_thead(
	qw(
		Feed
		Feed_ID
		Scheduled_Time
		Received_Time
		Feed_Date
		Update(UPD)
		)
);

compile_table();

#print_table($rows_ref);

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
	<title>$report_title</title>
	<link rel='stylesheet' type='text/css' href='styles.css' />
	<script language=\"javascript\" type=\"text/javascript\">
		<!--
		function popitup(url) {
			newwindow=window.open(url,'name','height=200,width=150');
			if (window.focus) {newwindow.focus()}
			return false;
		}
		
		// -->
	</script>
	</head>
	<body>
";
}

sub print_thead {
	my @headers = @_;
	if ($debug_mode) {
		push @headers, 'Timestamp';
	}
	my $num_headers = scalar @headers;
	my ( $prevdate, $nextdate ) = calc_adjacent();

	say "
<form method='GET'>
	
	<table cellspacing='0' width='100%' border=0>
		<thead>
			<tr>
				<th colspan='$num_headers' >
					<h2>GMT Date $headerdate&nbsp&nbsp&nbsp|&nbsp&nbsp&nbsp$headertime </h2>
				</th>
			</tr>
			<tr>
				<th colspan='$num_headers'>
					<input type='checkbox' name='search_upd' value='true' title='search by UPD filedate[-filenum]' id='upd_search' $upd_checked/>
						<label for='upd_search'>UPD</label>
					<input type='text' name='date' value='$dbdate' />
					<input type='text' name='search' value='$prev_search' title='search' id='search_box'/>
						<label for='search_box'>search by</label>
				<select name='search_type'>
					<option value='Feed'  $feed_selected>Feed Name</option>
					<option value='Feed_ID'  $id_selected>Feed ID</option>
					<option value='Schedule_Time'  $time_selected>Schedule Time</option>
					<option value='Feed_Date'  $feed_date_selected>Feed Date</option>
				</select>
				<input type='submit' value='search' />
				</th>
			</tr>
			
			<tr>
				<th colspan='$num_headers'>
					<input type='radio' id='legacy_cb' name='filter_legacy' value='true' onclick='this.form.submit();' $legacy_filter/>
						<label for='legacy_cb'>Only Legacy</label>
					<input type='radio' id='dis_cb' name='filter_dis' value='true' onclick='this.form.submit();' $dis_filter/>
						<label for='dis_cb'>Only DIS</label>	
					<input type='button' value='reset' onclick='parent.location=\"/\"' />
				</th>
			</tr>
			<tr>
				<th colspan='$num_headers'>
					<input type='checkbox' id='float_stat_cb' name='float_status' value='true' onclick='this.form.submit();' $float_status />
						<label for='float_stat_cb'>Float Status</label>
				</th>
			</tr>
			<tr>
				<th colspan='2'><a href='?date=$prevdate'><<</a> previous ($prevdate)</th>
				<th colspan='5'>($nextdate) next <a href='?date=$nextdate'>>></a></th>
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

sub compile_table {

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

	if ($prev_search) {
		if ( $search_type eq 'Feed' ) {
			$filter
				= $filter . " and UPPER(u.name) like UPPER('%$prev_search%')";
		}
		elsif ( $search_type eq 'Feed_ID' ) {
			$filter = $filter
				. " and UPPER(d.feed_id) like UPPER('%$prev_search%')";
		}
	}

	my $select_schedule = "
	  select distinct us.sched_id, us.update_id, us.sched_epoch, u.name, u.is_legacy, d.feed_id, u.prev_date
	  from [TQASched].[dbo].[Update_Schedule] us
	  join
	  [TQASched].[dbo].[Updates] u
	  on u.update_id = us.update_id and weekday = $wd
	  left join
	  [TQASched].[dbo].[Update_DIS] d
	  on d.update_id = u.update_id 
	  where 
	  --weekday = $wd
      --and u.update_id = us.update_id
      d.feed_id NOT LIKE 'FIEJV%'
      and d.feed_id NOT LIKE 'RDC%'
      $filter
      order by sched_epoch, name asc
	";

	$filter = '';
	if ($upd_checked) {
		$filter = "and filedate = $dbdate";
		if ($upd_num) {
			$filter = $filter . " and filenum = $upd_num";
		}
	}
	my $select_history = "
		select top 1 hist_id, hist_epoch, filedate, filenum, timestamp, late, feed_date
		from [TQASched].[dbo].[Update_History]
		where
		sched_id = ?
		$filter
		--and cast( floor( cast([timestamp] as float) ) as datetime) = '$headerdate'
		order by feed_date desc
	";

	#warn $select_history and die;
	#say 'executing sched query';
	my $sched_aref = $dbh_sched->selectall_arrayref($select_schedule);

	#say 'preparing history query';
	my $hist_query = $dbh_sched->prepare($select_history);

	#say 'iterating...';
	my $row_count = 0;
	my %display_rows;
	for my $row_aref ( @{$sched_aref} ) {
		$row_count++;
		my ( $sched_id, $update_id, $sched_offset, $name, $is_legacy,
			 $feed_id, $prev_date )
			= @{$row_aref};
		$hist_query->execute($sched_id);
		my ( $hist_id, $hist_offset, $filedate, $filenum, $hist_ts, $late,
			 $feed_date )
			= $hist_query->fetchrow_array();

		#say "fetched row for $sched_id";
		# this has been seen for today, has history record

		my ( $row_class, $status, $sched_time, $recvd_time, $daemon_ts,
			 $update, $feed_date_pretty )
			= row_info( $row_count,    $late,        $hist_id,
						$sched_offset, $hist_offset, $hist_ts,
						$filedate,     $filenum,     $feed_date,
						$prev_date
			);

		my $update_row = sprintf( "
		<tr class='$row_class'>
			<td>%s\t%s</td>
			<td>%s</td>
			<td>%s</td>
			<td>%s</td>
			<td>%s</td>
			<td>%s</td>
			%s
		</tr>
		",
			$name, ( $debug_mode ? "[$update_id]" : '' ), $feed_id,
			$sched_time, $recvd_time, $feed_date_pretty, $update,
			( $debug_mode ? "<td>$daemon_ts</td>" : '' ) );

		#say "inserting $status, $sched_id";
		if ($float_status) {
			push @{ $display_rows{$status} }, $update_row;
		}
		else {
			say $update_row;
		}
	}
	return unless $float_status;

	my @stati = qw(late wait recv);
	for my $status_key (@stati) {
		$row_count = 0;
		for my $line ( @{ $display_rows{$status_key} } ) {
			my $row_parity = $row_count % 2;
			if ($row_parity) {
				$line =~ s/_even/_odd/;
			}
			else {
				$line =~ s/_odd/_even/;
			}
			say $line;
			$row_count++;
		}

	}
}

# assign a style to row based on count
sub row_info {
	my ( $row_count,   $late,    $hist_id,  $sched_offset,
		 $hist_offset, $hist_ts, $filedate, $filenum,
		 $feed_date,   $prev_date
	) = @_;

	$feed_date ||= 'N/A';
	$feed_date =~ s/\s.*//;

	my $row_parity = $row_count % 2;

	my $sched_time = offset2time( $sched_offset, 1 );

	# if there is a history record, it can be ontime or late
	my ( $status, $daemon_ts, $recvd_time, $update );
	if ( $hist_id && !( date_math(-7) eq $feed_date ) ) {
		if ( $late eq 'N' || $late eq 'E' ) {
			if (    !defined $prev_date
				 && $late eq 'E'
				 && date_math(-1) eq $feed_date
				 && $sched_offset % 86400 < 10800 )
			{
				$status = 'wait';
			}
			else {
				$status = 'recv';
			}
		}
		else {
			$status = 'late';
		}

		#$status     = $late eq 'N' ? 'recv'                    : 'late';
		$recvd_time = $filedate ? offset2time($hist_offset) : 'N/A';
		$daemon_ts  = $hist_ts;
		$update     = $filedate ? "$filedate-$filenum" : 'N/A';
	}

	# no history record, still waiting
	else {
		$recvd_time = 'N/A';
		$status     = 'wait';
		$daemon_ts  = 'N/A';
		$update     = 'N/A';
		$feed_date  = 'N/A';
	}

	my $row_class = $status . ( $row_parity ? '_even' : '_odd' );

	#	if ($status eq 'wait' && ) {
	#
	#	}

	return ( $row_class, $status, $sched_time, $recvd_time,
			 $daemon_ts, $update, $feed_date );
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
	my ($date) = @_;
	$date ||= $headerdate;
	my ( $month, $day, $year ) = ( $date =~ m!(\d+)/(\d+)/(\d+)! );
	my $time = timegm( 0, 0, 0, $day, $month - 1, $year - 1900 );
	my ( $sec, $min, $hour, $mday, $mon, $y, $wday, $yday, $isdst )
		= gmtime($time);
	return $wday;
}

sub offset2time {
	my ( $offset, $sched_flag ) = @_;
	my $orig_offset = $offset;

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
	if ( $offset <= -1 ) {
		my $into_previous = $orig_offset;
		$past_flag++;

		# rewind into previous day
		#my $into_previous = 86400 + $offset;
		$hours = int( $into_previous / 3600 );

		$into_previous -= $hours * 3600;

		# if there are still extra hours due to bad offset conversion, correct
		$hours %= 24;
		$minutes = int( $into_previous / 60 );

		#	$past_flag = 0 if $offset == -1;
	}

	# TODO: change this to display actual date
	my $date_display = '';
	if ( $past_flag && !$sched_flag ) {
		$date_display = date_math(-1) || 'prev day';

	}
	elsif ($sched_flag) {
		$date_display = date_math(0);
	}
	elsif ($fut_flag) {
		$date_display = date_math( -$fut_flag + 1 )
			|| "$fut_flag future?";
	}
	else {
		my $math = $offset ? 0 : -1;
		$date_display = date_math($math);
	}

	# do a sanity check on hours/minutes
	if ( $hours < 0 || $minutes < 0 ) {
		$offset = $orig_offset;
		$offset %= 86400;
		$hours = int( $offset / 3600 );
		$offset -= $hours * 3600;
		$minutes = int( $offset / 60 );
	}

	return sprintf( '%s %02u:%02u', $date_display, $hours, $minutes );
}

# do date math in days on current view's date
sub date_math {
	my ($delta_days) = @_;
	my ( $month, $day, $year ) = ( $headerdate =~ m!(\d+)/(\d+)/(\d+)! )
		or ( return and warn "could not do date math!\n" );
	my $time = timegm( 0, 0, 0, $day, $month - 1, $year - 1900 );
	$time += $delta_days * 86400;
	my ( $sec, $min, $hour, $mday, $mon, $y, $wday, $yday, $isdst )
		= gmtime($time);
	return sprintf '%u-%02u-%02u', $y + 1900, $mon + 1, $mday;
}
