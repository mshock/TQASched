#! perl -w

# called by the http server
# generates HTML response containing report

# TODO use Catalyst for MVC
# getting tired of rolling my own for each app

use strict;
use feature qw(say switch);
use Time::Local qw(timegm);
use Net::FTP;
use File::Copy;

#use Data::Dumper;
use lib '..';
use TQASched;

$cfg = load_conf('..');
my $debug_mode = $cfg->report_debug;

# redirect that STDERR if it's not going to the term
redirect_stderr( $cfg->report_log ) if caller && !$debug_mode;

# we only need the handle for TQASched db in the report! that's all, folks!
my ($dbh_sched) = refresh_handles( ('sched', 'change') );

# all possible POST parameters
my ( $headerdate, $headertime, $dbdate ) = calc_datetime();


my $target_trans = $cfg->target_trans;
if ($target_trans) {
	# TODO enable FTP download upon moving to a newer server
	#popup_ftp($target_trans);
	popup_cdb($target_trans);
	exit;
}

# get POST params
# parsed from CLI arg key/value pairs
my ( $post_date,       $legacy_filter, $dis_filter,   $prev_search,
	 $search_type,     $upd_checked,   $float_status, $report_title,
	 $refresh_enabled, $daemon_freeze
) = ('') x 10;

my $upd_num = 0;


# check whether the daemon has been frozen
$daemon_freeze = $cfg->freeze;

$float_status    = cfg_checked( $cfg->float_status );
$post_date       = $cfg->date;
$legacy_filter   = cfg_checked( $cfg->legacy );
$dis_filter      = cfg_checked( $cfg->dis );
$prev_search     = $cfg->search;
$search_type     = uc $cfg->search_type;
$upd_checked     = cfg_checked( $cfg->search_upd );
$report_title    = $cfg->title || 'TQASched :: Monitor';
$refresh_enabled = cfg_checked( $cfg->enable_refresh );
my $refresh_seconds = $cfg->refresh_seconds || 0;
my $show_cols = cfg_checked( $cfg->show_cols );

if ( $legacy_filter && $dis_filter ) {
	( $legacy_filter, $dis_filter ) = ( '', '' );
}

# TODO use switch or hash
my ( $id_selected, $feed_selected, $time_selected, $feed_date_selected )
	= ('') x 4;
if ( $search_type eq 'FEED' ) {
	$feed_selected = 'selected';
}
elsif ( $search_type eq 'FEED_ID' ) {
	$id_selected = 'selected';
}
elsif ( $search_type eq 'SCHEDULE_TIME' ) {
	$time_selected = 'selected';
}
elsif ( $search_type eq 'FEED_DATE' ) {
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

print_thead();

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
		= $refresh_enabled && $refresh_seconds >= 15
		? "<meta http-equiv='refresh' content='$refresh_seconds' >"
		: sprintf( "<!-- auto refresh not enabled %s -->",
				   ( $debug_mode ? "(debug: $refresh_seconds secs)" : '' ) );

	my $extra_styles = '';
	my $body_top     = '';
	if ($debug_mode) {
		$extra_styles .= "
		<style>
			table tr td {
			  font-size: large;
			  font-family: monospace;
			  width: 8em;
			  white-space:nowrap;
			}
		</style>
	";
		$report_title = "[DEBUG] $report_title";
	}

# display notification that daemon is not running (for small manual downtimes)
	if ($daemon_freeze) {
		$report_title = "[FREEZE] $report_title";

		#		$extra_styles .= "
		#		<style>
		#			table {
		#			  border: 10px solid lightblue
		#			}
		#		</style>
		#		";
		$body_top = "
		<div id='left'></div>
		<div id='right'></div>
		<div id='top'></div>
		<div id='bottom'></div>";
	}
	say "
<html>
	<head>
		$header_refresh
		<title>$report_title</title>
		<link rel='stylesheet' type='text/css' href='styles.css' />
		$extra_styles
	</head>
	<body>
	$body_top
";
}

sub print_thead {
	my @headers = $debug_mode || $show_cols
		? qw(
		Feed
		Feed_ID
		Priority
		Scheduled_Time
		Received_Time
		Feed_Date
		UPD
		Trans_Num
		Seq_Num
		)
		: qw(
		Feed
		Feed_ID
		Scheduled_Time
		Received_Time
		UPD
	);

	push @headers, 'Timestamp' if $debug_mode;

	my $num_headers   = scalar @headers;
	my $left_colspan  = int( $num_headers / 2 );
	my $right_colspan = $num_headers - $left_colspan;
	my ( $prevdate, $nextdate ) = calc_adjacent();
	my $header_warning     = '';
	my $page_table_classes = '';

	# styles and formats for debug mode
	if ($debug_mode) {
		$page_table_classes .= ' debug ';

		# debug will also display weekday name
		my $weekday_name = code_weekday( get_wd(), 1 );
		$header_warning .= "
			<tr>
				<th colspan = '$num_headers' class='warning'>
					<div class='debug'>
					<h1>DEBUG</h1>
					<h5>$weekday_name</h5>
					</div>
				</th>
			</tr>";
	}

	# styles and formats for frozen daemon
	if ($daemon_freeze) {
		$page_table_classes .= ' freeze ';

		$header_warning .= "
	<tr>
		<th colspan = '$num_headers' class='warning'>
			<div class='freeze'>
			<h3>MANUAL FREEZE</h3>
			<h5>daemon has been frozen for maintenance <br />
			all feeds will refresh upon thaw</h5>
			</div>
		</th>
	</tr>"
			;
	}

	say "
<form method='GET'>
	
	<table cellspacing='0' width='100%' border=0 class='$page_table_classes'>
		<thead>
			$header_warning
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
					<input type='checkbox' id='enable_refresh' name='enable_refresh' value='true' onclick='this.form.submit();' $refresh_enabled />
						<label for='enable_refresh'>Auto Refresh</label>
					<input type='checkbox' id='float_stat_cb' name='float_status' value='true' onclick='this.form.submit();' $float_status />
						<label for='float_stat_cb'>Float Status</label>		
					<input type='checkbox' id='show_cols_cb' name='show_cols' value='true' onclick='this.form.submit();' $show_cols />
						<label for='show_cols_cb'>Show All</label>			
				</th>
			</tr>
			<tr>
				<th colspan='$left_colspan'><a href='?date=$prevdate'><<</a> previous ($prevdate)</th>
				<th colspan='$right_colspan'>($nextdate) next <a href='?date=$nextdate'>>></a></th>
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
		if ( $search_type eq 'FEED' ) {
			$filter .= " and UPPER(u.name) like UPPER('%$prev_search%')";
		}
		elsif ( $search_type eq 'FEED_ID' ) {
			$filter .= " and UPPER(d.feed_id) like UPPER('%$prev_search%')";
		}
		elsif ( $search_type eq 'SCHEDULE_TIME' ) {
			my $sched_offset = time2offset($prev_search) + 86400 * $wd;
			$filter .= " and us.sched_epoch = $sched_offset";
		}
	}
	my $next_wd = shift_wd( $wd, 1 );
	my $filter_sched = " and ( us.weekday = $wd )";

	my $select_schedule1 = "
	  select distinct us.sched_id, us.update_id, us.sched_epoch, u.name, u.is_legacy, d.feed_id, u.prev_date, u.priority
	  from [Update_Schedule] us
	  join
	  [Updates] u
	  on u.update_id = us.update_id and weekday = $wd
	  
	  left join
	  [Update_DIS] d
	  on d.update_id = u.update_id 
	  where 
	  --weekday = $wd
      d.feed_id NOT LIKE 'FIEJV%'
      and d.feed_id NOT LIKE 'RDC%'
      and us.enabled = 1
      $filter
      order by sched_epoch asc, name asc
	";

	#my $filter_sched = "and (us.weekday = $current_wd)";
	my $select_schedule2 = "
		select distinct us.sched_id, us.update_id, us.sched_epoch, u.name, u.is_legacy, d.feed_id, u.prev_date, u.priority
		from 
			Update_Schedule us
			join
			Updates u
			on u.update_id = us.update_id
			join
			Update_DIS d
			on
			d.update_id = us.update_id
		and u.update_id = d.update_id
		and d.feed_id NOT LIKE 'FIEJV%'
      	and d.feed_id NOT LIKE 'RDC%'
		where
		--and u.is_legacy = 0
		
		us.enabled = 1
		$filter_sched
		$filter
		
	
		order by sched_epoch asc, name asc
		";

	my $select_specials
		= "select sched_id, uh.update_id, 0, name, is_legacy, ud.feed_id, prev_date, priority
	from update_history uh join updates u
	on
	uh.update_id = u.update_id
	left join update_dis ud
	on u.update_id = ud.update_id
	where
	
		
			feed_date = '$dbdate'
			and late = 'S'
		
	";

	#warn $select_specials and exit;
	#warn $select_schedule2 and exit;
	#warn $select_schedule;
	$filter = '';
	if ( $prev_search && $search_type eq 'FEED_DATE' ) {
		$filter .= " and feed_date = '$prev_search'";
	}
	if ($upd_checked) {
		$filter = "and filedate = $dbdate";
		if ($upd_num) {
			$filter .= " and filenum = $upd_num";
		}
	}

	#warn $select_history and die;
	#say 'executing sched query';
	my $sched_aref    = $dbh_sched->selectall_arrayref($select_schedule2);
	my $specials_aref = $dbh_sched->selectall_arrayref($select_specials);

	#say 'iterating...';
	my $row_count = 0;
	my %display_rows;
	my $border_prev;
	my ( $border_prev1, $border_prev2 ) = ( 0, 0 );
	for my $row_aref ( @{$specials_aref}, @{$sched_aref} ) {

		my ( $sched_id, $update_id, $sched_offset, $name, $is_legacy,
			 $feed_id, $prev_date, $priority )
			= @{$row_aref};

		#		my $feed_date = sched_id2feed_date($sched_id, $dbdate);
		#		if (!$feed_date) {
		#			warn "$sched_id";
		#			next;
		#		}

		if ( $is_legacy && $wd == 0 && $prev_date ) {
			my ( $psched_id, $poffset ) = prev_sched_offset($sched_id);
			$sched_id = $psched_id if $psched_id;
		}

		#		else {
		#			my $sched_feed_date = sched_id2feed_date($sched_id,$dbdate);
		#			$filter .= " and feed_date = '$sched_feed_date'"
		#		}

		my $last_week_date = date_math( -7, $dbdate );
		my $dis_filter = '';
		#warn $last_week_date;
		if ( !$is_legacy ) {
			$dis_filter = "
	--	and seq_num  > 
	--		(select top 1 seq_num from update_history 
	--		where sched_id = $sched_id
	--		--and datediff(dd,  timestamp, '$dbdate') < 6 
	--		and feed_date <= '$last_week_date'
	--		order by seq_num desc)";
		}
		else {
			# getting assigne the incorrect feed date
			$dis_filter = "and feed_date > '$last_week_date'";
				#= "--and (abs(datediff(dd, '$dbdate', cast(cast(filedate as varchar(8)) as datetime))) < 7 or filedate is null";
		}
		my $select_history = "
		select top 1 hist_id, hist_epoch, filedate, filenum, timestamp, late, feed_date, seq_num, transnum,ops_id,comments
		from [Update_History]
		where
		
			sched_id = $sched_id
			and feed_date <= '$dbdate'
			$dis_filter
			$filter
		--	and late != 'S'
		
		
		order by hist_id desc
	";
	warn $select_history if $update_id == 195	;
		my $select_special = "
		select hist_id, hist_epoch, filedate, filenum, timestamp, late, feed_date, seq_num, transnum,ops_id,comments 
		from update_history
		where late = 'S'
		and feed_date = '$dbdate'
		order by hist_id desc
	";

	 #warn $select_special and exit if $sched_id == -1;# if $update_id == 406;
	 #exit;
	 #	open LOG, '>>test.log';
	 #	say LOG $select_history;
	 #	close LOG;
	 #
	 #say $select_history if $sched_id == 271;
		my $hist_query = $dbh_sched->prepare(
						$sched_id == -1 ? $select_special : $select_history );
		$hist_query->execute();

		my ( $hist_id,  $hist_offset, $filedate,  $filenum,
			 $hist_ts,  $late,        $feed_date, $seq_num,
			 $transnum, $ops_id,      $comments
		) = $hist_query->fetchrow_array();

		# if no history record, feed date was too far in past - wait or late

		# skip 'wait' updates when searching by feed date
		if (    defined $feed_date
			 && $prev_search
			 && $search_type eq 'FEED_DATE'
			 && $feed_date !~ $prev_search )
		{

			next;
		}
		$row_count++;

		#say "fetched row for $sched_id";
		# this has been seen for today, has history record

		my ( $row_class, $status, $sched_time, $recvd_time, $daemon_ts,
			 $update, $feed_date_pretty )
			= row_info( $row_count,    $late,        $hist_id,
						$sched_offset, $hist_offset, $hist_ts,
						$filedate,     $filenum,     $feed_date,
						$prev_date,    $is_legacy
			);

		my $border_class1 = '';
		my $border_class2 = '';
		( $border_class1, $border_class2, $border_prev )
			= format_border( $row_class, $border_prev );

		my $tr_title = get_title( $row_class, $ops_id, $comments );
		my $extra_classes = '';

#		$tr_title .= defined $seq_num
#			? "auhseqnum = $seq_num\n"
#			: "no seqnum\n";
#		$tr_title .= defined $transnum && $transnum > 0 ? "distransnum = $transnum" : 'no transnum';
#		$tr_title .= "'";

		my ($jsa_open, $jsa_close)= ('')x2;
		if ($transnum) {
			$jsa_open = "<a href='javascript:;' onClick=\"window.open('?target_trans=$transnum', 'Transaction $transnum', 'scrollbars=yes,width=500,height=925');\">";
			$jsa_close = '</a>';
		}

		$seq_num    ||= 'N/A';
		$transnum   ||= 'N/A';
		$recvd_time ||= 'N/A';
		
		
		my $update_row = sprintf( "
		<tr $tr_title class='$border_class1 $border_class2 $row_class $extra_classes'>
			<td>$jsa_open%s$jsa_close\t%s</td>
			<td>%s</td>
			%s
			<td>%s</td>
			<td>%s</td>
			%s
			<td>%s</td>
			%s
			%s
			%s
		</tr>
		",
			$name, ( $debug_mode ? "[$update_id]" : '' ),
			( $feed_id ? $feed_id : 'N/A' ),
			( $show_cols || $debug_mode ? "<td>$priority</td>" : '' ),
			$sched_time, $recvd_time,
			( $show_cols || $debug_mode ? "<td>$feed_date_pretty</td>" : '' ),
			$update,
			( $show_cols || $debug_mode ? "<td>$transnum</td>" : '' ),
			( $show_cols || $debug_mode ? "<td>$seq_num</td>"  : '' ),
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

	my @stati = qw(late wait laterecv recv);
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

# generate appropriate border for the row class
sub format_border {
	my ( $row_class, $border_prev ) = @_;

	my ( $border_class1, $border_class2 ) = ( '', '' );
	if ( $row_class =~ m/laterecv/ ) {
		$border_class1 = 'lateborder1';
		if ( !$border_prev ) {
			$border_class2 = 'lateborder2';
			$border_prev   = 1;
		}
		else {
			$border_class2 = 'lateborder3';
		}
	}
	elsif ( $row_class =~ m/empty/ ) {
		$border_class1 = 'emptyborder1';
		if ( !$border_prev ) {
			$border_class2 = 'emptyborder2';
			$border_prev   = 1;
		}
		else {
			$border_class2 = 'emptyborder3';
		}
	}
	elsif ( $row_class =~ m/skip/ ) {
		$border_class1 = 'skipborder1';
		if ( !$border_prev ) {
			$border_class2 = 'skipborder2';
			$border_prev   = 1;
		}
		else {
			$border_class2 = 'skipborder3';
		}
	}
	else {
		$border_prev = 0;
	}
	return ( $border_class1, $border_class2, $border_prev );
}

# assign a style to row based on count
sub row_info {
	my ( $row_count,   $late,      $hist_id,  $sched_offset,
		 $hist_offset, $hist_ts,   $filedate, $filenum,
		 $feed_date,   $prev_date, $is_legacy
	) = @_;

	$feed_date ||= 'N/A';
	$feed_date =~ s/\s.*//;

	my $row_parity = $row_count % 2;

	my $sched_time = offset2time( $sched_offset, 1 );

	my ( $status, $daemon_ts, $recvd_time, $update );

	# special updates are processed first
	if ( defined $late && $late eq 'S' ) {

		#warn $filenum and exit;
		$status     = 'special';
		$update     = $filedate ? "$filedate-$filenum" : 'N/A';
		$daemon_ts  = $hist_ts;
		$sched_time = 'N/A';
		$recvd_time = $filedate ? offset2time($hist_offset) : 'N/A';
	}

# history record exists, the feed date is not equal to one week ago and it isn't a skipped update

	elsif (    defined $hist_id
			&& $sched_offset >= 172800
			&& !( report_date_math(-7) eq $feed_date )
			&& $late ne 'K' )
	{

		# if marked as not late or empty
		if ( $late eq 'N' || $late eq 'E' ) {

# if update isn't marked as a previous day
# and not empty (marked as N - this was marked as recvd but still could be wrong day)
# and yesterday's feed date equals current feed date (typically the case)
# and the offset is early day, probably processed previous GMT day
			if (    !defined $prev_date
				 && $late eq 'E'
				 && report_date_math(-1) eq $feed_date
				 && $sched_offset % 86400 < 10800 )
			{

# think this was to mark updates as wait if they had previous empty updates and were also early day

				$status = 'wait';
			}
			else {

 # otherwise it's later in the day and not a prev_day feed so mark as received
				$status = 'recv';

				#				if ( $late eq 'N' ) {
				#					$status = 'recv';
				#				}
				#				elsif ( $late eq 'E' ) {
				#					$status = 'empty';
				#				}
			}
		}

		# all others are marked as late... maybe add elsif = 'Y'
		#		elsif ($late eq 'Y' && $sched_offset <= 108000) {
		#			#warn 'hits';
		#			$status = 'recv';
		#		}
		# weekend case, mark recvd instead of late

		else {
			$status = 'late';
		}

		#$status     = $late eq 'N' ? 'recv'                    : 'late';
		$recvd_time = $filedate ? offset2time($hist_offset) : 'N/A';
		$daemon_ts  = $hist_ts;
		$update     = $filedate ? "$filedate-$filenum" : 'N/A';

		# if a records was found and late, then it was received and late
		# use to apply border
		$status =~ s/late/laterecv/;
	}

	# skipped update
	elsif ( defined $late && $late eq 'K' ) {
		$status     = 'skip';
		$recvd_time = 'N/A';
		$update     = 'N/A';
		$daemon_ts  = $hist_ts;
	}

	# handle monday special display case
	elsif ( $sched_offset < 172800 && defined $late ) {
		$daemon_ts  = $hist_ts;
		$update     = $filedate ? "$filedate-$filenum" : 'N/A';
		$recvd_time = $filedate ? offset2time($hist_offset) : 'N/A';

		# TODO this is a display fix for weekend lateness
		if ( $late eq 'Y' ) {
			$status = 'recv';
		}
		if ( $late eq 'E' ) {
			$status     = 'empty';
			$recvd_time = 'N/A';
			$update     = 'N/A';
		}

		# really early recv'd updates
		else {
			$status = 'recv';
		}
	}

	# no history record, still waiting or it is late
	else {

		$recvd_time = 'N/A';
		$status     = 'wait';
		$daemon_ts  = 'N/A';
		$update     = 'N/A';
		$feed_date  = 'N/A';
	}

	my $row_class = '';

	# this is an empty update, change its color
	if (    defined $late
		 && $late       eq 'E'
		 && $status     eq 'recv'
		 && $recvd_time eq 'N/A'
		 && $update     eq 'N/A' )
	{
		$row_class = 'empty' . ( $row_parity ? '_even' : '_odd' );
	}

	# late check on all updates that are still in wait state by this point
	elsif ( !defined $late && $status eq 'wait' ) {
		#warn "$sched_offset $dbdate\n";
		my $display_late = time > sched_epoch( $sched_offset, $dbdate );
		$row_class = ( $is_legacy
					   ? ( $display_late ? 'error' : 'wait' )
					   : ( $display_late ? 'late' : 'wait' )
		) . ( $row_parity ? '_even' : '_odd' );
	}
	else {
		$row_class = $status . ( $row_parity ? '_even' : '_odd' );
	}

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
	# TODO what about dates further in the past, weekends especially?
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

	# TODO change this to display actual date
	my $date_display = '';
	if ( $past_flag && !$sched_flag ) {
		$date_display = report_date_math(-1) || 'prev day';

	}
	elsif ($sched_flag) {
		$date_display = report_date_math(0);
	}
	elsif ($fut_flag) {
		$date_display = report_date_math( -$fut_flag + 1 )
			|| "$fut_flag future?";
	}
	else {
		my $math = $offset ? 0 : -1;
		$date_display = report_date_math($math);
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

# determine if a config value is set to truth
# return 'checked' string for dynamic HTML form state
sub cfg_checked {
	my ($cfg_val) = @_;
	if ( defined $cfg_val ) {
		$cfg_val =~ REGEX_TRUE ? 'checked' : '';
	}
	else {
		return '';
	}

}

## convert 24hr time to seconds offset from beginning of the day
## next it will have a day offset in seconds added to it where Sunday = 0
#sub time2offset {
#	my $time_string = shift;
#	my ( $hours, $minutes ) = ( $time_string =~ m/(\d+):(\d+)/ );
#	unless ( defined $hours && defined $minutes ) {
#		warn "\tparsing error converting time to offset: $time_string\n";
#		return;
#	}
#	return $hours * 3600 + $minutes * 60;
#}

# returns human readable row title for given status
sub get_title {
	my ( $status, $ops_id, $comments ) = @_;
	$ops_id   ||= '';
	$comments ||= '';
	my $title = '';
	if ( $status =~ m/^recv/ ) {
		$title = 'received on time';
	}
	elsif ( $status =~ m/laterecv/ ) {
		$title = 'received late';
	}
	elsif ( $status =~ m/late$/ ) {
		$title = 'late, not yet received';
	}
	elsif ( $status =~ m/empty/ ) {
		$title = 'received and empty';
	}
	elsif ( $status =~ m/skip/ ) {
		$title = 'skipped';
	}
	elsif ( $status =~ m/error/ ) {
		$title = 'stale legacy checklist, awaiting operator refresh';
	}
	elsif ( $status =~ m/wait/ ) {
		$title = 'still waiting';
	}
	elsif ( $status =~ m/special/ ) {
		$title = 'special';
	}

	if ($ops_id) {
		$title .= "\n$ops_id";
	}
	if ($comments) {
		$title .= "\n$comments";
	}

	return "title='$title'";
}

# do date math in days on current view's date
sub report_date_math {
	my ($delta_days) = @_;
	my ( $month, $day, $year ) = ( $headerdate =~ m!(\d+)/(\d+)/(\d+)! )
		or ( return and warn "could not do date math!\n" );
	my $time = timegm( 0, 0, 0, $day, $month - 1, $year - 1900 );
	$time += $delta_days * 86400;
	my ( $sec, $min, $hour, $mday, $mon, $y, $wday, $yday, $isdst )
		= gmtime($time);
	return sprintf '%u-%02u-%02u', $y + 1900, $mon + 1, $mday;
}

# popup upd sourced from changedb
sub popup_cdb {
	my ($t_transnum) = @_;
	my ($dbh_prod1, $dbh_cdb) = refresh_handles('prod1', 'change');
	
	print "HTTP/1.0 200 OK\r\n";
	print "Content-type: text/html\n\n";
	
	
	my $select_transaction_query = "
	SELECT [Id]
      ,[TaskId]
      ,[Sender]
      ,[TransactionNumber]
      ,[FeedDate]
      ,[TaskReference]
      ,[SeqNum]
      ,[Options]
      ,[Priority]
      ,[BuildTime]
      ,[OutputFilePath]
      ,[FileCount]
      ,[FileSize]
      ,[IsLegacy]
      ,[ProcessTime]
      ,[Status]
      ,[FileDate]
      ,[FileNum]
      ,[PackTaskId]
  FROM [TQALIC].[dbo].[PackageQueue] with (NOLOCK)
	where transactionnumber = $t_transnum
";
	my  @query_results
		= $dbh_prod1->selectrow_array($select_transaction_query);
		
	my @pq_headers = qw(
	  Id
      TaskId
      Sender
      TransactionNumber
      FeedDate
      TaskReference
      SeqNum
      Options
      Priority
      BuildTime
      OutputFilePath
      FileCount
      FileSize
      IsLegacy
      ProcessTime
      Status
      FileDate
      FileNum
      PackTaskId
	);
	
	print "
	<html>
		<head>
			<title>Transaction Number: $t_transnum</title>
			<meta http-equiv='refresh' content='300' >
			<link rel='stylesheet' type='text/css' href='styles.css' />
		</head>	
		<body>
	<table class='popup'>
	";
	
	my $count = 0;
	for my $header (@pq_headers) {
		my $result = $query_results[$count++];
		print "
			<tr>
				<th>
				$header
				<th>
			</tr>
			<tr>
				<td>
				$result
				</td>
			</tr>
		"
	}
#	
#	my $cdb_query = "select * from changedb_current"
#	
#	$dbh_cdb->selectall_arrayref();
	
	print "
	</table>
	<table>
		<tr>
			<td>UPD:</td>
			<td>not yet supported</td>
		</tr>
	</table>
	</body>
	</html>
	";	
}

# popup upd sourced from FTP download
sub popup_ftp {
	my ($t_transnum) = @_;


	die 'no transum' if !$t_transnum;

	my $output = '';

	my ($dbh_prod1) = refresh_handles('prod1');

	print "HTTP/1.0 200 OK\r\n";
	print "Content-type: text/html\n\n";

	my $select_transaction_query = "
	select sender, outputfilepath from [TQALic].dbo.[PackageQueue] with (NOLOCK)
	where transactionnumber = $t_transnum
";

	my ( $sender, $output_file_local )
		= $dbh_prod1->selectrow_array($select_transaction_query);

	my $dbh_dis = TQASched::sender2dbh($sender);

	my $select_ftp_path = "
	select ftpfilepath from dataingestioninfrastructure.dbo.makeupdateinfo with (NOLOCK)
	where distransactionnumber = $t_transnum
";

	my ($ftp_path) = $dbh_dis->selectrow_array($select_ftp_path);

	my ( $ftp_host, $directory ) = parse_ftp_path($ftp_path);

	my $ftp = Net::FTP->new($ftp_host) or die 'failed to connect';

	$ftp->login( 'marketqa_client', 'voodo' ) or die 'failed to login';

	$ftp->cwd($directory) or die 'failed to cwd';

	my @files = $ftp->ls;
	
	$ftp->binary;

	my $local_file = $ftp->get( $files[0] );

	$ftp->quit;

	unless (-d 'Files') {
		mkdir 'Files';
	}
	
	move( $local_file, "Files/$local_file" );

	print "
	<html>
		<head>
			<title>Transaction Number: $t_transnum</title>
			<meta http-equiv='refresh' content='300' >
			<link rel='stylesheet' type='text/css' href='styles.css' />
		</head>	
		<body>
			<table>
				<tr>
					<td>Output File:</td>
					<td><a href='$local_file' class='download'>$local_file</a></td>
				</tr>
				<tr>
					<td>UPD:</td>
					<td>not yet supported</td>
				</tr>
			</table>
		</body>
	</html>
	";
}

# return the host for the FTP path to file
sub parse_ftp_path {
	my ($path) = @_;
	my ( $host, $dir ) = $path =~ m!ftp://((?:\d+\.?){4})/(.*)!;
	$host =~ s/\.4$/.20/;
	return ( $host, $dir );
}
