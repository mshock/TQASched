#! perl -w

# TQASched daemon polls scheduling data and updates database

use strict;
use feature 'say';

use lib '..';
use TQASched;

my $cfg = load_conf('..');

# redirect that STDERR if it's not going to the term
redirect_stderr( $cfg->daemon_log ) if caller;

say 'taking a peek at my handles';

# load database handles for use in the daemon - test run
my (  $dbh_sched, $dbh_auh,  $dbh_prod1, $dbh_dis1,
	   $dbh_dis2,  $dbh_dis3, $dbh_dis4,  $dbh_dis5
	) = refresh_handles();

say 'done looking at handles!';

say
	'hold onto your butts, the daemon is beginning its infinite duty cycle (or until otherwise notified, mangled, and/or killed)';
my $run_counter = 0;
while (++$run_counter) {
	say "daemon has awoken and is beginning cycle number $run_counter";
	# reload configs each run
	$cfg = load_conf('..');
	say 'configs reloaded, checking out some new db handles';

	# get new db handles each run
	(  $dbh_sched, $dbh_auh,  $dbh_prod1, $dbh_dis1,
	   $dbh_dis2,  $dbh_dis3, $dbh_dis4,  $dbh_dis5
	)  = refresh_handles();

	say 'got new db handles, running tasks';
	refresh_dis();
	refresh_legacy();

	say 'daemon run finished, slaying db handles';
	kill_handles( $dbh_sched, $dbh_auh,  $dbh_prod1, $dbh_dis1,
				  $dbh_dis2,  $dbh_dis3, $dbh_dis4,  $dbh_dis5 );
	say "sleeping for ${\$cfg->update_freq} seconds...";
	sleep( $cfg->update_freq );
}

# poll auh metadata for DIS feed statuses
sub refresh_auh {
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
		my ( $feed_id, $name, $offset, $sched_id, $update_id )
			= @{$update_aref};

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

		my ( $status, $exec_end, $fd, $fn, $sender, $trans_num, $build_time )
			= $dbh_prod1->selectrow_array($transactions);

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

			$backdate_updates
				= $dbh_sched->selectall_arrayref($backdate_query);

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
				or warn
				"\tno transaction # found for enum feed $name, skipping\n"
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
			( $status, $exec_end, $fd, $fn, $sender, $trans_num, $build_time )
				= $dbh_prod1->selectrow_array($transactions)
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
				update_history( { update_id    => $update_id,
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
				update_history( { update_id    => $update_id,
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
sub refresh_xls {

	# attempt to download the latest spreadsheet from OpsDocs server
	my $sched_xls = find_sched( $cfg->checklist );

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

			# skip unless filled in
			next
				unless $row_data->{update}
					&& $row_data->{filedate}
					&& $row_data->{filenum};

			my $name        = $row_data->{update};
			my $update_id   = get_update_id($name);
			my $sched_query = "
				select time, sched_id 
				from TQASched.dbo.Update_Schedule us
				where update_id = $update_id
				and weekday = '$weekday_code'
			";

			#say $sched_query and die;
			my ( $sched_offset, $sched_id )
				= $dbh_sched->selectrow_array($sched_query);

			unless ($sched_offset) {
				warn "no schedule entry for $name : $update_id : $sched_id\n";
				next;
			}

			my $exec_end     = gmtime(time);
			my $trans_offset = now_offset();
			my $ontime;

			# compare transaction execution time to schedule offset
			my $cmp_result = comp_offsets( $exec_end, $sched_offset );

			# if it's within an hour of the scheduled time, mark as on time
			# could also be early
			if ( $cmp_result == 0 ) {
				say "ontime $name $exec_end offset: $sched_offset";
				update_history( { update_id    => $update_id,
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
				say "late $name $exec_end to offset: $sched_offset";
				update_history( { update_id    => $update_id,
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
				say "waiting on $name, last trans: $exec_end";
			}
			else {
				warn
					"\tFAILED transaction offset sanity check: $name $sched_offset\n";
				next;
			}
		}
	}
}

sub refresh_dis {

}

sub refresh_legacy {

}
