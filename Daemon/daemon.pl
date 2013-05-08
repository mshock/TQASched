#! perl -w

# TQASched daemon polls scheduling data and updates database

use strict;
use Time::Local;
use feature 'say';

use lib '..';
use TQASched qw(:all);

$cfg = load_conf('..');

my $debug_mode = $cfg->debug;
my $pause_mode = $cfg->pause_mode;
my $refresh_to = $cfg->refresh_to;
my $refresh_from = $cfg->refresh_from;
my $refresh_date = $cfg->refresh_date;

# force runonce with refresh ranges, error if improper range
if ($refresh_to || $refresh_from || $refresh_date) {
	if ($refresh_to && !$refresh_from) {
		die 'refresh_to requires start date (-refresh_from)';
	}
	$cfg->set('runonce', 1);
}  

# redirect that STDERR if it's not going to the term
redirect_stderr( $cfg->daemon_log ) if caller && !$debug_mode;

say 'taking a peek at my handles';

# load database handles for use in the daemon - test run
my ( $dbh_sched, $dbh_auh,  $dbh_prod1, $dbh_dis1,
	 $dbh_dis2,  $dbh_dis3, $dbh_dis4,  $dbh_dis5
) = refresh_handles();

say 'done looking at handles!';

say
	'hold onto your butts, the daemon is beginning its infinite duty cycle (or until otherwise notified, mangled, and/or killed)';
write_log( { logfile => $cfg->daemon_log,
			 msg     => 'daemon starting duty cycle',
			 type    => 'INFO'
		   }
);
my $run_counter = 0;
while ( ++$run_counter ) {
	say "daemon has awoken and is beginning cycle number $run_counter";

	# reload file configs
	say $cfg->file( '../' . $cfg->config_file )
		? 'config file reloaded'
		: "failed to reload config file $!";

	# exit if freeze set
	if ( $cfg->freeze && !$cfg->force_refresh ) {
		say 'daemon has been frozen, exiting';
		exit;
	}
	
	# get new db handles each run
	(  $dbh_sched, $dbh_auh,  $dbh_prod1, $dbh_dis1,
	   $dbh_dis2,  $dbh_dis3, $dbh_dis4,  $dbh_dis5
	) = refresh_handles();

	# verify that all handles are defined
	# sometimes servers hang, daemon should wait on them rather than crash
	if ( my $num_bad = check_handles() ) {
		dsay "$num_bad handles not defined, skipping cycle";

		#
		#		write_log( { logfile => $cfg->daemon_log,
		#					 type    => 'ERROR',
		#					 msg     => "$num_bad handles not defined, skipping cycle"
		#				   }
		#		);
	}

	say 'got new db handles, running tasks';
	# auto-run a range of dates
	my @refresh_dates = ();
	if ($refresh_from) {
		my $refresh_end = $refresh_to ? $refresh_to : format_dateparts( get_today() );
		until ( $refresh_from > $refresh_end) {
			#say "end $refresh_end from $refresh_from";
			push @refresh_dates, $refresh_from;
			$refresh_from = date_math(1, $refresh_from, '');
		}
	}
	elsif ($refresh_date) {
		push @refresh_dates, $refresh_date;
	}
	else {
		push @refresh_dates, format_dateparts(get_today());
	}
	refresh($_) for @refresh_dates;
	say 'daemon run finished, slaying db handles';
	kill_handles( $dbh_sched, $dbh_auh,  $dbh_prod1, $dbh_dis1,
				  $dbh_dis2,  $dbh_dis3, $dbh_dis4,  $dbh_dis5 );
	
	last if $cfg->runonce;
	say "sleeping for ${\$cfg->update_freq} seconds...";
	sleep( $cfg->update_freq );
}

sub refresh {
	my ($target_date) = @_;
	my ( $year, $month, $day ) = $target_date ? parse_filedate($target_date) :  get_today();

	#my ( $year, $month, $day ) = ( 2013, 5, 1);
	my $refresh_date_string = sprintf( '%u%02u%02u', $year, $month, $day );
	say "\nREFRESH DATE: $refresh_date_string";

	# TODO fork children to do each refresh (how to handle handles?)

	refresh_dis( { year       => $year,
				   month      => $month,
				   day        => $day,
				   pause_mode => $pause_mode,
				 }
	) if $cfg->refresh_dis;

	refresh_legacy( { year       => $year,
					  month      => $month,
					  day        => $day,
					  pause_mode => $pause_mode,
					}
	) if $cfg->refresh_legacy;

	if ( $cfg->lookahead ) {
		my ( $tyear, $tmonth, $tday )
			= parse_filedate( date_math( 1, $refresh_date_string ) );
		say "lookahead: $tyear$tmonth$tday";
		refresh_dis( { year       => $tyear,
					   month      => $tmonth,
					   day        => $tday,
					   pause_mode => $pause_mode,
					 }
		) if $cfg->refresh_dis;
		refresh_legacy( { year       => $tyear,
						  month      => $tmonth,
						  day        => $tday,
						  pause_mode => $pause_mode,
						}
		) if $cfg->refresh_legacy;
	}

}

sub get_today {
	my ( $sec, $min, $hour, $mday, $mon, $year, $wday, $yday, $isdst )
		= gmtime(time);
	return ( $year + 1900, $mon + 1, $mday );
}

sub get_tomorrow {
	my ( $sec, $min, $hour, $mday, $mon, $year, $wday, $yday, $isdst )
		= gmtime( time + 86400 );
	return ( $year + 1900, $mon + 1, $mday );
}

# write to log when daemon exits (catches most cases)
END {
	write_log( { logfile => $cfg->daemon_log,
				 msg     => 'daemon has stopped',
				 type    => 'INFO'
			   }
	);
}
