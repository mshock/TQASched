#! perl -w

# TQASched daemon polls scheduling data and updates database

use strict;
use Time::Local;
use feature 'say';

use lib '..';
use TQASched qw(:all);

my $cfg = load_conf('..');

$debug_mode = $cfg->debug;
my $pause_mode = $cfg->pause_mode;

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

	# reload configs each run
	# not a good idea - overrides CLI args!
	# TODO preserve CLI args while refreshing the rest of configs from file
	#$cfg = load_conf('..');
	#say 'configs reloaded, checking out some new db handles';

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

	refresh();
	say 'daemon run finished, slaying db handles';
	kill_handles( $dbh_sched, $dbh_auh,  $dbh_prod1, $dbh_dis1,
				  $dbh_dis2,  $dbh_dis3, $dbh_dis4,  $dbh_dis5 );
	say "sleeping for ${\$cfg->update_freq} seconds...";
	last if $cfg->runonce;
	sleep( $cfg->update_freq );
}

sub refresh {

	my ( $year, $month, $day ) = get_today();

	#my ( $year, $month, $day ) = ( 2013, 4, 12 );

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
		my ( $tyear, $tmonth, $tday ) = get_tomorrow();
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
