#! perl -w

# TQASched daemon polls scheduling data and updates database

use strict;
use feature 'say';

use lib '..';
use TQASched qw(:all);

my $cfg = load_conf('..');

# redirect that STDERR if it's not going to the term
redirect_stderr( $cfg->daemon_log ) if caller;

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
	$cfg = load_conf('..');
	say 'configs reloaded, checking out some new db handles';

	# get new db handles each run
	(  $dbh_sched, $dbh_auh,  $dbh_prod1, $dbh_dis1,
	   $dbh_dis2,  $dbh_dis3, $dbh_dis4,  $dbh_dis5
	) = refresh_handles();

	# verify that all handles are defined
	# sometimes servers hang, daemon should wait on them rather than crash
	if ( my $num_bad = check_handles() ) {
		write_log( { logfile => $cfg->daemon_log,
					 type    => 'ERROR',
					 msg     => "$num_bad handles not defined, skipping cycle"
				   }
		);
	}

	say 'got new db handles, running tasks';

	#refresh_dis();
	refresh_legacy();

	say 'daemon run finished, slaying db handles';
	kill_handles( $dbh_sched, $dbh_auh,  $dbh_prod1, $dbh_dis1,
				  $dbh_dis2,  $dbh_dis3, $dbh_dis4,  $dbh_dis5 );
	say "sleeping for ${\$cfg->update_freq} seconds...";
	sleep( $cfg->update_freq );
}

# write to log when daemon exits (catches most cases)
END {
	write_log( { logfile => $cfg->daemon_log,
				 msg     => 'daemon has stopped',
				 type    => 'INFO'
			   }
	);
}
