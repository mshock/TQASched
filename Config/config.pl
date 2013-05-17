#! perl -w

# constructs AppConfig options hash for all configs
# possible future configuration routines
# # # # # # # # # # # # # # # # # # # # # # # # # # #
# was becoming too unwieldy to edit within TQASched.pm
# should now be easier to add/remove/set/get default configs
#
# USAGE:
#	require 'config.pl';
#	TQASched::Config::define_defaults(\$cfg);

# INV: this works, but is it sane?
package TQASched::Config;

# TRUTHINESS
1;

# one subroutine to define them all
# previously housed in TQASched.pm
sub define_defaults {

	# takes and acts upon passed reference to AppConfig object
	my $cfg_ref = shift;

	my %config_vars = (

		# server configs
		# server host port ex: localhost:9191
		server_port => { DEFAULT => 9191,
						 ARGS    => '=i',
						 ALIAS   => 'host_port|port|p',
		},

   # server auto-start, good to set in conf file once everything is running OK
		server_start => { DEFAULT => 0,
						  ARGS    => '!',
						  ALIAS   => 'start_server|s',
		},

		# server logfile path
		server_logfile => { DEFAULT => 'server.log',
							ALIAS   => 'server_log',
		},

		# path to script which prints content
		# this content is hosted through TCP/IP under HTTP
		server_report_script => { DEFAULT => 'report.pl',
								  ALIAS   => 'report_script',
		},

		server_maint_script => { DEFAULT => 'maint.pl',
								 ALIAS   => 'maint_script',
		},

		server_maint_mode => { DEFAULT => 0,
							   ALIAS   => 'maint_mode'
		},

   # daemon configs
   # daemon auto-start, good to set in conf file once everythign is running OK
		daemon_start => { DEFAULT => 0,
						  ARGS    => '!',
						  ALIAS   => 'start_daemon|d'
		},

		# periodicity of the daemon loop (seconds to sleep)
		daemon_update_frequency => { DEFAULT => 60,
									 ALIAS   => 'update_freq',
		},

		# daemon logfile path
		daemon_logfile => { DEFAULT => 'daemon.log',
							ALIAS   => 'daemon_log',
		},
		daemon_runonce => { DEFAULT => 0,
							ARGS    => '!',
							ALIAS   => 'runonce',
		},

		# allow daemon to poll updates for legacy feeds
		daemon_refresh_legacy => { DEFAULT => 1,
								   ARGS    => '!',
								   ALIAS   => 'refresh_legacy',
		},

		# allow daemon to poll updates for dis feeds
		daemon_refresh_dis => { DEFAULT => 1,
								ARGS    => '!',
								ALIAS   => 'refresh_dis',
		},

	   # enable the daemon to scan 1 GMT day ahead of current
	   # TODO seems to break feed date calcs, also not necessary for lookahead
		daemon_lookahead => { DEFAULT => 0,
							  ARGS    => '!',
							  ALIAS   => 'lookahead',
		},

		# look at the previous day first before attempting to store current for prev_date feeds
		daemon_lookbehind => {
			DEFAULT => 1,
			ARGS => '!',
			ALIAS => 'lookbehind',
		},

		# wait for keypress after each update processed
		# various commands available
		# Q => quit
		# TODO more commands - rerun, skip ahead, etc
		daemon_pause_mode => { DEFAULT => 0,
							   ARGS    => '!',
							   ALIAS   => 'pause_mode',
		},

		daemon_freeze => { DEFAULT => 0,
						   ALIAS   => 'freeze',
		},

		daemon_force_refresh => { DEFAULT => 0,
								  ARGS    => '!',
								  ALIAS   => 'force_refresh',
		},
		daemon_refresh_date => { DEFAULT => 0,
								  ARGS    => ':s',
								  ALIAS   => 'refresh_date',
		},
		daemon_refresh_from => {
			DEFAULT => 0,
								  ARGS    => ':s',
								  ALIAS   => 'refresh_from',
		},
		daemon_refresh_to => {
			DEFAULT => 0,
								  ARGS    => ':s',
								  ALIAS   => 'refresh_to',
		},

		# scheduling configs
		#
		# path to master scheduling data spreadsheet
		# TODO this probably no longer loads with new format
		sched_master_schedule => { DEFAULT => 'TQA_Update_Schedule.xls',
								   ARGS    => '=s',
								   ALIAS   => 'master_sched',
		},

		# path to blank master checklist
		sched_master_checklist => { DEFAULT => '.',
									ARGS    => '=s',
									ALIAS   => 'master_checklist',
		},

		# optional filename specification for checklist file
		sched_checklist => { DEFAULT => '',
							 ARGS    => '=s',
							 ALIAS   => 'checklist',
		},

		# path to the operator legacy update checklist (network) directory
		sched_checklist_path => { DEFAULT => '.',
								  ARGS    => '=s',
								  ALIAS   => 'checklist_path',
		},

		# initialize scheduling data
		# parse master schedule
		# insert scheduling records and metadata into db
		sched_init => { DEFAULT => 0,
						ARGS    => '!',
						ALIAS   => 'init_sched|i',
		},

	   # create scheduling the scheduling database framework from scratch, yum
		sched_create_db => { DEFAULT => 0,
							 ARGS    => '!',
							 ALIAS   => 'create_db|c',
		},

		# ignore db existence check, for debugging
		sched_force_create_db => { DEFAULT => 0,
								   ALIAS   => 'force_create',
		},

		# link update ids to feed ids in DIS
		sched_import_dis => { DEFAULT => 0,
							  ARGS    => '!',
							  ALIAS   => 'import_dis|m'
		},

		# report (content gen script) configs
		# report script's logfile
		report_logfile => {
			DEFAULT => 'report.log',
			ALIAS   => 'report_log',

		},

# path to css stylesheet file for report gen, hosted statically and only by request!
# all statically hosted files are defined relative to the TQASched/Resources/ directory, where they enjoy living (for now, bwahahaha)
		report_stylesheet => { DEFAULT => 'styles.css',
							   ALIAS   => 'styles|stylesheet',
		},

# path to jquery codebase (an image of it taken sometime in... Jan 2013) - not in use yet
		report_jquery => { DEFAULT => 'jquery.js',
						   ALIAS   => 'jquery',
		},

	# path to user created javascript libraries and functions - not in use yet
		report_user_js => { DEFAULT => 'js.js',
							ALIAS   => 'user_js',
		},

		# refresh rate for report page
		report_refresh => { DEFAULT => '300',
							ALIAS   => 'refresh_seconds',
		},
		report_view_debug => { DEFAULT => 0,
							   ALIAS   => 'report_debug',
		},

		#-------------------------------------------------------------------
		#  all CGI variables for the report follow:
		#-------------------------------------------------------------------
		# target date
		report_date => { DEFAULT => '',
						 ARGS    => ':i',
						 ALIAS   => 'date',
		},

		# filter only legacy radio button
		report_legacy_filter => {
								DEFAULT => '',
								ARGS    => ':s',
								ALIAS => 'legacy|legacy_filter|filter_legacy',
		},

		# filter only dis radio button
		report_dis_filter => { DEFAULT => '',
							   ARGS    => ':s',
							   ALIAS   => 'dis|dis_filter|filter_dis',
		},
		report_search => { DEFAULT => '',
						   ARGS    => ':s',
						   ALIAS   => 'search',
		},
		report_search_upd => { DEFAULT => '',
							   ARGS    => ':s',
							   ALIAS   => 'search_upd',
		},
		report_search_type => { DEFAULT => '',
								ARGS    => ':s',
								ALIAS   => 'search_type',
		},
		report_float_status => { DEFAULT => '',
								 ARGS    => ':s',
								 ALIAS   => 'float_status',
		},

		report_enable_refresh => { DEFAULT => '',
								   ARGS    => ':s',
								   ALIAS   => 'enable_refresh',
		},
		report_show_cols => { DEFAULT => '',
							  ARGS    => ':s',
							  ALIAS   => 'show_cols',
		},

		# optional window title, useful for side-by-side comparisons
		report_title => { DEFAULT => '',
						  ALIAS   => 'title',
		},
		
		report_popup_tnum => {
			DEFAULT => '',
			ARGS => ':i',
			ALIAS => 'target_trans'
		}, 

# refresh rate for the report page - can't be less than 10, and 0 means never.
# (in seconds)

		# default (misc) configs
		#
		# toggle or set verbosity level to turn off annoying, snarky messages
		default_verbosity => { DEFAULT => 1,
							   ARGS    => ':i',
							   ALIAS   => 'verbosity|verbose|v',
		},

		# toggle logging
		default_enable_logging => { DEFAULT => 1,
									ARGS    => '!',
									ALIAS   => 'logging|logging_enabled|l',
		},

		# timezone to write log timestamps in
		default_log_tz => { DEFAULT => 'local',
							ALIAS   => 'tz|timezone',
		},

		# helpme / manpage from pod
		default_help => { DEFAULT => 0,
						  ARGS    => '!',
						  ALIAS   => 'help|version|usage|h'
		},

# path to config file (relative to the module)
# (optional, I suppose if you wanted to list all database connection info in CLI args)
		default_config_file => { DEFAULT => "Config/TQASched.ini",
								 ARGS    => '=s',
								 ALIAS => "cfg_file|conf_file|config_file|f",
		},

# toggle dryrun mode = non-destructive test of module load and all db connections
		default_dryrun => { DEFAULT => 0,
							ARGS    => '!',
							ALIAS   => 'dryrun|y',
		},
		default_logfile => { DEFAULT => 'TQASched.log',
							 ALIAS   => 'log',
		},
		default_enable_warn => { DEFAULT => 1,
								 ALIAS   => 'enable_warn',
		},

		# server to use for SendMail.pm
		default_smtp_server => { DEFAULT => 'localhost',
								 ARGS    => '=s',
								 ALIAS   => 'smtp_server'
		},
		default_late_threshold => { DEFAULT => 0,
									ALIAS   => 'late_threshold',
		},
		default_sql_definitions => { DEFAULT => 'Config/TQASched.sql',
									 ALIAS   => 'create_script|sql_file'
		},
		default_pod_docs => { DEFAULT => 'README.pod',
							  ALIAS   => 'pod|readme|docs'
		},

		# used for toggling debug tools or reporting
		default_debug => { DEFAULT => 0,
						   ARGS    => ':i',
						   ALIAS   => 'debug',
		},
	);

	${$cfg_ref}->define( $_ => \%{ $config_vars{$_} } ) for keys %config_vars;
}
