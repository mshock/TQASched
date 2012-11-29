#! perl -w

package TQASched;

use strict;
use feature qw(say switch);
use Getopt::Std qw(getopts);
use Spreadsheet::ParseExcel;
use Spreadsheet::ParseExcel::Utility qw(ExcelFmt);
use Config::Simple;
use DBI;

# for inheritance later - only for daemon's webserver so far
our @ISA;

# globals
my ($daemon_lock);

my %opts;
getopts( 'c:df:h', \%opts );

( usage() and exit ) if $opts{h};

my $conf_file = $opts{c} || 'tqa_sched.conf';

say 'parsing config file...';

# load config file
my ( $sched_db, $auh_db, %conf ) = load_conf($conf_file);

# get excel file containing schedule info
my $sched_xls = $opts{f} || $conf{schedule_file} || find_sched();

# create parser and parse xls
my $xlsparser = Spreadsheet::ParseExcel->new();
my $workbook  = $xlsparser->parse($sched_xls)
	or die "unable to parse spreadsheet: $sched_xls\n", $xlsparser->error();
say 'done';

say 'initializing database handle...';

# initialize database handle
my $dbh_sched = init_handle($sched_db);
my $dbh_auh   = init_handle($auh_db);

say 'done';

# optionally create database and tables
( create_db() or die "failed to create database\n" ) if $opts{d};

# populate database from excel file

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

	# iterate over each row and store scheduling data
	for ( my $row = $row_min; $row <= $row_max; $row++ ) {
		next if $row <= 1;

		# per-update hash of column values
		my $row_data = {};
		for ( my $col = $col_min; $col <= $col_max; $col++ ) {
			my $cell = $worksheet->get_cell( $row, $col );
			last unless extract_row( $col, $cell, $row_data );
		}

		# skip rows that have no values, degenerates (ha)
		next unless $row_data->{update};

		# attempt to store rows that had values
		store_row( $weekday_code, $row_data )
			or warn "failed to store row $row for $weekday\n";
	}
}

####################################################################################
#	subs
#
####################################################################################

# run in daemon mode until interrupted
sub daemon {

	# length of time to sleep before updating report
	# in seconds
	# defaults to 1 minute
	my $update_freq = $conf{update_frequency} || 60;

	# fork child http server to host report
	fork or server();

	# trap interrupts to prevent exiting mid-update
	$SIG{'INT'} = 'INT_handler';

	# run indefinitely
	# polling spreadsheet and AUH db to update report at specified frequency
	while (1) {

		# lock against interrupt
		$daemon_lock = 1;

		# parse spreadsheet and insert new updates

		# examine AUH metadata and insert new updates

		# check if interrupts were caught
		if ( $daemon_lock > 1 ) {
			say sprintf(
					   'update completed, caught %u interrupts during update',
					   $daemon_lock - 1 );
			say 'interrupting TQASched services';
			exit(0);
		}
		else {
			$daemon_lock = 0;
		}

		sleep($update_freq);
	}
}

# server to be run in another process
# hosts the report webmon
sub server {

	# load webserver module and ISA relationship at runtime in child
	require HTTP::Server::Simple::CGI;
	push @ISA, 'HTTP::Server::Simple::CGI';
	my $server = TQASched->new( $conf{http_port} || 80 );
	$server->run();

	# just in case server ever returns
	die 'server has returned and is no longer running';
}

# interrupt (Ctrl+C) signal handler
# postpones interrupts recieved during update until finished
sub INT_handler {
	if ($daemon_lock) {

		# count the number of interrupts caught
		# also tracks whether interrupt was caught
		$daemon_lock++;
		say 'SIGINT caught, exiting when daemon releases update lock...';
	}
	else {

		# if not locked, business as usual
		say 'interrupted';
		exit(0);
	}
}

# override request handler for HTTP::Server::Simple
sub handle_request {
	my ( $self, $cgi ) = @_;

	# parse POST into CLI argument key/value pairs
	my $params_string = '';
	for ( $cgi->param ) {
		$params_string .= sprintf( '--%s=%s ', $_, $cgi->param($_) );
	}
	print `perl report.pl $params_string`;
}

# extract row into hash based on column number
sub extract_row {
	my ( $col, $cell, $row_href ) = @_;

	# get formatted excel value for most columns
	my $value = $cell ? $cell->value() : '';
	given ($col) {

		# time scheduled/expected
		when (/^0$/) {
			$row_href->{time_block} = $value if $value;
		}

		# update name (needs to be exactly the same every time)
		when (/^1$/) {
			$row_href->{update} = $value ? $value : return;
		}

		# priority - 'x' if not scheduled for the day
		when (/^2$/) {
			return unless $value;
			$row_href->{priority} = $value =~ m/x/ ? return : $value;
		}

		# file date
		when (/^3$/) {
			return unless $value;

			# extract unformatted datetime and convert to filedate integer
			my $time_excel = $cell->unformatted();
			my $value = ExcelFmt( 'yyyymmdd', $time_excel );

			# skip if not scheduled for this day
			$row_href->{filedate} = $value ? $value : return;
		}

		# file number
		when (/^4$/) {
			if ( $value != 0 ) {
				$row_href->{is_legacy} = 1;
				$row_href->{filenum} = $value ? $value : return;
			}
		}

		# ID
		when (/^5$/) {
			$row_href->{id} = $value;
		}

		# comment
		when (/^6$/) {
			$row_href->{comment} = $value;
		}

		# outside of parsing scope
		# return and go to next row
		default {return};
	}
	return 1;
}

# analyze and store a row from scheduling spreadsheet in database
sub store_row {
	my $weekday_code = shift;
	my $row_href     = shift;

	# don't store row if not scheduled for today
	# or row is blank
	# but not an error so return true
	return 1
		unless $row_href->{update}
			&& $row_href->{time_block}
			&& $row_href->{priority};

	# check if this update name has been seen before
	my $update_id;
	unless ( $update_id = get_update_id( $row_href->{update} ) ) {

		# if not, insert it into the database
		my $update_insert = "insert into [TQASched].dbo.[Updates] values 
			('$row_href->{update}','$row_href->{priority}', '$row_href->{is_legacy}')";
		$dbh_sched->do($update_insert)
			or warn
			"error inserting update: $row_href->{update}, probably already inserted\n",
			$dbh_sched->errstr
			and return;

		# get the id of the new update
		$update_id
			= $dbh_sched->last_insert_id( undef, undef, 'Updates', undef )
			or warn "could not retrieve last insert id\n", $dbh_sched->errstr
			and return;
	}

	# put entry in scheduling table
	my $time_offset  = time2offset( $row_href->{time_block} );
	my $sched_insert = "
		insert into [TQASched].dbo.[Update_Schedule] values 
			('$update_id','$weekday_code','$time_offset')
	";
	$dbh_sched->do($sched_insert)
		or warn
		"failed to insert update schedule info for update: $row_href->{update}\n",
		$dbh_sched->errstr
		and return;
}

# convert time of day (24hr) into minute offset from 12:00am
sub time2offset {
	my $time_string = shift;
	my ( $hours, $minutes ) = ( $time_string =~ m/(\d+):(\d+)/ );
	unless ( defined $hours && defined $minutes ) {
		warn "parsing error converting time to offset: $time_string\n";
		return;
	}
	return $hours * 60 + $minutes;
}

# get an update's id from name
sub get_update_id {
	my $name         = shift;
	my $select_query = "
		select update_id from [TQASched].dbo.[Updates] 
		where name = '$name'
	";
	return ( ( $dbh_sched->selectall_arrayref($select_query) )->[0] )->[0];
}

# load db info and optional params from config file
sub load_conf {
	my $conf_file = shift;

	# if file doesn't exist, create
	unless ( -f $conf_file ) {
		die "could not load config file: $conf_file, ",
			( init_conf()
			  ? 'a skeleton config has been created'
			  : 'failed to create skeleton config'
			),
			"\n";
	}

	my $cfg = new Config::Simple($conf_file);

	my $sched_db = $cfg->param( -block => 'sched_db' )
		or die
		"could not load sched database info from config file, check config file\n";
	my $auh_db = $cfg->param( -block => 'auh_db' )
		or die
		"could not load auh database info from config file, check config file\n";
	my $opts = $cfg->param( -block => 'opts' )
		or warn "could not load optional configs from config file\n"
		and return ( $sched_db, $auh_db, {} );

	return ( $sched_db, $auh_db, %{$opts} );
}

# translate weekday string to code
sub code_weekday {
	my $weekday = shift;
	my $rv;
	given ($weekday) {
		when (/monday/i)    { $rv = 'M' }
		when (/tuesday/i)   { $rv = 'T' }
		when (/wednesday/i) { $rv = 'W' }
		when (/thursday/i)  { $rv = 'R' }
		when (/friday/i)    { $rv = 'F' }
		when (/saturday/i)  { $rv = 'S' }
		when (/sunday/i)    { $rv = 'N' }
		default             { $rv = 'U' };
	}
	return $rv;
}

# create a basic config file for the user to fill in
sub init_conf {
	open( my $conf, '>', 'tqa_sched.conf' )
		or warn "could not create config file: $!\n" and return;
	print $conf '# basic config file
# insert database connection info here
[db]
server=
user=
pwd=
# optional configs
[opts]
# update frequency (in seconds) when running as daemon
update_frequency=60
http_port=8080';
	close $conf;
	return 1;
}

# get handle for master on sql server
sub init_handle {
	my $db = shift;

	# connecting to master since database may need to be created
	return
		DBI->connect(
		sprintf(
			"dbi:ODBC:Driver={SQL Server};Database=%s;Server=%s;UID=%s;PWD=%s",
			'master', $db->{server}, $db->{user}, $db->{pwd},
		)
		) or die "failed to initialize database handle\n", $DBI::errstr;
}

# create database if not already present
sub create_db {

	# if already exists, return
	say 'database already exists, skipping create flag' and return 1
		if check_db();
	say 'creating TQASched database...';

	# create the database
	$dbh_sched->do("create database [TQASched]")
		or die "could not create TQASched database\n";

	# create the tables
	$dbh_sched->do(
		"create table [TQASched].dbo.[Updates] (
		update_id int not null identity(1,1),
		name varchar(255) not null unique,
		priority tinyint,
		is_legacy bit
	)"
	) or die "could not create Updates table\n", $dbh_sched->errstr;
	$dbh_sched->do(
		"create table [TQASched].dbo.[Update_Schedule] (
		sched_id int not null identity(1,1),
		update_id int not null,
		weekday char(1),
		time int
	)"
		)
		or die "could not create Update_Schedule table\n", $dbh_sched->errstr;
	$dbh_sched->do(
		"create table [TQASched].dbo.[Update_History] (
		hist_id int not null identity(1,1),
		update_id int not null,
		sched_id int not null,
		time int,
		filedate int,
		filenum tinyint
	)"
		)
		or die "could not create Update_History table\n", $dbh_sched->errstr;

	say 'done creating db';
	return 1;
}

# check that database exists
sub check_db {
	my $check_query = "select db_id('TQASched')";
	return ( ( $dbh_sched->selectall_arrayref($check_query) )->[0] )->[0];
}

# drop the database
sub drop_db {
	return $dbh_sched->do('drop database TQASched')
		or die "could not drop TQASched database\n", $dbh_sched->errstr;
}

# clear all update records in database
sub clear_updates {
	return $dbh_sched->do('delete from [TQASched].dbo.[Updates]')
		or die "error in clearing Updates table\n", $dbh_sched->errstr;
}

# clear all scheduling records in database
sub clear_schedule {
	return $dbh_sched->do('delete from [TQASched].dbo.[Update_Schedule]')
		or die "error in clearing Schedule table\n", $dbh_sched->errstr;
}

# look for a schedule file in local dir
sub find_sched {
	say 'excel schedule file not specified, searching local directory...';
	opendir( my $dir, '.' );
	my @files = readdir($dir);
	closedir $dir;

	for (@files) {
		say "\tfound: $_" and return $_ if /^DailyCheckList.*xls$/i;
	}
	usage() and die "could not find a schedule spreadsheet\n";
}

sub usage {
	pod2usage( -verbose > 1 );
	print '
	usage: perl gen_db.pl
		-c config file specified
		-d create empty database framework
		-f schedule xls file	
		-h this message
';
}

=pod

=head1 NAME

TQASched - a tool for monitoring both legacy and DIS feed timeliness in AUH.

=head1 SYNOPSIS

perl tqa_sched.pl [optional flags]

=head1 DESCRIPTION

AUH content schedule monitoring tool
Creates and populates a database with content scheduling data from content scheduling spreadsheet
run daemon which reads AUH metadata and operator checklists to generate a feed timeliness report

=head1 OPTIONS

=over 6

=item B<-c>=I<configpath>

Specify path for config file in the command line.
Defaults to tqa_sched.pl and is generated automatically if not found.

=item B<-d>

Create a new TQASched database if not already present.

=item B<-h>

Print usage

=item B<-f>=I<schedulepath>

Specify path to Excel scheduling/checklist document 

=back

=head1 FILES

=over 6

=item F<tqa_sched.pl>

This self-documented script.
It is both a tool and a deamon - 
the only executable component of the TQASched application.
Refer to documentation for usage and configuration.

=item F<tqa_sched.conf>

A config file for the database credentials and other options. 
Path can be specified with -c flag.
Defaults to tqa_sched.pl and is generated automatically if not found.

=item F<DailyChecklist.xls>

Schedule checklist Excel spreadsheet.
This is used for either initializing the TQASched database
or for assigning the I<Filedate> and I<Filenum> of legacy content sets.
The syntax of this document is strict, see example: F<ExampleChecklist.xls>

=back

=head1 AUTHOR

Matt Shockley

=head1 COPYRIGHT AND LICENSE

Copyright 2012 Matt Shockley

This program is free software; you can redistribute it and/or modify 
it under the same terms as Perl itself.

=cut
