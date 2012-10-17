#! perl -w

use strict;
use feature qw(say switch);
use Getopt::Std qw(getopts);
use Spreadsheet::ParseExcel;
use Spreadsheet::ParseExcel::Utility qw(ExcelFmt);
use Config::Simple;
use DBI;

my %opts;
getopts( 'c:df:h', \%opts );

( usage() and exit ) if $opts{h};

my $conf_file = $opts{c} || 'tqa_sched.conf';

say 'parsing config file...';

# load config file
my ( $db, %conf ) = load_conf($conf_file);

# get excel file containing schedule info
my $sched_xls = $opts{f} || $conf{schedule_file} || find_sched();

# create parser and parse xls
my $xlsparser = Spreadsheet::ParseExcel->new();
my $workbook  = $xlsparser->parse($sched_xls)
  or die "unable to parse spreadsheet: $sched_xls\n", $xlsparser->error();
say 'done';

say 'initializing database handle...';

# initialize database handle
my $dbh = init_handle($db);
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
	say "\tunable to parse weekday, skipping" and next if $weekday_code eq 'U';

	# find the row and column bounds for iteration
	my ( $col_min, $col_max ) = $worksheet->col_range();
	my ( $row_min, $row_max ) = $worksheet->row_range();

	# iterate over each row and store scheduling data
	for ( my $row = $row_min ; $row <= $row_max ; $row++ ) {
		next if $row <= 1;

		# per-update hash of column values
		my $row_data = {};
		for ( my $col = $col_min ; $col <= $col_max ; $col++ ) {
			my $cell = $worksheet->get_cell( $row, $col );
			last unless extract_row( $col, $cell, $row_data );

		}
		# skip rows that have no values, degenerates (ha)
		next unless %{$row_data};
		# attempt to store rows that had values
		store_row( $weekday_code, $row_data )
		  or warn "failed to store row $row for $weekday\n";
	}
}

####################################################################################
#	subs
#
####################################################################################

# TODO: need an is_legacy checkbox
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

		# update
		when (/^1$/) {

			# skip blank update rows
			$row_href->{update} = $value ? $value : return;
		}

		# priority
		when (/^2$/) {
			$row_href->{priority} = $value;
		}

		# file date
		when (/^3$/) {

			# extract unformatted datetime and convert to filedate integer
			my $time_excel = $cell->unformatted();
			my $value = ExcelFmt( 'yyyymmdd', $time_excel );

			# skip if not scheduled for this day
			$row_href->{filedate} = $value ? $value : return;
		}

		# file number
		when (/^4$/) {
			$row_href->{filenum} = $value ? $value : return;
		}

		# ID - 'x' if not scheduled for the day
		when (/^5$/) {
			$row_href->{id} = $value;
		}

		# comment
		when (/^6$/) {
			$row_href->{comment} = $value;
		}
		# outside of parsing scope
		# return and go to next row
		default { return };
	}
	return 1;
}

# analyze and store a row from scheduling spreadsheet in database
sub store_row {
	my $weekday_code = shift;
	my $row_href     = shift;

	# don't store row if not scheduled for today
	# but not an error so return true
	return 1
	  unless $row_href->{filedate}
		  && $row_href->{update}
		  && $row_href->{time_block}
		  && !$row_href->{id};

	# check if this update name has been seen before
	my $update_id;
	unless ( $update_id = get_update_id( $row_href->{update} ) ) {

		# if not, insert it into the database
		my $update_insert = "insert into [TQASched].dbo.[Updates] values 
			('$row_href->{update}','$row_href->{priority}', '$row_href->{is_legacy}')";
		$dbh->do($update_insert)
		  or warn
"error inserting update: $row_href->{update}, probably already inserted\n",
		  $dbh->errstr
		  and return;

		# get the id of the new update
		$update_id = $dbh->last_insert_id( undef, undef, 'Updates', undef )
		  or warn "could not retrieve last insert id\n", $dbh->errstr
		  and return;
	}

	# put entry in scheduling table
	my $time_offset  = time2offset( $row_href->{time_block} );
	my $sched_insert = "
		insert into [TQASched].dbo.[Update_Schedule] values 
			('$update_id','$weekday_code','$time_offset')
	";
	$dbh->do($sched_insert)
	  or warn
	  "failed to insert update schedule info for update: $row_href->{update}\n",
	  $dbh->errstr
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
	return ( ( $dbh->selectall_arrayref($select_query) )->[0] )->[0];
}

# load db info and optional params from config file
sub load_conf {
	my $conf_file = shift;

	# if file doesn't exist, create
	unless ( -f $conf_file ) {
		die "could not load config file: $conf_file, ",
		  (
			init_conf()
			? 'a skeleton config has been created'
			: 'failed to create skeleton config'
		  ),
		  "\n";
	}

	my $cfg = new Config::Simple($conf_file);

	my $db = $cfg->param( -block => 'db' )
	  or die
	  "could not load database info from config file, check config file\n";
	my $opts = $cfg->param( -block => 'opts' )
	  or warn "could not load optional configs from config file\n"
	  and return ( $db, {} );

	return ( $db, %{$opts} );
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
[opts]';
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
	$dbh->do("create database [TQASched]")
	  or die "could not create TQASched database\n";

	# create the tables
	$dbh->do(
		"create table [TQASched].dbo.[Updates] (
		update_id int not null identity(1,1),
		name varchar(255) not null unique,
		priority tinyint,
		is_legacy bit
	)"
	) or die "could not create Updates table\n", $dbh->errstr;
	$dbh->do(
		"create table [TQASched].dbo.[Update_Schedule] (
		sched_id int not null identity(1,1),
		update_id int not null,
		weekday char(1),
		time int
	)"
	) or die "could not create Update_Schedule table\n", $dbh->errstr;
	say 'done';
	return 1;
}

# check that database exists
sub check_db {
	my $check_query = "select db_id('TQASched')";
	return ( ( $dbh->selectall_arrayref($check_query) )->[0] )->[0];
}

# drop the database
sub drop_db {
	return $dbh->do('drop database TQASched')
	  or die "could not drop TQASched database\n", $dbh->errstr;
}

# clear all update records in database
sub clear_updates {
	return $dbh->do('delete from [TQASched].dbo.[Updates]')
	  or die "error in clearing Updates table\n", $dbh->errstr;
}

# clear all scheduling records in database
sub clear_schedule {
	return $dbh->do('delete from [TQASched].dbo.[Update_Schedule]')
	  or die "error in clearing Schedule table\n", $dbh->errstr;
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