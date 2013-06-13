#! perl -w

# query all tables for upd rows

use strict;
use feature 'say';
use DBI;

use lib '..';
use TQASched;

$cfg = load_conf('..');

my $upd = $cfg->xupd;
my $table = $cfg->xtable;

my ( $fd, $fn ) = $upd =~ m/^(\d+)-(\d+)$/;

my $filename = '';

my @tables = ();
my @errors = ();
my %codes;
my ($dbh_cdb ) = refresh_handles(  'change' );
my $tmp_fh;
open $tmp_fh , '>', 'Files/temp.upd' or die $!;

if ($table) {
	push @tables, $table;
}
else {
	my $tnames_aref = $dbh_cdb->selectall_arrayref("
		select distinct tablename, status from ChangeDB_current.dbo.update_log
		where filedate = '$fd'
		and filenum = $fn");
	for my $tname_aref (@$tnames_aref) {
		my ($tname, $status) =  @$tname_aref;
		
		my ($headercode) = $dbh_cdb->selectrow_array("
			select updcode from ChangeDB_current.dbo.tableinfo 
			where name = UPPER('$tname')");
		$codes{$tname} = $headercode;
		if (defined $status && $status == 0) {
			push @tables, $tname;
		}		
		else {
			push @errors, $tname;
		}				
	}

}

say $tmp_fh "ChangeDB Dump: $upd";

for my $tname (@tables) {
	
	my $select_upd_query = "
		select * from changedb_current.dbo.$tname
		where 
		FileDate_ = '$fd'
		and FileNum_ = $fn
		order by RowNum_ asc
	";
	#warn $select_upd_query;
	my $cdb_sth = $dbh_cdb->prepare($select_upd_query) or die $select_upd_query;
	$cdb_sth->{'LongReadLen'} = 20000;
	$cdb_sth->execute();

	my $headercode = $codes{$tname};
	
	say $tmp_fh "\n[$headercode]\t->\t$tname"; 
	
	
	while (my @upd_row = $cdb_sth->fetchrow_array()) {
	
		my @output_row;
		my ( $filedate, $filenum, $rownum, @upd_row ) = @upd_row;
		for my $val (@upd_row) {
			$val = defined $val ? $val : '';
			$val =~ s/[^[:ascii:]]+//g;
			push @output_row, $val;
		}

		say $tmp_fh join "\t", @output_row;
	}
}

close $tmp_fh;

