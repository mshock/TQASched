#! perl -w

# called by the http server
# generates HTML response containing report

use strict;
use Getopt::Long qw(GetOptions);

# all possible POST parameters
my ($test);

# get POST params
# parsed from CLI arg key/value pairs
GetOptions(
	'test=s' => \$test,
);

print "HTTP/1.0 200 OK\r\n";
print "Content-type: text/html\n\n";

print $test;