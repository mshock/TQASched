#! perl -w

# TQASched web server

package WebServer;

use strict;
use feature 'say';
use lib '..';
use TQASched;

my $cfg = TQASched::load_conf();

# share ISA across scope to webserver
our @ISA;

use base qw(HTTP::Server::Simple::CGI);

# for statically hosted files (css, js, etc.)
use HTTP::Server::Simple::Static;

# create a new instance of server
my $server = WebServer->new( $cfg->port );

say 'server is starting up its jets';

# execute server process
$server->run();

# just in case server ever returns
write_log( { type => 'ERROR',
					   msg  => 'server has returned and is no longer running'
					 }
);

# override request handler for HTTP::Server::Simple
sub handle_request {
	my ( $self, $cgi ) = @_;

	# parse POST into CLI argument key/value pairs
	# TODO: use AppConfig's CGI parser
	my $params_string = '';
	for ( $cgi->param ) {
		$params_string .= sprintf( '--%s="%s" ', $_, $cgi->param($_) )
			if defined $cgi->param($_);
	}

	# static serve web directory for css, charts (later, ajax)
	if ( $cgi->path_info =~ m/\.(css|xls|js|ico)/ ) {
		$self->serve_static( $cgi, '../Resources' );
		return;
	}

	print `perl ${\$cfg->target_script} $params_string`;
}
