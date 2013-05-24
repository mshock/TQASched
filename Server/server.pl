#! perl -w

# TQASched web server

package WebServer;

use strict;
use feature 'say';

use lib '..';
use TQASched qw(:min);

$cfg = load_conf('..');

# redirect that STDERR if it's not going to the term
redirect_stderr( $cfg->server_log ) if caller;

# share ISA across scope to webserver
our @ISA;

use base qw(HTTP::Server::Simple::CGI);

# for statically hosted files (css, js, etc.)
use HTTP::Server::Simple::Static;

# create a new instance of server
my $server = WebServer->new( $cfg->port );

say 'hold your hats, the server is starting up its jets';

write_log( { logfile => $cfg->server_log,
			 type    => 'INFO',
			 msg     => 'server instance created, intiating hosting process',
		   }
);

# execute server process
$server->run();

#######################################################################
# point of no return - execution should never cross or server is dead
#######################################################################

# just in case server ever returns, write an error to log
write_log( { logfile => $cfg->server_log,
			 type    => 'ERROR',
			 msg     => 'server has returned and is no longer running'
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

		# static serve web directory for css, generated charts (later, ajax)
		if ( $cgi->path_info =~ m/\.(css|xls|js|ico|jpg|gif)/i ) {
			$self->serve_static( $cgi, 'Resources' );
			return;
		}
		elsif ( $cgi->path_info =~ m/\.(zip)/i ) {
			# fork a new process for hosting downloaded resources
			my $pid = fork();
			if ($pid == 0){
				$self->serve_static( $cgi, 'Files' );
				return;
			}
			return;
		}

		write_log( { logfile => $cfg->server_log,
					 type    => 'INFO',
					 msg     => "${\$cgi->remote_addr}\t$params_string"
				   }
		);

		# reload config file
		$cfg->file( '../' . $cfg->config_file )
			or warn "failed to reload config file $!";

		my $hosted_script
			= $cfg->maint_mode ? $cfg->maint_script : $cfg->report_script;
		print `perl $hosted_script $params_string`;

}
