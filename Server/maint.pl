#! perl -w

use strict;

# default page
# use to host notifications when report is offline

######################################################
#	NOTICE POSTED:
#		anything to STDOUT before this point will
#		botch HTTP communication protocol
######################################################

print "HTTP/1.0 200 OK\r\n";
print "Content-type: text/html\n\n";

######################################################
#	STDOUT = html content
#	anything else will be printed to the page
#	(useful for debug)
######################################################

print "
<html>
	<head>
		<title>Server Maint</title>
		<meta http-equiv='refresh' content='300' >
		<link rel='stylesheet' type='text/css' href='styles.css' />
		<script type='text/javascript'>
			
			window.onload = function() {
				/* set your parameters(
				number to countdown from,
				pause between counts in milliseconds,
				function to execute when finished
				)
				*/
				startCountDown(99, 1000, myFunction);
			}

			function startCountDown(i, p, f) {
				// store parameters
				var pause = p;
				var fn = f;
				// make reference to div
				var countDownObj = document.getElementById('countDown');
				if (countDownObj == null) {
					// error
					alert('div not found, check your id');
					// bail
					return;
				}
				
				var boxdark = {r: 35, g: 225, b: 30};
				
				countDownObj.count = function(i) {
				// write out count
				countDownObj.innerHTML = i;
				/*
				var rgbstring = 'rgb(';
				var bgstring = rgbstring.concat(
					(Math.round(Math.min(boxdark['r']*1.25, 255))).toString(), ',',
             		(Math.round(Math.min(boxdark['g']*1.25, 255))).toString(), ',',
             		(Math.round(Math.min(boxdark['b']*1.25, 255))).toString(),
					')'
				);
				countDownObj.style.backgroundColor = bgstring;
				*/
				if (i == 0) {
					// execute function
					fn();
					// stop
					return;
				}
				setTimeout(function() {
						// repeat
						countDownObj.count(i - 1);
						},
					pause
				);
				}
				// set it going
				countDownObj.count(i);
			}

			function myFunction() {
				location.reload()
			}
			</script>
	</head>
	<body style='text-align:center;'>
		<br />
		<h2 style='color: red;'>TQASched report is down for maintenance</h2>
		<br />
		<hr />
		<h3>this page will refresh automatically until maintenance has finished</h3>
		<hr />
		<div id='countDown' style='border:1px solid darkgray; padding: 10; margin-top: 7; display:inline-block; font-weight:bold; background:#23E11E;' ></div>
	</body>
</html>

";