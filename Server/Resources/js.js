/*
custom javascript goes in this file
it is automatically included in the portal.pl content generation script

Note concerning JQuery:
due to overhead involved with loading the JQuery library, it is not automatically linked
to utilize all of JQuery's wonderful magic add the following to <head> block:
<script src="jquery.js" type="text/javascript">
*/

// test function for ensuring new JS is error-free
function test_js() 
{
	alert("User Javascript file (js/js.js) interpreted OK");
}

// dynamic countdown on page
var milisec=0
var seconds=10
document.countdown.digits.value='10'

function display(){
	if (milisec<=0){
	milisec=9
	seconds-=1
	}
	if (seconds<=-1){
	milisec=0
	seconds+=1
	}
	else
	milisec-=1
	document.countdown.digits.value=seconds+"."+…
	setTimeout("display()",100)
}
display();