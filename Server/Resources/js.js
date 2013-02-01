/*
custom javascript goes in this file
it is automatically included in the portal.pl content generation script

Note concerning JQuery:
due to overhead involved with loading the JQuery library, it is not automatically linked
to utilize all of JQuery's wonderful magic add the following to <head> block:
<script src="jquery.js" type="text/javascript">
*/

// test function for ensuring new JS is error-free
function Test_JS() 
{
	alert("User Javascript file (js/js.js) interpreted OK");
}