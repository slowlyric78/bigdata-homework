my $regex, $log1, $log2;

$regex = '(.*?)\s.*\[(.*)\].*\d\d\d\s([0-9\-]+)\s\".*\"\s\"(.*)\"';

$log1 = 'ip43 - - [24/Apr/2011:06:37:14 -0400] "GET /faq/ HTTP/1.1" 200 11077 "-" "Java/1.6.0_04"';
$log2 = 'ip13 - - [25/Apr/2011:02:01:18 -0400] "GET /sun3/clockchip/ HTTP/1.1" 304 - "-" "Mozilla/5.0 (compatible; Googlebot/2.1; +http://www.google.com/bot.html)"';



my $mystring;
# Assign a value (string literal) to the variable.

if ($log1 =~ $regex) {
    print "$1 : $2 : $3 : $4\n";
} else { print "log1 is not good\n"; }

if ($log2 =~ $reged) {
    print "$1 : $2 : $3 : $4\n";
} else { print "log2 is not good\n"; }


# $mystring = "Hello world!";
# Does the string contains the word "World"?

# if($mystring =~ m/World/) { print "Yes"; } else { print "No"; }
# print "\n";