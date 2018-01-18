#!/usr/bin/perl

#  Licensed to the Apache Software Foundation (ASF) under one
#  or more contributor license agreements.  See the NOTICE file
#  distributed with this work for additional information
#  regarding copyright ownership.  The ASF licenses this file
#  to you under the Apache License, Version 2.0 (the
#  "License"); you may not use this file except in compliance
#  with the License.  You may obtain a copy of the License at
#
#      http://www.apache.org/licenses/LICENSE-2.0
#
#  Unless required by applicable law or agreed to in writing, software
#  distributed under the License is distributed on an "AS IS" BASIS,
#  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
#  See the License for the specific language governing permissions and
#  limitations under the License.

use Digest::SHA qw(hmac_sha1 hmac_sha1_hex);
use Digest::HMAC_MD5 qw(hmac_md5 hmac_md5_hex);
use Getopt::Long;
use MIME::Base64;

use strict;
use warnings;
my $key       = undef;
my $useparts  = undef;
my $duration  = undef;
my $keyindex  = undef;
my $verbose   = 0;
my $url       = undef;
my $client    = undef;
my $algorithm = 1;
my $pathparams = 0;
my $sig_anchor = undef;
my $proxy = undef;

my $result = GetOptions(
	"url=s"       => \$url,
	"useparts=s"  => \$useparts,
	"duration=i"  => \$duration,
	"key=s"       => \$key,
	"client=s"    => \$client,
	"algorithm=i" => \$algorithm,
	"keyindex=i"  => \$keyindex,
	"verbose"     => \$verbose,
	"pathparams"  => \$pathparams,
    "proxy=s"     => \$proxy,
    "siganchor=s"  => \$sig_anchor
);

if ( !defined($key) || !defined($url) || !defined($duration) || !defined($keyindex) ) {
	&help();
	exit(1);
}

if ( $proxy && $proxy !~ m{http://.*:\d\d}) {
  &help();
  exit(1);
}

$proxy ||= "";

my (@sigparams);

push(@sigparams,"C=$client") if ( defined($client) );
push(@sigparams,"E=" . ( time() + $duration ));
push(@sigparams,"A=$algorithm");
push(@sigparams,"K=$keyindex");
push(@sigparams,"P=$useparts");
push(@sigparams,"S=");

$url =~ s{^https?://}{};

my $scheme = $&;

# split path for hash determine hash
my @pathparts = (split(m{/}, $url));
my ($lastpath,$querybase) = split(m{\?}, $pathparts[$#pathparts]);
my $queryadd;
my $digest;

$querybase ||= "";
$queryadd ||= "";

if ( $pathparams && ! @pathparts ) {
    print STDERR "\nERROR: No file segment in the path when using --pathparams.\n\n";
    &help();
    exit(1);
}

my $sigstring;

if ( $pathparams ) {
  $sigstring = $pathparams = ";" . join(";",@sigparams);
} elsif ( $querybase ) {
  $queryadd = '&' . join("&",@sigparams);
  $sigstring = "?$querybase" . $queryadd;
} else {
  $sigstring = $queryadd = "?" . join("&",@sigparams);
}

#
# construct string...
#

my @bits = split(m//,$useparts);
my $string;

if ( $pathparams ) {
  # never use last element given pathparams
  $string = join("/", grep { (@bits > 1 ? (shift @bits) : $bits[0]); } @pathparts[0..$#pathparts-1]) . $pathparams;
} else {
  $string = join("/", grep { (@bits > 1 ? (shift @bits) : $bits[0]); } @pathparts) . $queryadd;
  # re-add query if it was excluded
  $string .= "?$querybase" 
     if ( ! $bits[0] && $querybase );
}

$digest = hmac_sha1_hex( $string, $key ) 
   if ( $algorithm == 1 );
$digest = hmac_md5_hex( $string, $key ) 
   if ( $algorithm != 1 );

$verbose && print "\nSigned String: $string\n\n";
$verbose && print "\nUrl: $url\n";
$verbose && print "\nsigning_signature $sigstring\n";
$verbose && print "\ndigest: $digest\n";

# RFC 3986 URL definitions:
#    pchar         = unreserved / pct-encoded / sub-delims / ":" / "@"
#    unreserved    = ALPHA / DIGIT / "-" / "." / "_" / "~"
#    sub-delims    = "!" / "$" / "&" / "'" / "(" / ")" / "*" / "+" / "," / ";" / "="
#
# of non-alphanumerics...
#    allowed: @ - . _ ~ ! $ & ' ( ) * + , ; : = 
#    escaped: \\ " # ` / ? < > { } [ ] %
#
if ( $pathparams ) {
#  $pathparams =~ s{[\\"#`/\?<>\{\}\[\]]}{ "%".hex(ord($_)) }ex;
  $pathparams = MIME::Base64::encode($pathparams . $digest,"");
  $pathparams =~ y{=}{}d;
  $pathparams =~ y{+/}{-_};
  $pathparams = ( $sig_anchor ? ";$sig_anchor=$pathparams" : "/$pathparams" );
  $pathparts[-2] .= $pathparams;
  $url = join("/",@pathparts);
}

$queryadd && ($queryadd .= $digest);

$proxy = "--proxy $proxy " if ($proxy);

print "curl -s -o /dev/null -v --max-redirs 0 $proxy'$scheme$url$queryadd'\n\n";

sub help {
	print "sign.pl - Example signing utility in perl for signed URLs\n";
	print "Usage: \n";
	print "  ./sign.pl  --url <value> \\ \n";
	print "             --useparts <value> \\ \n";
	print "             --algorithm <value> \\ \n";
	print "             --duration <value> \\ \n";
	print "             --keyindex <value> \\ \n";
	print "             [--client <value>] \\ \n";
	print "             --key <value>  \\ \n";
	print "             [--verbose] \n";
	print "             [--pathparams] \n";
	print "             [--proxy <url:port value>] ex value: http://myproxy:80\n";
	print "\n";
}
