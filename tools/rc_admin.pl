#!/usr/bin/env perl
#
#
use strict;
use warnings;

use File::Copy;

my %el_versions = ( "EL6" => 1, "EL7" => 1);
my %modes = ("install" => 1, "upgrade" => 1, "uninstall" => 1);
my %phases = ("pre-uninstall" => 1, "post-install" => 1);
my $installdir; 
my $mode;
my $phase;

# returns the enterprise linux version.
sub el_version {
	my @el_version = split(/\./, `/usr/bin/uname -r`);
	exists $el_versions{uc $el_version[3]} ? return uc $el_version[3] 
		: die("unsupported el_version: $el_version[3]");
}

sub usage {
	print STDERR "\nUsage: rc_admin.pl phase mode install_directory\n\n";
	print "\tvalid phase arguments:\n";
	print "\t\t'pre-uninstall'  - pre uninstall phase\n";
	print "\t\t'post-install'   - post install phase\n";

	print "\tvalid mode arguments:\n";
	print "\t\t'install' or 'uninstall'\n\n";
	exit 1;
}

if ( $#ARGV < 2 || !(exists $phases{$ARGV[0]}) || !(exists $modes{$ARGV[1]})) {
        &usage();
} else {
	$phase = $ARGV[0];
	$mode = $ARGV[1];
	$installdir = $ARGV[2];
}

my $EL_VERSION = el_version();

if (-d "$installdir/rc") {
	chdir("$installdir/rc");
} else {
	die("no such directory $installdir/rc");
}

if ($phase eq "post-install") {
	if ($EL_VERSION eq "EL6" && $mode eq "install") {
		print "installing /etc/init.d/trafficserver.\n";
		`/bin/cp trafficserver /etc/init.d/`;			
		if ($? != 0) {
			print "Failed to copy trafficsever to /etc/init.d/ : $!";
		} else {
			chmod(0755, "/etc/int.d/trafficserver");
		}
		`/sbin/chkconfig --add trafficserver`;
		if ($? != 0) {
			print "Failed running /sbin/chkconfig --add  trafficsever";
		} 

	} elsif ("$EL_VERSION" eq "EL7" && $mode eq "install") {
		print "installing /usr/lib/systemd/system/trafficserver.service.\n";
		`/bin/cp trafficserver.service /usr/lib/systemd/system/`;			
		if ($? != 0) {
			print "Failed to copy trafficsever.service to /usr/lib/systemd/system: $!";
		} else {
			chmod(0644, "/usr/lib/systemd/system/trafficserver.service");
		}
		`/bin/systemctl daemon-reload`;
		if ($? != 0) {
			print "Failed running /bin/systemctl daemon-reload";
		} 
		`/bin/systemctl enable trafficserver`;
		if ($? != 0) {
			print "Failed running /bin/systemctl enable  trafficsever";
		} 
	}
} elsif ($phase eq "pre-uninstall") {
	print "Shutting down trafficserver.\n";
	`/sbin/service trafficserver stop`;
	if ($? != 0) {
		print "Failed running /sbin/service trafficsever stop\n";
	}  else {
		print "trafficserver has stopped.\n";
	}

	if ($mode eq "uninstall") {
		print "disabling trafficserver.\n";
		if ($EL_VERSION eq "EL6") {
			`/sbin/chkconfig --del trafficserver`;
			if ($? != 0) {
				print "Failed running /sbin/chkconfig --del trafficsever";
			}  else {
				print "trafficserver has been disabled.\n";
			}
			unlink("/etc/init.d/trafficserver");
		} elsif ($EL_VERSION eq "EL7") {
			`/bin/systemctl disable trafficserver`;
			if ($? != 0) {
				print "Failed running /sbin/systemctl disable trafficsever";
			}  else {
				print "trafficserver has been disabled.\n";
			}
			unlink ("/usr/lib/systemd/system/trafficserver.service");
			`/bin/systemctl daemon-reload`;
			if ($? != 0) {
				print "Failed running /bin/systemctl daemon-reload";
			} 
		}
	}
}

exit 0;
