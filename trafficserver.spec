%global tag %(git describe --long |      sed 's/^\\\(.*\\\)-\\\([0-9]\\\+\\\)-g\\\([0-9a-f]\\\+\\\)$/\\\1/' | sed 's/-/_/')
%global distance %(git describe --long | sed 's/^\\\(.*\\\)-\\\([0-9]\\\+\\\)-g\\\([0-9a-f]\\\+\\\)$/\\\2/')
%global commit %(git describe --long |   sed 's/^\\\(.*\\\)-\\\([0-9]\\\+\\\)-g\\\([0-9a-f]\\\+\\\)$/\\\3/')
%global git_serial %(git rev-list HEAD | wc -l)
%global install_prefix "/opt"
%global api_stats "4096"
%global _find_debuginfo_dwz_opts %{nil}

Name:		trafficserver
Version:	%{tag}
Epoch:		%{git_serial}
Release:	%{distance}.%{commit}%{?dist}
Summary:	Apache Traffic Server
#Packager:	Jeffrey_Elsloo at Cable dot Comcast dot com
Vendor:		IPCDN
Group:		Applications/Communications
License:	Apache License, Version 2.0
URL:		https://gitlab.sys.comcast.net/cdneng/apache/tree/master/trafficserver
BuildRoot:	%(mktemp -ud %{_tmppath}/%{name}-%{version}-%{release}-XXXXXX)
Requires:	tcl, hwloc, pcre, openssl, libcap
BuildRequires:	autoconf, automake, libtool, gcc-c++, glibc-devel, openssl-devel, expat-devel, pcre, libcap-devel, pcre-devel, perl-ExtUtils-MakeMaker, tcl-devel, hwloc-devel

%description
Apache Traffic Server with Comcast modifications and environment specific modifications

%prep
rm -rf %{name}
git clone git@github.comcast.com:cdneng/trafficserver.git %{name}
%setup -D -n %{name} -T
git checkout build-master-6.2.x
git checkout %{commit} .
autoreconf -vfi

%build
./configure --prefix=%{install_prefix}/%{name} --with-user=ats --with-group=ats --with-build-number=%{release} --enable-experimental-plugins --with-max-api-stats=%{api_stats}
make %{?_smp_mflags}

%install
make DESTDIR=$RPM_BUILD_ROOT install
# WARNING!  Don't build a RPM on a 'real' (ats server) box
# Totally ghetto, but ATS build scripts aren't RPM (DESTDIR=$RPM_BUILD_ROOT, etc) compliant
# ..so why haven't we fixed them? VSSCDNENG-767

mkdir -p $RPM_BUILD_ROOT/opt/trafficserver/etc/trafficserver/snapshots
mkdir -p $RPM_BUILD_ROOT/opt/trafficserver/rc
cp $RPM_BUILD_DIR/%{name}/rc/trafficserver $RPM_BUILD_ROOT/opt/trafficserver/rc/
cp $RPM_BUILD_DIR/%{name}/rc/trafficserver.service $RPM_BUILD_ROOT/opt/trafficserver/rc/
cp $RPM_BUILD_DIR/%{name}/tools/rc_admin.pl $RPM_BUILD_ROOT/opt/trafficserver/rc/

%clean
rm -rf $RPM_BUILD_ROOT

%pre
id ats &>/dev/null || /usr/sbin/useradd -u 176 -r ats -s /sbin/nologin -d /

%post
/opt/trafficserver/rc/rc_admin.pl post-install install /opt/trafficserver

%preun
# if 0 uninstall, if 1 upgrade
if [ "$1" = "0" ]; then
	/opt/trafficserver/rc/rc_admin.pl pre-uninstall uninstall /opt/trafficserver
elif [ "$1" = "1" ]; then
	/opt/trafficserver/rc/rc_admin.pl pre-uninstall upgrade /opt/trafficserver
fi

%postun
# Helpful in understanding order of operations in relation to install/uninstall/upgrade:
#     http://www.ibm.com/developerworks/library/l-rpm2/
# if 0 uninstall, if 1 upgrade
if [ "$1" = "0" ]; then
	id ats &>/dev/null && /usr/sbin/userdel ats
fi

%files
%defattr(-,root,root)
%dir /opt/trafficserver
/opt/trafficserver/bin
/opt/trafficserver/include
/opt/trafficserver/lib
/opt/trafficserver/man
/opt/trafficserver/libexec
/opt/trafficserver/share
/opt/trafficserver/rc
%dir /opt/trafficserver/var
%attr(-,ats,ats) /opt/trafficserver/var/trafficserver
%dir /opt/trafficserver/var/log
%attr(-,ats,ats) /opt/trafficserver/var/log/trafficserver
%dir /opt/trafficserver/etc
%attr(-,ats,ats) %dir /opt/trafficserver/etc/trafficserver
%attr(-,ats,ats) %dir /opt/trafficserver/etc/trafficserver/snapshots
/opt/trafficserver/etc/trafficserver/body_factory
/opt/trafficserver/etc/trafficserver/trafficserver-release
%config(noreplace) %attr(644,ats,ats) /opt/trafficserver/etc/trafficserver/cache.config
%config(noreplace) %attr(644,ats,ats) /opt/trafficserver/etc/trafficserver/cluster.config
%config(noreplace) %attr(644,ats,ats) /opt/trafficserver/etc/trafficserver/congestion.config
%config(noreplace) %attr(644,ats,ats) /opt/trafficserver/etc/trafficserver/hosting.config
%config(noreplace) %attr(644,ats,ats) /opt/trafficserver/etc/trafficserver/icp.config
%config(noreplace) %attr(644,ats,ats) /opt/trafficserver/etc/trafficserver/ip_allow.config
%config(noreplace) %attr(644,ats,ats) /opt/trafficserver/etc/trafficserver/log_hosts.config
%config(noreplace) %attr(644,ats,ats) /opt/trafficserver/etc/trafficserver/logs_xml.config
%config(noreplace) %attr(644,ats,ats) /opt/trafficserver/etc/trafficserver/metrics.config
%config(noreplace) %attr(644,ats,ats) /opt/trafficserver/etc/trafficserver/parent.config
%config(noreplace) %attr(644,ats,ats) /opt/trafficserver/etc/trafficserver/plugin.config
%config(noreplace) %attr(644,ats,ats) /opt/trafficserver/etc/trafficserver/records.config
%config(noreplace) %attr(644,ats,ats) /opt/trafficserver/etc/trafficserver/remap.config
%config(noreplace) %attr(644,ats,ats) /opt/trafficserver/etc/trafficserver/socks.config
%config(noreplace) %attr(644,ats,ats) /opt/trafficserver/etc/trafficserver/splitdns.config
%config(noreplace) %attr(644,ats,ats) /opt/trafficserver/etc/trafficserver/ssl_multicert.config
%config(noreplace) %attr(644,ats,ats) /opt/trafficserver/etc/trafficserver/storage.config
%config(noreplace) %attr(644,ats,ats) /opt/trafficserver/etc/trafficserver/vaddrs.config
%config(noreplace) %attr(644,ats,ats) /opt/trafficserver/etc/trafficserver/volume.config
%config(noreplace) %attr(644,ats,ats) /opt/trafficserver/etc/trafficserver/stats.config.xml

%changelog
* Wed Jun 8 2016 John Rushford <john_rushford(at)cable.comcast.com>
- Added tools/rc_admin.pl to complete rpm tasks under both Enterprise Linux 6 or 7 using either chkconfig or systemd commands.
- Modified this spec file to use rc_admin.pl
* Wed Aug 7 2013 Jeff Elsloo <jeffrey_elsloo(at)cable.comcast.com>
- Modified to support building 3.3.x
- Modified to support upgrades
* Sun Aug 12 2012 John Benton <john_benton(at)cable.comcast.com>
- Initial RPM build based on SVN version 2376
- Rev for ATS 3.2.0 based on SVN version 2470
- Rev for ATS 3.2.0 based on SVN version 2555
- Rev for ATS 3.2.0 based on SVN version 4812
