%global install_prefix "/opt"
Name:		trafficserver
Version:	4.2.1
Release:	6%{?dist}
Summary:	Apache Traffic Server
Packager:	Jeffrey_Elsloo at Cable dot Comcast dot com
Vendor:		Comcast NETO
Group:		Applications/Communications
License:	Apache License, Version 2.0
URL:		https://gitlab.sys.comcast.net/cdneng/apache/tree/master/trafficserver
Source0:	%{name}-%{version}.tar.bz2
Patch0:		ats-4.0.1-remove-cache-url.patch
Patch1:		ats-4.2.1-consistent-parent.patch
Patch2:		ats-4.0.2-parent-selection-on-top-of-consistent-parent.patch
Patch3:		4.2.1-fixup-ram-cache.patch
Patch4:		ats-4.2.1-synthetic-fix.patch
BuildRoot:	%(mktemp -ud %{_tmppath}/%{name}-%{version}-%{release}-XXXXXX)
Requires:	tcl, tcl-devel, boost
BuildRequires:	autoconf, automake, libtool, gcc-c++, glibc-devel, openssl-devel, expat-devel, pcre, libcap-devel, pcre-devel, perl-ExtUtils-MakeMaker, boost-devel

%description
Apache Traffic Server with Comcast modifications and environment specific modifications

%prep
%setup -q -n %{name}-%{version}
id ats &>/dev/null || /usr/sbin/useradd -u 176 -r ats -s /sbin/nologin -d /

%patch0 -p1
%patch1 -p1
%patch2 -p1
%patch3 -p1
%patch4 -p1

%build
autoreconf -vi
./configure --prefix=%{install_prefix}/%{name} --with-user=ats --with-group=ats --disable-hwloc
make %{?_smp_mflags}

%install
make DESTDIR=$RPM_BUILD_ROOT install
# WARNING!  Don't build a RPM on a 'real' (ats server) box
# Totally ghetto, but ATS build scripts aren't RPM (DESTDIR=$RPM_BUILD_ROOT, etc) compliant
# ..so why haven't we fixed them? VSSCDNENG-767

# added to make this hack actually work if you need to build more than once.. -jse
#if [ -d "$RPM_BUILD_ROOT/%{install_prefix}" ]; then
#	rm -rf $RPM_BUILD_ROOT/%{install_prefix}
#fi

#mkdir -p $RPM_BUILD_ROOT/%{install_prefix}
#mv %{install_prefix}/%{name} $RPM_BUILD_ROOT/%{install_prefix}/

mkdir -p $RPM_BUILD_ROOT/etc/init.d
cp $RPM_BUILD_DIR/%{name}-%{version}/rc/trafficserver $RPM_BUILD_ROOT/etc/init.d/
chmod 755 $RPM_BUILD_ROOT/etc/init.d/trafficserver

%clean
rm -rf $RPM_BUILD_ROOT

%pre
#echo "This is the pre install section"
id ats &>/dev/null || /usr/sbin/useradd -u 176 -r ats -s /sbin/nologin -d /

%post
chkconfig --add trafficserver

%preun
if [ -e /opt/trafficserver/var/trafficserver/server.lock ] || [ -e /opt/trafficserver/var/trafficserver/cop.lock ] || [ -e /opt/trafficserver/var/trafficserver/manager.lock ]
then
  echo "trafficserver still running or lock files exit in /opt/trafficserver/var/trafficserver/. Please stop before uninstalling."
  exit 1
fi

%postun
# Helpful in understanding order of operations in relation to install/uninstall/upgrade:
#     http://www.ibm.com/developerworks/library/l-rpm2/
# if 0 uninstall, if 1 upgrade
if [ "$1" = "0" ]; then
	id ats &>/dev/null && /usr/sbin/userdel ats
	echo "Successfully uninstalled trafficserver..."
fi

%files
%defattr(-,root,root)
/etc/init.d/trafficserver
%dir /opt/trafficserver
%dir /opt/trafficserver/etc
/opt/trafficserver/bin
/opt/trafficserver/include
/opt/trafficserver/lib
/opt/trafficserver/lib64
/opt/trafficserver/libexec
/opt/trafficserver/share
/opt/trafficserver/etc/trafficserver/body_factory
/opt/trafficserver/etc/trafficserver/trafficserver-release
%attr(-,ats,ats) /opt/trafficserver/var
%attr(-,ats,ats) %dir /opt/trafficserver/etc/trafficserver
%config(noreplace) %attr(644,ats,ats) /opt/trafficserver/etc/trafficserver/cache.config
%config(noreplace) %attr(644,ats,ats) /opt/trafficserver/etc/trafficserver/cluster.config
%config(noreplace) %attr(644,ats,ats) /opt/trafficserver/etc/trafficserver/congestion.config
%config(noreplace) %attr(644,ats,ats) /opt/trafficserver/etc/trafficserver/hosting.config
%config(noreplace) %attr(644,ats,ats) /opt/trafficserver/etc/trafficserver/icp.config
%config(noreplace) %attr(644,ats,ats) /opt/trafficserver/etc/trafficserver/ip_allow.config
%config(noreplace) %attr(644,ats,ats) /opt/trafficserver/etc/trafficserver/log_hosts.config
%config(noreplace) %attr(644,ats,ats) /opt/trafficserver/etc/trafficserver/logs_xml.config
%config(noreplace) %attr(644,ats,ats) /opt/trafficserver/etc/trafficserver/parent.config
%config(noreplace) %attr(644,ats,ats) /opt/trafficserver/etc/trafficserver/plugin.config
%config(noreplace) %attr(644,ats,ats) /opt/trafficserver/etc/trafficserver/records.config
%config(noreplace) %attr(644,ats,ats) /opt/trafficserver/etc/trafficserver/remap.config
%config(noreplace) %attr(644,ats,ats) /opt/trafficserver/etc/trafficserver/socks.config
%config(noreplace) %attr(644,ats,ats) /opt/trafficserver/etc/trafficserver/splitdns.config
%config(noreplace) %attr(644,ats,ats) /opt/trafficserver/etc/trafficserver/ssl_multicert.config
%config(noreplace) %attr(644,ats,ats) /opt/trafficserver/etc/trafficserver/storage.config
%config(noreplace) %attr(644,ats,ats) /opt/trafficserver/etc/trafficserver/update.config
%config(noreplace) %attr(644,ats,ats) /opt/trafficserver/etc/trafficserver/vaddrs.config
%config(noreplace) %attr(644,ats,ats) /opt/trafficserver/etc/trafficserver/volume.config
%config(noreplace) %attr(644,ats,ats) /opt/trafficserver/etc/trafficserver/prefetch.config
%config(noreplace) %attr(644,ats,ats) /opt/trafficserver/etc/trafficserver/stats.config.xml

%changelog
* Wed Aug 7 2013 Jeff Elsloo <jeffrey_elsloo(at)cable.comcast.com>
- Modified to support building 3.3.x
- Modified to support upgrades
* Sun Aug 12 2012 John Benton <john_benton(at)cable.comcast.com>
- Initial RPM build based on SVN version 2376
- Rev for ATS 3.2.0 based on SVN version 2470
- Rev for ATS 3.2.0 based on SVN version 2555
- Rev for ATS 3.2.0 based on SVN version 4812
