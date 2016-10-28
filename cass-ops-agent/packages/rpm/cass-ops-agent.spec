%define jarversion 1.2.0
Name:		cass-ops-agent
Version:	%{?version}
Release:	%{?release}
Summary:	Cassandra @ EVIDENCE.com

Group:		unsorted
License:	Restricted
URL:		http://evidence.com
Source0:        %{name}-%{version}.tar.gz

Requires:	apache-commons-daemon-jsvc

%description


%prep
%autosetup

%build
make release

%install
%make_install

%files
%defattr(-, root, root)
%doc

/usr/local/bin/cass-ops-cli
/usr/local/lib/cass-ops-agent/%{name}-%{jarversion}.jar
/etc/cass-ops-agent
%attr(0755, cassandra, cassandra) /var/log/cass-ops-agent
%attr(0755, cassandra, cassandra) /var/run/cass-ops-agent
%attr(0755, cassandra, cassandra) /var/lib/cass-ops-agent

%post
/sbin/chkconfig --add cass-ops-agent
exit 0

%preun
if [ "$1" = 0 ] ; then
/sbin/service cass-ops-agent stop > /dev/null 2>&1
/sbin/chkconfig --del cass-ops-agent
fi
exit 0 

%postun
if [ "$1" -ge 1 ]; then
/sbin/service cass-ops-agent restart > /dev/null 2>&1
fi
exit 0 

%changelog
* Tue Dec 02 2014 Taser International <engineering@evidence.com> - 1.1.0
- Initial package build
