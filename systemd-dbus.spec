# Licensed to the Apache Software Foundation (ASF) under one or more
# contributor license agreements.  See the NOTICE file distributed with
# this work for additional information regarding copyright ownership.
# The ASF licenses this file to You under the Apache License, Version 2.0
# (the "License"); you may not use this file except in compliance with
# the License.  You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

%global python3_pkgversion 3.11
# Get the actual path to the specified version of Python 3
%{expand:%%global __python3 %%(which python%{python3_pkgversion})}

# Determine if we should attempt to build a Python 2 package as well
%if 0%{?rhel} && 0%{?rhel} < 9
  %global python2_available %(test -x %{_bindir}/python2 && rpm -q python2-devel > /dev/null 2>&1 && echo 1 || echo 0)
  %if "%{python2_available}" == "1"
    %global with_python2 1
  %else
    %global with_python2 0
  %endif
%else
  %global with_python2 0
%endif


Name:           systemd-dbus
Version:        0.3
Release:        1%{?dist}
Summary:        Python bindings for systemd D-Bus management

License:        Apache-2.0
URL:            https://github.com/JeffreySmith/systemd_dbus
Source0:        %{name}-%{version}.tar.gz

BuildRequires:  gcc
BuildRequires:  systemd-devel
BuildRequires:  python%{python3_pkgversion}-devel
BuildRequires:  python%{python3_pkgversion}-setuptools
%if %{with_python2}
BuildRequires:  python2-devel
BuildRequires:  python2-setuptools
%endif

%description
A Python library providing bindings for managing systemd units and querying
D-Bus properties via libsystemd. Supports starting, stopping, restarting,
enabling, and disabling units, as well as querying unit properties and
system properties via D-Bus.

Falls back to invoking systemctl directly if D-Bus is unavailable or if
permission is denied.

%package -n python%{python3_pkgversion}-systemd-dbus
Summary:        Python %{python3_pkgversion} bindings for %{name}
Requires:       polkit
Requires:       python%{python3_pkgversion}
Requires:       systemd-libs

%description -n python%{python3_pkgversion}-systemd-dbus
A Python 3 library providing bindings for managing systemd units and querying
D-Bus properties via libsystemd. Supports starting, stopping, restarting,
enabling, and disabling units, as well as querying unit and system properties
via D-Bus. Also contains functionality for writing Systemd Unit files, as well
as polkit rules.

Falls back to invoking systemctl directly if D-Bus is unavailable or if
permission is denied.

%if %{with_python2}
%package -n python2-systemd-dbus
Summary:        Python 2 bindings for systemd D-Bus management
Requires:       polkit
Requires:       python2
Requires:       systemd-libs

%description -n python2-systemd-dbus
A Python 2 library providing bindings for managing systemd units and querying
D-Bus properties via libsystemd. Supports starting, stopping, restarting,
enabling and disabling units, as well as querying unit and system properties
via D-Bus. Also contains functionality for writing Systemd Unit files, as well
as polkit rules.

Falls back to invoking systemctl directly if D-Bus is unavailable or if
permission is denied.
%endif

%prep
%autosetup -n %{name}-%{version}

%build
%{__python3} setup.py build
%if %{with_python2}
%py2_build
%endif

%install
%{__python3} setup.py install --skip-build --root %{buildroot}
%if %{with_python2}
%py2_install
%endif


%files -n python%{python3_pkgversion}-systemd-dbus
%license LICENSE
%doc README.md
%{python3_sitearch}/systemd_dbus/
%{python3_sitearch}/systemd_dbus-%{version}-*.egg-info/

%if %{with_python2}
%files -n python2-systemd-dbus
%{python2_sitearch}/systemd_dbus/
%{python2_sitearch}/systemd_dbus-%{version}-*.egg-info/
%endif

%changelog
* Wed Mar 25 2026 Jeffrey Smith <jeffrey.smith7@gmail.com> - 0.2-1
- Initial package
