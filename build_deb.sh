#!/usr/bin/env bash
PYTHON_VERSION=${1:-3.11}

# Generate debian/control from template
sed "s/@PYTHON@/python${PYTHON_VERSION}/g" debian/control.in >debian/control

PYTHON_VERSION=${PYTHON_VERSION} dpkg-buildpackage -us -uc -b

# Cleanup generated files
rm -f debian/control
rm -f debian/python3*-systemd-dbus.install
rm -f debian/python3*-systemd-dbus-dev.install
