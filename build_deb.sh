#!/usr/bin/env bash
PYTHON_VERSION=${PYTHON_VERSION:-3.11}
OUTPUT_DIR=${OUTPUT_DIR:-$(pwd)/dist}
# Generate debian/control from template
sed "s/@PYTHON@/python${PYTHON_VERSION}/g" debian/control.in >debian/control

PYTHON_VERSION=${PYTHON_VERSION} dpkg-buildpackage -us -uc -b

mv ../python${PYTHON_VERSION}-systemd-dbus_*.deb ${OUTPUT_DIR}/ 2>/dev/null || true

# Cleanup generated files
rm -f debian/control
rm -f debian/python3*-systemd-dbus.install
rm -f debian/python3*-systemd-dbus-dev.install
