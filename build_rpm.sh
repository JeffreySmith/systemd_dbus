#!/usr/bin/env bash
PYTHON_VERSION="${PYTHON_VERSION:-3.11}"
TOPDIR=$(pwd)/rpmbuild
mkdir -p ${TOPDIR}/{SOURCES,SPECS,BUILD,RPMS,SRPMS}

# Delete any old source files
rm -rf ${TOPDIR}/SOURCES/*.tar.gz
# Download source
spectool -g -C ${TOPDIR}/SOURCES systemd-dbus.spec

# Build
rpmbuild -ba \
  --define "_topdir ${TOPDIR}" \
  --define "python3_pkgversion ${PYTHON_VERSION}" \
  systemd-dbus.spec

echo "Built RPMs:"
find ${TOPDIR}/RPMS -name "*.rpm"
