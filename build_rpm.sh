#!/usr/bin/env bash
PYTHON_VERSION="${PYTHON_VERSION:-3.11}"
TOPDIR=$(pwd)/rpmbuild
mkdir -p ${TOPDIR}/{SOURCES,SPECS,BUILD,RPMS,SRPMS}

# Delete any old source files
rm -rf ${TOPDIR}/SOURCES/*.tar.gz

NAME="systemd-dbus"
VERSION=$(rpmspec -q --qf '%{version}\n' systemd-dbus.spec 2>/dev/null | head -1)
TARBALL="${NAME}-${VERSION}.tar.gz"

tar czf "${TOPDIR}/SOURCES/${TARBALL}" \
  --transform "s|^\.|${NAME}-${VERSION}|" \
  --exclude='./.git' \
  --exclude='./rpmbuild' \
  --exclude='./*.egg-info' \
  --exclude='./__pycache__' \
  --exclude='./dist' \
  --exclude='./build' \
  --exclude-vcs \
  .

rpmbuild -ba \
  --define "_topdir ${TOPDIR}" \
  --define "python3_pkgversion ${PYTHON_VERSION}" \
  systemd-dbus.spec

echo "Built RPMs:"
find ${TOPDIR}/RPMS -name "*.rpm" -type f
