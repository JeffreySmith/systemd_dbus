#!/bin/sh

cppcheck \
  --suppressions-list=cppcheck-suppressions.txt \
  --enable=all \
  --include=$(python3-config --includes | sed 's/-I//') \
  src/systemd_dbus/c/
