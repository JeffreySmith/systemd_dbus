# Manage Systemd over DBus

This library can be used to Start/Stop/Restart services through Systemd, first
using DBus, then falling back to either use Python's `subproccess` module, or
if running through Ambari, using its functionality to do that instead.

## Installation

You must have `systemd-devel` or `libsystemd-dev` installed, as well as a c compiler.

Then run `pip install .` from the source directory.

After that, you should be able to run

```python
from systemd_dbus import SystemdManager

manager = SystemdManager()
print(manager.pid("sshd.service"))
print(manager.version("sshd.service"))
# '.service' will be appended if not provided
manager.start("kudu.service")
manager.stop("kudu")
manager.restart("kudu.service")

```

## Missing Functionality

Expected features to be added:

1. Enabling/Disabling services
2. Get the status of a service
3. Reload the Systemd Daemon
