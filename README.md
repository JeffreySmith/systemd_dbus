# Manage Systemd over DBus

This library can be used to Start/Stop/Restart services through Systemd, first
using DBus, then falling back to either use Python's `subproccess` module, or
if running through Ambari, using its functionality to do that instead.

If Ambari is detected, you can also write out Polkit rules, to allow specific
users to manage a specific service without needing root access, and also Systemd
unit file generation, which allows you to create Systemd unit files programatically.

This library works with both Python 2.7 and Python 3. It has only been tested
with 2.7 and 3.11, but will likely work with at least Python 3.8 or later.

## Installation

You must have `systemd-devel` or `libsystemd-dev` installed, as well as a c compiler.

Then run `pip install .` from the source directory.

Or you can run `pip install git+https://github.com/JeffreySmith/systemd_dbus` to
install directly from main.

## Basic Usage

```python
from systemd_dbus import SystemdManager

manager = SystemdManager()
print(manager.pid("sshd.service"))
print(manager.version("sshd.service"))
# '.service' will be appended if not provided
manager.start("kudu-master.service")
manager.stop("kudu-master")
manager.restart("kudu-master.service")

manager.active("kudu-master") # returns True or False

manager.enable("kudu-master") # enables the service to start on boot
manager.timezone() # get's the timezone of the system
manager.container() # Will tell you the type of container the system is running
#in, or None if it's not running in a container
manager.virtualization() # Will tell you the type of virtualization the system is
#running on

pid = manager.pid("sshd.service") # Gives you the pid of the specified service

manager.daemon_reload() # Reload the Systemd Daemon, needed if you make changes
# to a unit file 

manager.log("Your message here") # Log a message to syslog/journald.
# You can override the default level (INFO) by passing log_level,
# Which can be any log level from Python's syslog.syslog.LOG_*
```

## Fallback

When using this library, it will first attempt to access everything through
DBus, but if that fails, it will attempt to fallback to using Ambari's
functionality (if available), and if not, the subprocess module from Python's
standard library.

Not all functionality is available through the fallback methods, so some
functions may not work if DBus is not available, but the most common ones
should work.

## Missing Functionality

Expected features to be added:

1. ~~Enabling/Disabling services~~
2. ~~Get the status of a service~~
3. ~~Reload the Systemd Daemon~~

## Static Checking the C Code

Much of this library is written in C. If you install
[cppcheck](https://github.com/danmar/cppcheck), you can run

```bash
./cppcheck_script.sh
```

to check the c code for any errors.

The following is an expected warning, and can be safely ignored.

```
src/systemd_dbus/c/systemd_dbus.c:687:16: style: The function 'PyInit__sdbus' is never used. [unusedFunction]
PyMODINIT_FUNC PyInit__sdbus(void) { return _init_module(); }
               ^
*systemd_dbus.c:0:0: information: Unmatched suppression: unusedFunction [unmatchedSuppression]

^
nofile:0:0: information: Active checkers: 113/186 (use --checkers-report=<filename> to see details) [checkersReport]
```
