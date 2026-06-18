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

It can also act as a context manager, so you can write something like the following
if there's only one place you want it enabled:

```python
def some_function():
  with SystemdManager() as manager:
    manager.stop("my_service")
    manager.start("my_service.service")
    timezone = manager.timezone()
```

At the end of the above block, the resources (D-Bus connection and memory
allocated with malloc) will be cleaned up.

## Usage in Ambari

There are two ways to initialize SystemdManager in Ambari, depending on if you
can be sure the package is installed before the script is initialized.

If the library is already installed:

```python
from systemd_dbus import PolkitRule, SystemdManager, UnitFile
from resource_management.core.exceptions import ComponentIsNotRunning
class SomeMpack(Script)

  def __init__(self):
    super(SomeMpack, self).__init__()
    self.manager = SystemdManager() 
    self.my_service_name = "my_service"
    self.user = "my_user"
    self.group = "my_group"
  def install(self,env):
    pass

  def configure(self, env):
    unit_file = UnitFile(
      service_name=self.my_service_name,
      user=self.my_user,
      group=self.my_group,
    )
    # set_options is used to set many options at once,
    # but each key can be set individually as well
    unit_file.set_options(
      service=[
        # Systemd Property, value, comment.
        # Any other args after those 3 will be ignored
        ("ExecStart", start_command),
        ("EnvironmentFile", "-/my_env_file", "My Env file"),
      ],
      unit=[
        ("Description", "Description of my service"),
      ],
    )
    polkit_rule = PolkitRule(
      unit_file_name,
      users=[list_of_users_allowed_to_manage_this_service],
      ambari_user=ambari_user,
    )
    
    unit_file.write()
    polkit_rule.write()
    self.manager.daemon_reload()
  
  def start(self, env):
    self.manager.start(self.my_service_name)

  def stop(self, env):
    self.manager.stop(self.my_service_name)

  def restart(self, env):
    self.stop(env)
    self.start(env)
  
  def status(self, env):
    if not self.manager.active(self.my_service_name):
      raise ComponentIsNotRunning
```

If the library is not guaranteed to be installed before the script is initialized:

```python
class SomeMpack(Script)

  def __init__(self):
    super(SomeMpack, self).__init__()
    self.manager = None


  def install(self,env):
    # Things get installed here
    pass

  @property
  def manager(self):
    """Lazy load the manager. The systemd library may not be installed
    until after self.install()"""
    if self._manager is None:
      from systemd_dbus import SystemdManager
      self._manager = SystemdManager()
    return self._manager

  def configure(self, env):
    # Now import UnitFile and PolkitRule
    from systemd_dbus import UnitFile, PolkitRule
    # Same as the first example

```

This is essentially doing a lazy import of systemd_dbus,
which will be unnecessary in python3.15 and later once [pep-0810](https://peps.python.org/pep-0810/)
is implemented.

Any resources will be cleaned up when the `SystemdManager` instance is garbage collected.

## Fallback

When using this library, it will first attempt to access everything through
DBus, but if that fails, it will attempt to fallback to using Ambari's
functionality (if available), and if not, the subprocess module from Python's
standard library.

Not all functionality is available through the fallback methods, so some
functions may not work if DBus is not available, but the most common ones
should work.

## Default Options

There are a number of options that will be set by default, some of which
are to make usage simpler, some of which are to improve security.

The most important options to verify work with your service are the following:

1. ReadWritePaths - Controls which paths the service can write to.
2. RuntimeDirectory (If needed)
3. ProtectHome - Prevents writing to the user's home directory.
4. PrivateTmp - Prevents other services from reading tmp files made by this service

`ReadWritePaths` is the option that is most likely to cause issues with your service,
since it controls where the service can write to.
In a Unit file, many keys can be specified multiple times, and the values
will not override each other. So if you need multiple directories to be writable,
you can specify `ReadWritePaths` as many times as needed.

## Systemd Unit File Options

The best place to find information about Systemd unit files is the [official docs](https://www.freedesktop.org/software/systemd/man/latest/systemd.unit.html).

At a bare minimum, you should have the following options defined:

```config
[Unit]
    Description=Description of your service
    After=network.target
[Service]
    User=<The user the service should be run as>
    Group=<Optional group the service should be run as>
    ExecStart=<start command for your service>
    ExecStop=<Optional stop command for your service>
[Install]
    WantedBy=multi-user.target

```

## Useful Functionality

If your component has more than one service (ie 'master', 'worker', 'namenode', etc),
you can define at initialization:

```python
nn_unit_file = UnitFile(service_name="hadoop", user="hdfs", component_name="namenode",)
nn_unit_file.set_start_command(my_start_command)
nn_unit_file.set_stop_command(my_stop_command)
nn_unit_file.write()
self.manager.daemon_reload()
```

This will create a unit file called `hadoop-namenode.service`.

You can manage individual keys for the Unit File:

```python
# Deletes all keys that match "EnvironmentFile"
UnitFile.delete_key("EnvironmentFile")
# Delete only the keys that match 'value'
UnitFile.delete_key("ExecStart", value="command_I_want_to_remove")
# Update some key
UnitFile.update_key(key, new_value, match="optional_value_to_match")
```

## Polkit Support

[Polkit](https://en.wikipedia.org/wiki/Polkit) is component for controlling privileges in Linux.
It can be used to allow/deny users, groups, etc, without using sudo.
These rules can be written and used without breaking the system, unlike sudo, which can do quite a lot of damage.
These rules are also easily reloadable at runtime, making using them very simple.

The only caveat is that not all Linux distributions support Polkit - For Ubuntu,
you must be running Ubuntu 23.04 or later, and for RHEL/CentOS, you must be
running RHEL/CentOS 8 or later.

Essentially, this can be used to automatically grant users the permissions
required to manage a service, without needing root access.

These rules are written in Javascript and will be written to `/usr/share/polkit-1/rules.d/`.

A basic example of automatically creating a polkit rules for a service is the following:

```python

def configure(self, env):
  # Same as above
  unit_file = UnitFile(
    service_name=self.my_service_name,
    user=self.my_user,
    group=self.my_group,
  )
  unit_file.set_options(
    service=[
      ("ExecStart", start_command),
      ("EnvironmentFile", "-/my_env_file", "My Env file"),
    ],
    unit=[
      ("Description", "Description of my service"),
    ],
  )

  polkit_rule = PolkitRule(
    "my_service", # This will become the name of the rule
    users=[params.my_user, params.my_hadoop_user],
    group=params.hadoop_group # 'group' is optional
    ambari_user=[pwd.getpwuid(os.getuid())[0]]
    ambari_group="group_that_ambari_user_belongs_to", # Also optional
  )

```

PolkitRule() has two additional specified parameters, `manual_rules`, in which
you can pass a Jinja template that you specifiy yourself, and `prefix`, which
by default is `40`, and controls the order in which files will be loaded.

Aside from that, it can take any other arguments that you might need in your template.
The args that the default template supports are:

1. users: list\[str\]
2. group: str
3. ambari_user: str
4. ambari_group: str

For your own manual Polkit rules, add any additional keywords to the instantiation
of the class:

```python
rule =PolkitRule(
  "name",
  users=["bob"],
  ambari_user="ambari"),
  my_dynamic_lib_dir=params.my_lib_dir,
)
rule.write()
```

If Polkit support is not available on the system, `.write()` is `noop`.

For full documentation on Polkit rules, see the [official documentation](https://www.freedesktop.org/software/polkit/docs/latest/polkit.8.html)

## Building a Debian package

To build the Debian package, make sure that you have all the needed Debian
package tools installed, and then run

```bash
chmod +x build_deb.sh
./build_deb.sh
```

By default, this will build a package for Python 3.11, but you can override
this by overriding the Python version:

```bash
PYTHON_VERSION=3.9 ./build_deb.sh
```

## Building an RPM package

To build an RPM package, make sure you have `rpmdevtools` installed, then run

```bash
./build_rpm.sh
```

You can change the version of Python used by doing the following:

```bash
PYTHON_VERSION=3.9 ./build_rpm.sh
```

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
