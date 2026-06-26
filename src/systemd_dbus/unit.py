#!/usr/bin/env ambari-python-wrap
"""Licensed to the Apache Software Foundation (ASF) under one
or more contributor license agreements.  See the NOTICE file
distributed with this work for additional information
regarding copyright ownership.  The ASF licenses this file
to you under the Apache License, Version 2.0 (the
"License"); you may not use this file except in compliance
with the License.  You may obtain a copy of the License at

  http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing,
software distributed under the License is distributed on an
"AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
KIND, either express or implied.  See the License for the
specific language governing permissions and limitations
under the License.
"""

from __future__ import print_function
__lazy_modules__ = []
#from resource_management.core import sudo

try:
    from collections.abc import Sequence
except ImportError:
    from collections import Sequence

from io import UnsupportedOperation
import os
import pwd
import sys

# Allows us to use basestring across Python 2 and 3 by setting it to str if Python 3
# Cleans up checking the types of certain variables
try:
    basestring
    ModuleNotFoundError = ImportError
except NameError:
    basestring = str

class Option:
    """Represents a key value pair option in a systemd unit file."""

    def __init__(self, key, value, comment=None):
        # type: (str, str, str|None) -> None
        self.key = key
        self.value = value
        self.comment = comment
        if not self.key or "\n" in self.key:
            raise ValueError("Invalid key {0}".format(self.key))
        # Always capitalize the first character of the key
        self.key = self.key[:1].upper() + self.key[1:]
        if not self.value or not isinstance(self.value, basestring):
            self.value = ""
        # Normalize empty comments to None
        if self.comment is not None and self.comment.strip() == "":
            self.comment = None

    def __str__(self):
        # type: () -> str
        output = []
        if self.comment:
            output.append("\t# {0}".format(self.comment))
        output.append("\t{0}={1}".format(self.key, self.value))
        return "\n".join(output)

    def __getitem__(self, index):
        # type: (int) -> str|None
        """Returns either a String or None"""
        if index == 0:
            return self.key
        elif index == 1:
            return self.value
        elif index == 2:
            return self.comment
        else:
            raise IndexError("Index '{0}' out of range".format(index))

    def __setitem__(self, index, value):
        # type: (int, str) -> None
        if index == 0:
            self.key = value
        elif index == 1:
            self.value = value
        elif index == 2:
            self.comment = value
        else:
            raise IndexError("Index '{0}' out of range".format(index))

    def __eq__(self, other):
        if not isinstance(other, Option):
            return False
        return self.key == other.key and self.value == other.value

    def __hash__(self):
        return hash((self.key, self.value))


class Section:
    """Represents a section in a systemd unit file."""

    def __init__(self, name, comment=None):
        # type: (str, str|None) -> None
        self.name = name.capitalize()
        self.comment = comment
        self.items = []

    def add(self, key, value, comment=None, **kwargs):
        """Add a key value pair to the specified section."""
        self.items.append(Option(key, value, comment))
        for k, v in kwargs.items():
            self.items.append(Option(k, v, None))

    def set_key(self, key, value, comment=None):
        """Set all keys that match to a the value or create it if not found."""
        found = False
        for idx, item in enumerate(self.items):
            if item.key == key:
                found = True
                self.items[idx] = Option(key, value, comment)
        if not found:
            self.items.append(Option(key, value, comment))

    def capitalize_keys(self, key):
        return key[:1]

    def update(self, idx, value, comment=None):
        """Update the value of an existing key by index."""
        if 0 <= idx < len(self.items):
            item = self.items[idx]
            self.items[idx] = Option(item.key, value, comment)
        else:
            raise IndexError("Index out of range")

    def update_all(self, key, value, comment=None, match=None):
        """Update all occurrences of a key with a new value.
        Or, optionally, only update keys whose value matches `match`"""
        for idx, item in enumerate(self.items):
            if item.key == key and (match is None or item.value == match):
                self.items[idx] = Option(key, value, comment)

    def delete(self, key, value=None):
        """Delete all keys that match the key value pair. If value is None,
        delete all keys that match the key."""
        n = len(self.items)
        if n == 0:
            return
        i = 0
        while i < n:
            item = self.items[i]
            if item[0] == key and (value is None or item[1] == value):
                break
            i += 1

        if i == n:
            return

        new_items = self.items[:i]
        for item in self.items[i:]:
            if item[0] == key and (value is None or item[1] == value):
                continue
            else:
                new_items.append(item)

        self.items = new_items[:]

    def clear(self):
        """Clear all items in a section."""
        del self.items[:]

    def __len__(self):
        return len(self.items)

    def __iter__(self):
        return iter(self.items)

    def __str__(self):
        if not self.items:
            return ""

        header = "# {0}\n[{1}]".format(self.comment, self.name) if self.comment else "[{0}]".format(self.name)

        section = [header] + [str(item) for item in self.items]
        return "\n".join(section)

    def __contains__(self, key):
        if isinstance(key, Option):
            return any(item == key for item in self.items)
        if isinstance(key, basestring):
            return any(item.key == key for item in self.items)

        return False

    def __getitem__(self, index):
        if isinstance(index, int) or isinstance(index, slice):
            return self.items[index]
        elif isinstance(index, basestring):
            return [item for item in self.items if item.key == index]
        raise TypeError("Index must be an int, slice, or str")


VALID_UNIT_TYPES = set([
    "service",
    "timer",
    "socket",
    "target",
    "device",
    "mount",
    "automount",
    "swap",
    "slice",
    "scope",
    "snapshot",
])

class UnitFile:
    """Class for creating and managing systemd unit files"""

    def __init__(
        self,
        service_name, # This is the name of the component overall. This could be "kudu", for example
        user,
        component_name=None, # component_name is the actual service name that will be started. IE "kudu-master"
        group=None,
        runtime_dir=None,
        folder="/usr/lib/systemd/system/",
        unit_type="service",
    ):
        if unit_type not in VALID_UNIT_TYPES:
            raise ValueError("Invalid unit type {0}. Valid types are: {1}".format(unit_type, VALID_UNIT_TYPES))
        unit_type = "." + unit_type.replace(".","")


        self.name = service_name.replace(unit_type, "")
        if component_name is not None:
            component_name = component_name.replace(unit_type, "")

        self.unit_file = (
            component_name + unit_type if component_name else self.name + unit_type
        )
        self.file_path = os.path.join(folder, self.unit_file)
        self._content = ""
        self.options = dict(
            (name, Section(name, None))
            for name in [
                "Unit",
                "Install",
                "Service",
                "Socket",
                "Mount",
                "Automount",
                "Swap",
                "Path",
                "Timer",
                "Slice",
            ]
        )

        if isinstance(runtime_dir, basestring):
            runtime_directory = runtime_dir
        elif isinstance(runtime_dir, Sequence):
            runtime_directory = " ".join(runtime_dir)
        else:
            runtime_directory = self.name

        if unit_type == ".service":
            unit = [
                ("After", "network.target")
            ]
            install = [
                ("WantedBy", "multi-user.target")
            ]
            service = [
                ("Type", "simple"),
                ("User", user),
                ("Group", group if group else user),
                ("ProtectSystem", "full"),
                ("ReadWritePaths", "/etc/{0}".format(self.name)),
                ("ReadWritePaths", "/usr/odp/"),
                ("LogsDirectory", self.name),
                ("RuntimeDirectory", runtime_directory),
                ("LockPersonality", "yes"),
                ("ProtectKernelModules", "yes"),
                ("ProtectKernelTunables", "yes"),
                ("ProtectControlGroups", "yes"),
                ("NoNewPrivileges", "true"),
                ("ProtectHome", "true"),
                ("PrivateTmp", "true"),
                ("PrivateDevices", "true"),
            ]
        else:
            unit = []
            install = []
            service = []

        default_opts = {
            "Unit": unit,
            "Install": install,
            "Service": service,
            "Socket": [],
            "Mount": [],
            "Automount": [],
            "Swap": [],
            "Path": [],
            "Timer": [],
            "Slice": [],
        }

        for section, opts in default_opts.items():
            if section not in self.options:
                self.options[section] = Section(section, None)
            for opt in opts:
                self.options[section].add(*opt)

        self.user = user

    def set_options(
        self,
        unit=None,
        install=None,
        service=None,
        socket=None,
        mount=None,
        automount=None,
        swap=None,
        path=None,
        timer=None,
        slice=None,
    ):
        """Create a systemd unit file with the given content"""
        sections = [
            (name, value)
            for name, value in [
                ("Unit", unit),
                ("Install", install),
                ("Service", service),
                ("Socket", socket),
                ("Mount", mount),
                ("Automount", automount),
                ("Swap", swap),
                ("Path", path),
                ("Timer", timer),
                ("Slice", slice),
            ]
            if value is not None
        ]

        for name, data in sections:
            name = name.capitalize()
            if name not in self.options:
                self.options[name] = Section(name, None)
            for item in data:
                # We only care about the first 3 elements. The rest are ignored
                key, value = item[:2]
                rest = list(item[2:])
                comment = str(rest[0]) if rest else None
                self.options[name].add(str(key), str(value), comment=comment)

    def set_key(
        self,
        key,
        value,
        section,
        comment=None,
    ):
        """Set all matching keys to this value."""
        section = section.capitalize()
        if section not in self.options:
            self.options[section] = Section(section, None)
        self.options[section].set_key(key, value, comment)

    def set_start_command(
        self,
        command,
    ):
        """A helper function to set the command to start the service."""
        self.delete_key("ExecStart")
        self.set_key("ExecStart", command, "Service")

    def set_stop_command(
        self,
        command,
    ):
        """A helper function to set the stop command."""
        self.delete_key("ExecStop")
        self.set_key("ExecStop", command, "Service")

    def set_user(
        self,
        user,
        group=None,
    ):
        """Set the User and Group for the service."""
        self.delete_key("User")
        self.set_key("User", user, "Service")
        if group:
            self.set_key("Group", group, "Service")

    def add_writable_path(self, path, comment=None, *paths):
        """Add permission to read and write to the paths. *paths can contain additional paths."""
        self.options["Service"].add("ReadWritePaths", path, comment)
        for p in paths:
            self.options["Service"].add("ReadWritePaths", p, None)

    def add_env_var(self, var, value):
        """Add one variable to the environment"""
        self.options["Service"].add("Environment", "{0}={1}".format(var, value))

    def add_env_vars(self, env):
        """Add a number of variables. You must pass a dictionary of key value pairs."""
        if not isinstance(env, dict):
            raise ValueError("env must be a dictionary of key value pairs")
        for k, v in env.items():
            self.options["Service"].add("Environment", "{0}={1}".format(k,v))


    def delete_key(self, key, value=None, **kwargs):
        """Delete a key. Optionally, the key must match `value`"""
        keys_to_delete = [(key, value)] + list(kwargs.items())
        for section in self.options.values():
            for k, v in keys_to_delete:
                section.delete(k, v)

    @staticmethod
    def create_env_file(env_options, file_path, owner="root", permission=0o600):
        # type: (dict, str, str, int) -> None
        """Create an environment file to use with Systemd"""
        if env_options is None or not isinstance(env_options, dict):
            raise ValueError("env_options must be a dictionary of simple key value pairs.")

        content = []
        for k, v in env_options.items():
            if isinstance(v, dict) or isinstance(v, list) or isinstance(v, tuple) or isinstance(v, set):
                raise ValueError("env_options must be a dictionary of simple key value pairs.")
            content.append("{0}=\"{1}\"".format(k, v))

        output = "\n".join(content) + "\n"

        try:
            user = pwd.getpwnam(owner)

        except KeyError as e:
            print("User '{}' does not exist - {}".format(owner, e), file=sys.stderr)
            raise

        try:
            from resource_management.core import sudo
            sudo.create_file(file_path, output, encoding="utf-8")
            sudo.chown(file_path, user)
            sudo.chmod(file_path, permission)
        except ModuleNotFoundError:
            with open(file_path, "w") as f:
                print(output, file=f)
                os.chown(file_path, user.pw_uid, user.pw_gid)
                os.chmod(file_path, permission)

        except (FileNotFoundError, PermissionError, UnsupportedOperation) as e:
            print("Could not save to file {} - {}".format(file_path, e), file=sys.stderr)
            raise
 
    def update_key(
        self,
        key,
        new_value,
        comment=None,
        match=None,
        **kwargs
    ):
        """Update all keys that optionally match `match` with a new value and
        comment. When passing additional key-value pairs as kwargs, those keys
        will be updated without comments or match criteria."""
        keys_to_update = [(key, new_value, comment, match)] + [
            (k, v, None, None) for k, v in kwargs.items()
        ]

        for section in self.options.values():
            for k, v, c, m in keys_to_update:
                section.update_all(k, v, c, match=m)

    def clear(self):
        """Clear all existing options and _content."""
        for option in self.options.values():
            option.clear()
        self._content = ""

    def delete(self):
        """Delete the systemd unit file"""
        try:
            from resource_management.core import sudo
            if os.path.exists(self.file_path):
                sudo.unlink(self.file_path)

        except ModuleNotFoundError:
            if os.path.exists(self.file_path):
                os.unlink(self.file_path)

    def create_manual(self, content):
        """Set the systemd unit file contents directly"""
        self._content = content

    def write(self):
        """Write the systemd unit file to self.file_path"""
        unit_file = str(self)
        if len(unit_file) == 0:
            print("Unit File: {} is empty", file=sys.stderr)
        try:
            from resource_management.core import sudo
            sudo.create_file(self.file_path, unit_file, encoding="utf-8")
        except ModuleNotFoundError:
            with open(self.file_path, "w") as f:
                print(unit_file, file=f)


    def __str__(self):
        if self._content:
            return self._content
        unit_file = []
        for section, items in self.options.items():
            if len(items) > 0:
                unit_file.append("[{0}]".format(section))
            for opt in items:
                opt_str = str(opt).rstrip()
                if opt_str:
                    unit_file.append(opt_str)
        output = "\n".join(unit_file) + "\n"
        return output


