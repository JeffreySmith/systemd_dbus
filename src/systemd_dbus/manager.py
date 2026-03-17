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

import re
import subprocess
import warnings
try:
    from systemd_dbus import _sdbus
    SDBUS_AVAILABLE=True
except ImportError:
    SDBUS_AVAILABLE=False
    warnings.warn("sdbus library not available, falling back to systemctl for systemd management")


try:
    from resource_management.core import shell
    AMBARI_AVAILABLE = True
except ImportError:
    AMBARI_AVAILABLE = False



_DBUS_METHODS = {
    "start_unit": _sdbus.start_unit,
    "stop_unit": _sdbus.stop_unit,
    "restart_unit": _sdbus.restart_unit,
}

class SystemdError(Exception):
    pass

class SystemdManager:
    _dbus_available = False

    def __init__(self):
        if SDBUS_AVAILABLE:
            self._dbus_available = SystemdManager._check_dbus()
            self.container_type = self.container()
            self._dbus_available = self.container_type is None
        else:
            self._dbus_available = False

    @classmethod
    def _check_dbus(cls):
        """Check if D-Bus is available and functional. Returns True if available, False if not. Raises SystemdError for unexpected errors.
        """
        try:
            return _sdbus.check_dbus_available()
        except _sdbus.SystemdDBusError as e:
            warnings.warn("D-Bus unavailable, falling back to systemctl: {}".format(e))
            return False

    def _call(self, fn_name, unit_name):
        """Generic caller for start, stop, and restart."""
        unit_name = unit_name if unit_name.endswith(".service") else "{}.service".format(unit_name)
        if self._dbus_available:
            try:
                fn = _DBUS_METHODS.get(fn_name)
                if fn is None:
                    raise SystemdError("Unsupported D-Bus method: {}".format(fn_name))
                fn(unit_name)
            except _sdbus.SystemdDBusError as e:
                msg = str(e)
                msg_lower = msg.lower()
                if "denied" in msg_lower or "interactive authentication" in msg_lower:
                    warnings.warn("D-Bus permission denied for {}, attempting fallback".format(fn_name))
                    self._fallback_call(fn_name, unit_name)
                    return
                raise SystemdError("{0} failed for {1!r}: {2}".format(fn_name, unit_name, msg))
        else:
            self._fallback_call(fn_name, unit_name)

    def _fallback_call(self, fn_name, unit_name, timeout = 30,
                       additional_args = None):
        """Fallback implementation using systemctl command. 
        This is automatically run if DBus is not enabled, or some types of 
        errors occur when running throug DBus."""
        replaced_fn_name = fn_name.replace("_unit", "")
        command = ["systemctl", replaced_fn_name, unit_name]
        if additional_args:
            command.extend(additional_args)
        if AMBARI_AVAILABLE:
            code, _, stderr = shell.checked_call(
                tuple(command),
                sudo=True,
                stderr=subprocess.PIPE,
                quiet=True,
            )
            if code != 0:
                raise SystemdError(
                    "systemctl {0!r} failed for {1!r} through Ambari: {2}".format(replaced_fn_name, unit_name, stderr.strip())
                )
        else:
            try:
                process = subprocess.Popen(command, stdout=subprocess.PIPE,
                                           stderr=subprocess.PIPE)
            except OSError as e:
                raise SystemdError("Failed to execute systemctl command: {}".format(e))
            try:
                _, stderr = process.communicate(timeout=timeout)
            except subprocess.TimeoutExpired:
                process.kill()
                _, stderr = process.communicate()
                raise SystemdError(
                    "systemctl {0!r} timed out after {1} seconds for {2!r}".format(replaced_fn_name, timeout, unit_name)
                )
            if process.returncode != 0:
                raise SystemdError(
                    "systemctl {!r} failed for {!r}: {}".format(replaced_fn_name, unit_name, stderr.decode().strip())
                )

    def _fallback_with_stdout(self, fn_name, unit_name, timeout = 30,
                              additional_args = None):
        """Fallback implementation using systemctl command. 
        This is automatically run if DBus is not enabled, or some types of 
        errors occur when running throug DBus. Also returns stdout."""

        replaced_fn_name = fn_name.replace("_unit", "")
        command = ["systemctl", replaced_fn_name, unit_name]
        if additional_args:
            command.extend(additional_args)
        if AMBARI_AVAILABLE:
            code, stdout, stderr = shell.checked_call(
                tuple(command),
                sudo=True,
                stderr=subprocess.PIPE,
                quiet=True,
            )
            if code != 0:
                raise SystemdError(
                    "systemctl {!r} failed for {!r} through Ambari: {}".format(replaced_fn_name, unit_name, stderr.decode().strip())
                )
            return stdout.strip()
        else:
            try:
                process = subprocess.Popen(command, stdout=subprocess.PIPE,
                                           stderr=subprocess.PIPE)
            except OSError as e:
                raise SystemdError("Failed to execute systemctl command: {}".format(e))
            try:
                stdout, stderr = process.communicate(timeout=timeout)
            except subprocess.TimeoutExpired:
                process.kill()
                _, stderr = process.communicate()
                raise SystemdError(
                    "systemctl {!r} timed out after {} seconds for {!r}".format(replaced_fn_name, timeout, unit_name)
                )
            if process.returncode != 0:
                raise SystemdError(
                    "systemctl {!r} failed for {!r}: {}".format(replaced_fn_name, unit_name, stderr.decode().strip())
                )
            return stdout.strip()

    def _get_property(self, destination, path, interface,
                      property, dbus_type):
        """Get a property value from DBus."""
        try:
            return _sdbus.get_property(destination, path, interface,
                                       property, dbus_type)
        except _sdbus.SystemdDBusError as e:
            raise SystemdError("Failed to get {!r}: {}".format(property, e))

    def daemon_reload(self):
        """Reload the systemd daemon to pick up any changes to unit files."""
        if self._dbus_available:
            try:
                _sdbus.daemon_reload()
            except _sdbus.SystemdDBusError as e:
                msg = str(e)
                if "Interactive authentication" in msg:
                    warnings.warn("D-Bus permission denied for daemon_reload, attempting fallback")
                    self._fallback_reload()
                    return
                raise SystemdError("Systemd daemon reload failed: {}".format(msg))
        else:
            self._fallback_reload()

    def _fallback_reload(self, timeout = 30):
        """Fallback implementation of daemon reload using systemctl command. This is automatically run if DBus is not enabled, or if permission is denied when attempting to reload through DBus.
        """
        if AMBARI_AVAILABLE:
            code, _, stderr = shell.checked_call(
                ("systemctl", "daemon-reload"),
                sudo=True,
                stderr=subprocess.PIPE,
                quiet=True,
            )
            if code != 0:
                raise SystemdError(
                    "systemctl daemon-reload failed through Ambari: {}".format(stderr.decode().strip())
                )
        else:
            try:
                process = subprocess.Popen(["systemctl", "daemon-reload"],
                                           stdout=subprocess.PIPE,
                                           stderr=subprocess.PIPE)
            except OSError as e:
                raise SystemdError("Failed to execute systemctl command: {}".format(e))
            try:
                _, stderr = process.communicate(timeout=timeout)
            except subprocess.TimeoutExpired:
                process.kill()
                _, stderr = process.communicate()
                raise SystemdError(
                    "systemctl daemon-reload timed out after {} seconds".format(timeout)
                )
            if process.returncode != 0:
                raise SystemdError(
                    "systemctl daemon-reload failed: {}".format(stderr.decode().strip())
                )

    def start(self, unit_name):
        """Start a systemd unit. The .service suffix is optional."""
        self._call("start_unit", unit_name)

    def stop(self, unit_name):
        """Stop a systemd unit. The .service suffix is optional."""
        self._call("stop_unit", unit_name)

    def restart(self, unit_name):
        """Restart a systemd unit. The .service suffix is optional."""
        self._call("restart_unit", unit_name)

    def enable(self, unit_name):
        """Enable a systemd unit. Returns a list of (type, symlink, dest) changes made."""
        unit_name = unit_name if unit_name.endswith(".service") else "{}.service".format(unit_name)
        if self._dbus_available:
            try:
                _, changes = _sdbus.enable_unit(unit_name)
                return changes
            except _sdbus.SystemdDBusError as e:
                raise SystemdError("enable_unit failed for {!r}: {}".format(unit_name, e))
        else:
            self._fallback_call("enable_unit", unit_name)
            return []

    def disable(self, unit_name):
        """Disable a systemd unit. Returns a list of (type, symlink, dest) changes made."""
        unit_name = unit_name if unit_name.endswith(".service") else "{}.service".format(unit_name)
        if self._dbus_available:
            try:
                return _sdbus.disable_unit(unit_name)
            except _sdbus.SystemdDBusError as e:
                raise SystemdError("disable_unit failed for {!r}: {}".format(unit_name, e))
        else:
            self._fallback_call("disable_unit", unit_name)
            return []

    def version(self):
        """Get the systemd version. Returns None if D-Bus is unavailable."""
        if not self._dbus_available:
            return None
        val = self._get_property(
            "org.freedesktop.systemd1",
            "/org/freedesktop/systemd1",
            "org.freedesktop.systemd1.Manager",
            "Version",
            "s",
        )
        if not isinstance(val, str):
            raise SystemdError("Unexpected type for systemd Version property: {}".format(type(val)))
        m = re.search(r"^([0-9]+)", val)
        return int(m.group(0)) if m else None

    def timezone(self):
        """Get the system timezone. Returns None if D-Bus is unavailable."""
        if not self._dbus_available:
            return None
        return self._get_property(
            "org.freedesktop.timedate1",
            "/org/freedesktop/timedate1",
            "org.freedesktop.timedate1",
            "Timezone",
            "s",
        )

    def pid(self, unit_name):
        """Get the main PID of a systemd unit. Returns None if not running."""
        unit_name = unit_name if unit_name.endswith(".service") else "{}.service".format(unit_name)
        if not self._dbus_available:
            raw = self._fallback_with_stdout(
                "show", unit_name,
                additional_args=["--property=MainPID", "--no-pager"],
                timeout=10,
            ).decode()
            if not raw or "=" not in raw:
                return None
            try:
                pid = int(raw.split("=", 1)[1])
                return pid if pid != 0 else None
            except ValueError:
                raise SystemdError(
                    "Failed to parse PID from systemctl output: {!r}".format(raw)
                )

        try:
            # This should only ever return a number that can fit into an int, but because of the Python 2 api, 
            # can return as a long. If we convert it to an int that can be used by an external process.
            pid = int(_sdbus.get_unit_property(unit_name, "MainPID"))
            return pid if pid != 0 else None
        except _sdbus.SystemdDBusError as e:
            raise SystemdError("Failed to get MainPID for {!r}: {}".format(unit_name, e))
        except ValueError as e:
            raise SystemdError("MainPID for {!r} not a valid number: {}. This is likely a bug in the c code".format(unit_name, e))

    def container(self):
        """Returns None if it is not running in a container, or a string identifying the container type if it is"""
        if self._dbus_available:
            try:
                val = _sdbus.get_property(
                    "org.freedesktop.systemd1",
                    "/org/freedesktop/systemd1",
                    "org.freedesktop.systemd1.Manager",
                    "Virtualization",
                    "s",
                )
            except _sdbus.SystemdDBusError as e:
                raise SystemdError("Failed to get Container property: {}".format(e))

            container_types = {
                "docker", "lxc", "lxc-libvirt", "lxc-oci", "rkt", "systemd-nspawn", "podman", "wsl", "proot", "pouch",
            }
            return val if val in container_types else None
        else:
            return self._fallback_container()

    def _fallback_container(self):
        try:
            process = subprocess.Popen(
                ["systemd-detect-virt", "--container"],
                stdout=subprocess.PIPE,
                stderr=subprocess.PIPE,
            )
            stdout, _ = process.communicate(timeout=5)
            if process.returncode == 0:
                return stdout.decode().strip() or None
            return None

        except (OSError, subprocess.TimeoutExpired) as e:
            warnings.warn("Error occured while trying to detect if we're running in a container: {}".format(e))
            return None
