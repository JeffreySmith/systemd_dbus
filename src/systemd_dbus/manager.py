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

import ctypes
import importlib.util
import re
import subprocess
import warnings

from typing import Final

try:
    # If this is available, try to run any fallback commands with sudo privileges
    from resource_management.core import shell
    AMBARI_AVAILABLE = True
except ImportError:
    # If not, use subprocess instead
    AMBARI_AVAILABLE = False


# This is the size of the error buffer in bytes
ERRBUF_SIZE: Final[int] = 1024

class DBusType:
    STRING = b"s"
    UINT32 = b"u"
    INT32  = b"i"
    UINT64 = b"t"
    INT64  = b"x"
    BOOL   = b"b"

class SystemdError(Exception):
    pass

class SystemdManager:
    _lib = None
    _dbus_available = None

    def __init__(self):
        if SystemdManager._lib is None:
            SystemdManager._lib = self._load_lib()
        if SystemdManager._dbus_available is None:
            SystemdManager._dbus_available = self._check_dbus()

    @classmethod
    def _load_lib(cls):
        spec = importlib.util.find_spec("systemd_dbus._native")
        if spec is None or not spec.origin:
            raise RuntimeError("Could not find systemd_dbus._native module - There may be an installation issue")

        lib = ctypes.CDLL(spec.origin)

        # All of these have the same type signature
        for fn_name in ("start_unit", "stop_unit", "restart_unit", "enable_unit", "disable_unit"):
            fn = getattr(lib, fn_name)
            fn.argtypes = [
                ctypes.c_char_p, # unit name
                ctypes.c_char_p, # error buffer
                ctypes.c_size_t] # error buffer length
            fn.restype = ctypes.c_int # return type is a status int. Non-zero means failure

        # Check to see that dbus is running
        lib.check_dbus_available.argtypes = [
            ctypes.c_char_p, # error buffer
            ctypes.c_size_t, # error buffer length
        ]
        lib.check_dbus_available.restype = ctypes.c_int

        # Get properties from Systemd
        lib.get_property.argtypes = [
            ctypes.c_char_p,  # destination
            ctypes.c_char_p,  # path
            ctypes.c_char_p,  # interface
            ctypes.c_char_p,  # property
            ctypes.c_char_p,  # type
            ctypes.c_char_p,  # result
            ctypes.c_size_t,  # result_len
            ctypes.c_char_p,  # errbuf
            ctypes.c_size_t,  # errbuf_len
        ]
        lib.get_property.restype = ctypes.c_int

        # Get properties from a specified unit file
        lib.get_unit_property.argtypes = [
            ctypes.c_char_p,  # unit_name
            ctypes.c_char_p,  # interface
            ctypes.c_char_p,  # property
            ctypes.c_char_p,  # type
            ctypes.c_char_p,  # result
            ctypes.c_size_t,  # result_len
            ctypes.c_char_p,  # errbuf
            ctypes.c_size_t,  # errbuf_len
        ]
        lib.get_unit_property.restype = ctypes.c_int

        # Restart the sysetmd daemon
        lib.daemon_reload.argtypes = [
            ctypes.c_char_p,
            ctypes.c_size_t,
        ]
        lib.daemon_reload.restype = ctypes.c_int

        return lib

    def _check_dbus(self) -> bool:
        errbuf = ctypes.create_string_buffer(ERRBUF_SIZE)
        assert(self._lib is not None)
        r = self._lib.check_dbus_available(errbuf, ctypes.sizeof(errbuf))
        if r < 0:
            msg = errbuf.value.decode(errors="replace")
            import warnings
            warnings.warn(f"D-Bus unavailable, falling back to calling systemctl directly: {msg}")

        return True

    def _call(self, fn_name: str, unit_name: str) -> None:
        unit_name = unit_name if unit_name.endswith(".service") else f"{unit_name}.service"
        if self._dbus_available:
            errbuf = ctypes.create_string_buffer(ERRBUF_SIZE)
            fn = getattr(self._lib, fn_name)
            r = fn(unit_name.encode(), errbuf, ctypes.sizeof(errbuf))
            if r < 0:
                msg = errbuf.value.decode(errors="replace") or f"errno {-r}"
                # If we are denied access over dbus, try to manage the service using systemctl directly instead
                if "AccessDenied" in msg or "Interactive authentication" in msg:
                    warnings.warn(f"D-Bus permission denied for {fn_name}, attempting fallback")
                    self._fallback_call(fn_name, unit_name)
                    return
                raise SystemdError(f"{fn_name} failed for {unit_name!r}: {msg}")

    def _fallback_call(self, fn_name: str, unit_name: str, timeout: int =30, additional_args:list|None=None, password:str|None=None) -> None:
        replaced_fn_name = fn_name.replace("_unit", "")
        command = ["systemctl", replaced_fn_name, unit_name]
        if additional_args:
            command.extend(additional_args)
        if AMBARI_AVAILABLE:
            from resource_management.core import shell
            code, stdout, stderr = shell.checked_call(
                tuple(command),
                sudo=True,
                stderr=subprocess.PIPE,
                quiet=True,
            )
            if code != 0:
                raise SystemdError(f"systemctl {replaced_fn_name!r} failed for {unit_name!r} through Ambari: {stderr.strip()}")
        else:
            from subprocess import Popen, PIPE
            try:
                process = Popen(command, stdout=PIPE, stderr=PIPE)
            except OSError as e:
                raise SystemdError(f"Failed to execute systemctl command: {e}")
            try:
                _, stderr = process.communicate(timeout=timeout)
            except subprocess.TimeoutExpired:
                # Try to kill the process if it's still running, and then see if we can grab any output from it
                process.kill()
                _, stderr = process.communicate()
                raise SystemdError(f"systemctl {replaced_fn_name!r} timed out after {timeout} seconds for {unit_name!r}")
            if process.returncode != 0:
                raise SystemdError(f"systemctl {replaced_fn_name!r} failed for {unit_name!r}: {stderr.decode().strip()}")

    def _fallback_with_stdout(self, fn_name: str, unit_name: str, timeout: int =30, additional_args:list|None=None) -> bytes :
        replaced_fn_name = fn_name.replace("_unit", "")
        command = ["systemctl", replaced_fn_name, unit_name]
        if additional_args:
            command.extend(additional_args)
        if AMBARI_AVAILABLE:
            from resource_management.core import shell
            code, stdout, stderr = shell.checked_call(
                tuple(command),
                sudo=True,
                stderr=subprocess.PIPE,
                quiet=True,
            )
            if code != 0:
                raise SystemdError(f"systemctl {replaced_fn_name!r} failed for {unit_name!r} through Ambari: {stderr.strip()}")
            return stdout.strip()
        else:
            from subprocess import Popen, PIPE
            try:
                process = Popen(command, stdout=PIPE, stderr=PIPE)
            except OSError as e:
                raise SystemdError(f"Failed to execute systemctl command: {e}")
            try:
                stdout, stderr = process.communicate(timeout=timeout)
            except subprocess.TimeoutExpired:
                # Try to kill the process if it's still running, and then see if we can grab any output from it
                process.kill()
                _, stderr = process.communicate()
                raise SystemdError(f"systemctl {replaced_fn_name!r} timed out after {timeout} seconds for {unit_name!r}")
            if process.returncode != 0:
                raise SystemdError(f"systemctl {replaced_fn_name!r} failed for {unit_name!r}: {stderr.decode().strip()}")

            return stdout.strip()


    def daemon_reload(self) -> None:
        """Reload the systemd daemon to pick up any changes to unit files. This is required after installing or modifying unit files, but does not require a full restart of the systemd service. Note that this will reload all unit files on the system, so it may take some time to complete if there are a large number of units. If D-Bus is unavailable or if permission is denied, this will attempt to call `systemctl daemon-reload` directly as a fallback."""
        if self._dbus_available:
            errbuf = ctypes.create_string_buffer(ERRBUF_SIZE)
            r = self._lib.daemon_reload(errbuf, ctypes.sizeof(errbuf))
            if r < 0:
                msg = errbuf.value.decode(errors="replace") or f"errno {-r}"
                if "AccessDenied" or "Interactive authentication" in msg:
                    warnings.warn("D-Bus permission denied for daemon_reload, attempting fallback")
                    self._fallback_reload()
                    return
                raise SystemdError(f"Systemd daemon reload failed: {msg}")
        else:
            self._fallback_reload()

    def _fallback_reload(self, timeout=30) -> None:
        if AMBARI_AVAILABLE:
            from resource_management.core import shell
            code, stdout, stderr = shell.checked_call(
                ("systemctl", "daemon-reload"),
                sudo=True,
                stderr=subprocess.PIPE,
                quiet=True,
            )
            if code != 0:
                raise SystemdError(f"systemctl daemon-reload failed through Ambari: {stderr.strip()}")
        else:
            from subprocess import Popen, PIPE
            try:
                process = Popen(["systemctl", "daemon-reload"], stdout=PIPE, stderr=PIPE)
            except OSError as e:
                raise SystemdError(f"Failed to execute systemctl command: {e}")
            try:
                _, stderr = process.communicate(timeout=timeout)
            except subprocess.TimeoutExpired:
                process.kill()
                _, stderr = process.communicate()
                raise SystemdError(f"systemctl daemon-reload timed out after {timeout} seconds")
            if process.returncode != 0:
                raise SystemdError(f"systemctl daemon-reload failed: {stderr.decode().strip()}")


    def start(self, unit_name: str) -> None:
        """Start a systemd unit by name. The unit name may be specified with or without the .service suffix."""
        self._call("start_unit", unit_name)

    def stop(self, unit_name: str) -> None:
        """Stop a systemd unit by name. The unit name may be specified with or without the .service suffix."""
        self._call("stop_unit", unit_name)

    def restart(self, unit_name: str) -> None:
        """Restart a systemd unit by name. The unit name may be specified with or without the .service suffix."""
        self._call("restart_unit", unit_name)

    def enable(self, unit_name: str) -> None:
        """Enable a systemd unit by name. The unit name may be specified with or without the .service suffix."""
        self._call("enable_unit", unit_name)

    def disable(self, unit_name: str) -> None:
        """Disable a systemd unit by name. The unit name may be specified with or without the .service suffix."""
        self._call("disable_unit", unit_name)

    def _get_property(self, destination: str, path: str, interface: str, property: str, dbus_type: bytes) -> str:
        result_buf = ctypes.create_string_buffer(ERRBUF_SIZE)
        errbuf = ctypes.create_string_buffer(ERRBUF_SIZE)
        assert(self._lib is not None)

        r = self._lib.get_property(
            destination.encode(),
            path.encode(),
            interface.encode(),
            property.encode(),
            dbus_type,
            result_buf,
            ctypes.sizeof(result_buf),
            errbuf,
            ctypes.sizeof(errbuf),
        )
        if r < 0:
            msg = errbuf.value.decode(errors="replace")
            raise SystemdError(f"Failed to get {property!r}: {msg}")

        return result_buf.value.decode().strip()

    def version(self) -> int | None:
        """Get the systemd version running on the system. For now, only works through DBus"""
        if self._dbus_available is None:
            return None
        val = self._get_property(
            "org.freedesktop.systemd1",
            "/org/freedesktop/systemd1",
            "org.freedesktop.systemd1.Manager",
            "Version",
            DBusType.STRING,
        )
        m = re.search(r"^([0-9]+)", val)
        return int(m.group(0)) if m else None

    def timezone(self) -> str | None:
        """Get the system timezone. For now, only works through DBus"""
        if self._dbus_available is None:
            return None
        return self._get_property(
            "org.freedesktop.timedate1",
            "/org/freedesktop/timedate1",
            "org.freedesktop.timedate1",
            "Timezone",
            DBusType.STRING,
        )

    def pid(self, unit_name: str) -> int | None:
        """Get the main PID of a systemd unit. The unit name may be specified with or without the .service suffix. If the service isn't running, returns None"""
        if self._dbus_available is None:
            pid = self._fallback_with_stdout("show", unit_name, additional_args=["--property=MainPID", "--no-pager"], timeout=10).decode()
            if not pid or "=" not in pid:
                return None
            output = pid.split("=")
            if len(output) > 1:
                try:
                    pid_val = int(output[1])
                    return pid_val if pid_val != 0 else None
                except ValueError:
                    raise SystemdError(f"Failed to parse PID from systemctl output: {pid!r}")
            return

        unit_name = unit_name if unit_name.endswith(".service") else f"{unit_name}.service"
        result = ctypes.create_string_buffer(32)
        errbuf = ctypes.create_string_buffer(ERRBUF_SIZE)
        r = self._lib.get_unit_property(
            unit_name.encode(),
            b"org.freedesktop.systemd1.Service",
            b"MainPID",
            b"u",
            result,
            ctypes.sizeof(result),
            errbuf,
            ctypes.sizeof(errbuf),
        )
        if r < 0:
            msg = errbuf.value.decode(errors="replace")
            raise SystemdError(f"Failed to get MainPID for {unit_name!r}: {msg}")
        pid = int(result.value.decode())
        return pid if pid != 0 else None
