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
import pytest
from unittest.mock import MagicMock, patch
from systemd_dbus import SystemdManager

def systemd_available():
    try:
        mgr = SystemdManager()
        return mgr._dbus_available
    except Exception:
        return False

@pytest.fixture(scope="session")
def test_dbus():
    import subprocess, os
    proc = subprocess.Popen(
        ["dbus-daemon", "--config-file=tests/dbus/test-bus.conf", "--print-address"],
        stdout=subprocess.PIPE,
    )
    assert(proc.stdout is not None)
    address = proc.stdout.readline().decode().strip()

    os.environ["DBUS_SYSTEM_BUS_ADDRESS"] = address
    yield address
    proc.terminate()

@pytest.mark.skipif(not systemd_available(), reason="systemd not available")
def test_version_returns_int():
    mgr = SystemdManager()
    v = mgr.version()
    assert isinstance(v, int)
    assert v > 0

@pytest.mark.skipif(not systemd_available(), reason="systemd not available")
def test_timezone_returns_string():
    mgr = SystemdManager()
    tz = mgr.timezone()
    assert isinstance(tz, str)
    assert len(tz) > 0
