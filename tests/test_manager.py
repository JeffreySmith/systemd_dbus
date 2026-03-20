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

import pytest
from unittest.mock import MagicMock, patch

@pytest.fixture
def mock_sdbus():
    with patch("systemd_dbus.manager._sdbus") as mock:
        mock.SystemdDbusError = Exception
        mock.Bus.return_value = MagicMock()
        mock.get_unit_property.return_value = "active"
        yield mock

@pytest.fixture
def manager(mock_sdbus):
    from systemd_dbus import SystemdManager
    return SystemdManager()

def test_active_true(manager, mock_sdbus):
    mock_sdbus.get_unit_property.return_value = "active"
    assert manager.active("sshd.service") is True

def test_active_false(manager, mock_sdbus):
    mock_sdbus.get_unit_property.return_value = "inactive"
    assert manager.active("sshd.service") is False

def test_appends_service_suffix(manager, mock_sdbus):
    manager.active("sshd")
    mock_sdbus.get_unit_property.assert_called_with("sshd.service", "ActiveState")

def test_pid_none_less_equal_zero(manager, mock_sdbus):
    mock_sdbus.get_unit_property.return_value = 0
    assert manager.pid("sshd.service") is None

    mock_sdbus.get_unit_property.return_value = -2 
    assert manager.pid("sshd.service") is None

def test_active_raises_on_error(manager, mock_sdbus):
    mock_sdbus.get_unit_property.side_effect = Exception("Unit not found")
    from systemd_dbus.manager import SystemdError
    with pytest.raises(SystemdError):
        manager.active("nonexistent.service")
