import pytest

pytestmark = pytest.mark.integration

@pytest.fixture
def manager():
    from systemd_dbus import SystemdManager
    m = SystemdManager()
    if not m._dbus_available:
        pytest.skip("D-Bus is not available")
    yield m
    m.close()

def test_check_dbus_available(manager):
    assert manager._dbus_available is True

def test_timezone_returns_string(manager):
    tz = manager.timezone()
    assert isinstance(tz, str)
    assert len(tz) > 0

def test_active_unit(manager):
    assert manager.active("systemd-journald.service") is True

def test_pid(manager):
    pid = manager.pid("systemd-journald.service")
    assert isinstance(pid, int)
    assert pid > 0

def test_get_unit_property_active_state(manager):
    state = manager.get_unit_property("systemd-journald.service", "ActiveState")
    assert state in ("active", "inactive", "failed", "activating", "deactivating", "reloading")

def test_get_unit_property_nonexistent(manager):
    from systemd_dbus.manager import SystemdError
    with pytest.raises(SystemdError):
        manager.get_unit_property("systemd-journald.service", "does_not_exist")

def test_get_property_types(manager):

    # Test string
    tz = manager._get_property(
        "org.freedesktop.timedate1",
        "/org/freedesktop/timedate1",
        "org.freedesktop.timedate1",
        "Timezone", "s"
    )
    assert isinstance(tz, str)

    # Test bool
    ntp = manager._get_property(
        "org.freedesktop.timedate1",
        "/org/freedesktop/timedate1",
        "org.freedesktop.timedate1",
        "NTP", "b"
    )
    assert isinstance(ntp, bool)

    # test uint64
    time = manager._get_property(
        "org.freedesktop.timedate1",
        "/org/freedesktop/timedate1",
        "org.freedesktop.timedate1",
        "TimeUSec", "t"
    )
    assert isinstance(time, int)

    assert time > 0 

