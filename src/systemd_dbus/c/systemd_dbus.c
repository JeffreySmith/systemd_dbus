/*Licensed to the Apache Software Foundation (ASF) under one
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
*/
#include "dbus_api.h"
#include <Python.h>
#include <assert.h>
#include <errno.h>
#include <inttypes.h>
#include <stdbool.h>
#include <stddef.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <systemd/sd-bus.h>
// Some of the C api changed between Python 2 and 3. Redirect the old names to
// the new ones for building for Python 2.
// This will only run when this is building in Python 2
#if PY_MAJOR_VERSION < 3
#define PyLong_FromLong PyInt_FromLong
#define PyUnicode_FromString PyString_FromString
#endif

// Define a generic holder for Systemd errors
static PyObject *SystemdDBusError = NULL;

typedef struct {
  PyObject_HEAD sd_bus *bus;
} BusObject;

// Used to automatically deallocate the bus connection when the object goes
// out of scope in Python
static void Bus_dealloc(BusObject *self) {
  if (self->bus) {
    sd_bus_unref(self->bus);
    self->bus = NULL;
  }
  Py_TYPE(self)->tp_free((PyObject *)self);
}

// Initialize the bus connection when the object is created in Python
static PyObject *Bus_new(PyTypeObject *type, PyObject *args, PyObject *kwds) {
  BusObject *self = (BusObject *)type->tp_alloc(type, 0);
  if (!self) {
    fprintf(stderr, "Failed to initialize Bus");
    return PyErr_NoMemory();
  }
  self->bus = NULL;
  return (PyObject *)self;
}

// Actual initialization of the bus connection
static int Bus_init(BusObject *self, PyObject *args, PyObject *kwds) {
  int r;

  r = sd_bus_open_system(&self->bus);
  if (r < 0) {
    char errbuf[1024] = {0};
    snprintf(errbuf, sizeof(errbuf), "Failed to connect to system bus: %s",
             strerror(-r));
    PyErr_SetString(SystemdDBusError, errbuf);
  }

  return r < 0 ? r : 0;
}
// Gives a `with bus_connection as b:` type of context in Python
static PyObject *Bus_enter(BusObject *self, PyObject *args) {
  Py_INCREF(self);
  return (PyObject *)self;
}
// Deallocs the bus connection when exiting the context in Python
static PyObject *Bus_exit(BusObject *self, PyObject *args) {
  if (self->bus) {
    sd_bus_unref(self->bus);
    self->bus = NULL;
  }
  Py_RETURN_FALSE;
}
// Let Python know about the __enter__ and __exit__ methods for the context
// manager
static PyMethodDef Bus_methods[] = {
    {"__enter__", (PyCFunction)Bus_enter, METH_NOARGS,
     "Enter the runtime context related to this object."},
    {"__exit__", (PyCFunction)Bus_exit, METH_VARARGS,
     "Exit the runtime context related to this object."},
    {NULL, NULL, 0, NULL}};

// Define the Bus for Python
static PyTypeObject BusType = {
    PyVarObject_HEAD_INIT(NULL, 0).tp_name = "_sdbus.Bus",
    .tp_basicsize = sizeof(BusObject),
    .tp_dealloc = (destructor)Bus_dealloc,
    .tp_flags = Py_TPFLAGS_DEFAULT,
    .tp_doc = "A persistent bus connection using sd-bus",
    .tp_methods = Bus_methods,
    .tp_init = (initproc)Bus_init,
    .tp_new = Bus_new};

// This initially checks if DBus is available, before a persistent connection is
// made
static PyObject *py_check_dbus_available(PyObject *self, PyObject *args) {
  char errbuf[1024] = {0};
  int r;

  // clang-format off
  Py_BEGIN_ALLOW_THREADS
    r = check_dbus_available(errbuf, sizeof(errbuf));
  Py_END_ALLOW_THREADS

  if (r < 0) {
    PyErr_SetString(SystemdDBusError, errbuf);
    return NULL;
  }
  // clang-format on
  // This is actually returning, ignore any static checkers
  Py_RETURN_TRUE;
}

// This is for checking the dbus connection after a persistent connection has
// been made
static PyObject *py_ping_dbus(PyObject *self, PyObject *args) {
  BusObject *bus;
  char errbuf[1024] = {0};
  int r;

  if (!PyArg_ParseTuple(args, "O!", &BusType, &bus)) {
    return NULL;
  }

  if (!bus->bus) {
    PyErr_SetString(SystemdDBusError, "Bus connection is not connected");
    return NULL;
  }

  // clang-format off
  Py_BEGIN_ALLOW_THREADS
    r = ping_dbus(bus->bus, errbuf, sizeof(errbuf));
  Py_END_ALLOW_THREADS

  if (r < 0) {
    PyErr_SetString(SystemdDBusError, errbuf);
    return NULL;
  }
  // clang-format on
  // This is actually returning, ignore any static checkers
  Py_RETURN_TRUE;
}

// Start a unit file using the dbus connection
static PyObject *py_start_unit(PyObject *self, PyObject *args) {
  BusObject *bus;
  const char *unit;
  char errbuf[1024] = {0};
  int r;

  char *unit_copy = NULL;

  if (!PyArg_ParseTuple(args, "O!s", &BusType, &bus, &unit)) {
    return NULL;
  }

  if (!bus->bus) {
    PyErr_SetString(SystemdDBusError, "Bus connection is not connected");
    return NULL;
  }

  // Make a local copy of the unit name since we are releasing the thread, and
  // `unit` comes from Python
  unit_copy = strdup(unit);
  if (!unit_copy) {
    return PyErr_NoMemory();
  }

  // clang-format off
  Py_BEGIN_ALLOW_THREADS
    r = call_method(bus->bus, "StartUnit", unit_copy, errbuf, sizeof(errbuf));
  Py_END_ALLOW_THREADS
  
  free(unit_copy);
  unit_copy = NULL;

  if (r < 0) {
    PyErr_SetString(SystemdDBusError, errbuf);
    return NULL;
  }
  // clang-format on
  // This is actually returning, ignore any static checkers
  Py_RETURN_NONE;
}

// Stop a unit file using the persistent dbus connection
static PyObject *py_stop_unit(PyObject *self, PyObject *args) {
  BusObject *bus;
  const char *unit;
  char errbuf[1024] = {0};
  int r;

  char *unit_copy = NULL;

  if (!PyArg_ParseTuple(args, "O!s", &BusType, &bus, &unit)) {
    return NULL;
  }

  if (!bus->bus) {
    PyErr_SetString(SystemdDBusError, "Bus connection is not connected");
    return NULL;
  }
  // Make a local copy of the unit name since we are releasing the thread, and
  // `unit` comes from Python

  unit_copy = strdup(unit);
  if (!unit_copy) {
    return PyErr_NoMemory();
  }

  // clang-format off
  Py_BEGIN_ALLOW_THREADS
    r = call_method(bus->bus, "StopUnit", unit_copy, errbuf, sizeof(errbuf));
  Py_END_ALLOW_THREADS

  free(unit_copy);
  unit_copy = NULL;

  if (r < 0) {
    PyErr_SetString(SystemdDBusError, errbuf);
    return NULL;
  }
  // clang-format on
  // This is actually returning, ignore any static checkers
  Py_RETURN_NONE;
}
// Restart a unit file using the persistent dbus connection
static PyObject *py_restart_unit(PyObject *self, PyObject *args) {
  BusObject *bus;
  const char *unit;
  char errbuf[1024] = {0};
  int r;
  char *unit_copy = NULL;
  if (!PyArg_ParseTuple(args, "O!s", &BusType, &bus, &unit)) {
    return NULL;
  }

  if (!bus->bus) {
    PyErr_SetString(SystemdDBusError, "Bus connection is not connected");
    return NULL;
  }
  // Make a local copy of the unit name since we are releasing the thread, and
  // `unit` comes from Python

  unit_copy = strdup(unit);
  if (!unit_copy) {
    return PyErr_NoMemory();
  }

  // clang-format off
  Py_BEGIN_ALLOW_THREADS
    r = call_method(bus->bus, "RestartUnit", unit_copy, errbuf, sizeof(errbuf));
  Py_END_ALLOW_THREADS

  free(unit_copy);
  unit_copy = NULL;

  if (r < 0) {
    PyErr_SetString(SystemdDBusError, errbuf);
    return NULL;
  }
  // clang-format on
  // This is actually returning, ignore any static checkers
  Py_RETURN_NONE;
}

// Do the correct type conversion for the value returned from D-Bus
static PyObject *dbus_val_to_python(const DBusValue *val) {
  switch (val->type) {
  // String types
  case 's':
  case 'o':
  case 'g':
    return PyUnicode_FromString(val->s_buf);
  // Unsigned integers

  // byte
  case 'y':
    return PyLong_FromUnsignedLong(val->val.y);
  // uint16
  case 'q':
    return PyLong_FromUnsignedLong(val->val.q);
  // uint32
  case 'u':
    return PyLong_FromUnsignedLong(val->val.u);
  // uint64_t
  case 't':
    return PyLong_FromUnsignedLongLong(val->val.t);
  // Signed integers

  // int16_t
  case 'n':
    return PyLong_FromLong(val->val.n);
  // int32
  case 'i':
    return PyLong_FromLong(val->val.i);
  // int64_t
  case 'x':
    return PyLong_FromLongLong(val->val.x);
  // Boolean
  case 'b':
    return PyBool_FromLong(val->val.b);
  // double
  case 'd':
    return PyFloat_FromDouble(val->val.d);
  // Unix FD
  case 'h':
    return PyLong_FromLong(val->val.h);
  default:
    PyErr_Format(SystemdDBusError, "Unsupported DBus type: %c", val->type);
    return NULL;
  }
}

// Get a property from a particular unit file from a list of known properties.
// This prevents requiring knowledge of the type and interface
static PyObject *py_get_unit_property(PyObject *self, PyObject *args) {
  const char *unit_name;
  const char *property;
  BusObject *bus;

  char *unit_name_copy = NULL;
  char *property_copy = NULL;
  char *interface_copy = NULL;
  char *type_copy = NULL;

  char errbuf[1024] = {0};
  DBusValue val = {0};
  int r;

  if (!PyArg_ParseTuple(args, "O!ss", &BusType, &bus, &unit_name, &property)) {
    return NULL;
  }

  if (!bus->bus) {
    PyErr_SetString(SystemdDBusError, "Bus connection is not connected");
    return NULL;
  }
  const PropertyInfo *info = lookup_property(property);
  if (!info) {
    PyErr_Format(PyExc_ValueError, "Unknown property: %s", property);
    return NULL;
  }
  // Make a local copy since we are releasing the thread, and
  // these comes from Python

  unit_name_copy = strdup(unit_name);
  property_copy = strdup(property);
  interface_copy = strdup(info->interface);
  type_copy = strdup(info->type);

  // Since they're all set to NULL by default, this should be safe
  if (!unit_name_copy || !property_copy || !interface_copy || !type_copy) {
    free(unit_name_copy);
    free(property_copy);
    free(interface_copy);
    free(type_copy);
    unit_name_copy = NULL;
    property_copy = NULL;
    interface_copy = NULL;
    type_copy = NULL;
    return PyErr_NoMemory();
  }

  // clang-format off
Py_BEGIN_ALLOW_THREADS
  r = get_unit_property_raw(
    bus->bus,
    unit_name_copy,
    property_copy,
    interface_copy,
    type_copy,
    &val,
    errbuf,
    sizeof(errbuf)
  );
Py_END_ALLOW_THREADS
  //clang-format on

  free(unit_name_copy);
  free(property_copy);
  free(interface_copy);
  free(type_copy);
  unit_name_copy = NULL;
  property_copy = NULL;
  interface_copy = NULL;
  type_copy = NULL;
  if (r < 0) {
    PyErr_SetString(SystemdDBusError, errbuf);
    return NULL;
  }

  return dbus_val_to_python(&val);
}

// Get some arbitrary property from DBus
static PyObject *py_get_property(PyObject *self, PyObject *args) {
  const char *destination;
  const char *path;
  const char *interface;
  const char *property;
  const char *type;

  char *destination_copy = NULL;
  char *path_copy = NULL;
  char *interface_copy = NULL;
  char *property_copy = NULL;
  char *type_copy = NULL;

  BusObject *bus;

  DBusValue val = {0};

  char errbuf[1024] = {0};
  int r;

  if (!PyArg_ParseTuple(args, "O!sssss", &BusType, &bus, &destination, &path, &interface,
                        &property, &type)) {
    return NULL;
  }

  if (!bus->bus) {
    PyErr_SetString(SystemdDBusError, "Bus connection is not connected");
    return NULL;
  }

  if (!valid_property_type(type[0])) {
      PyErr_Format(PyExc_ValueError, "Unsupported D-Bus type: %s", type);
      return NULL;
  }
  // Make a local copy since we are releasing the thread, and
  // these comes from Python
  destination_copy = strdup(destination);
  path_copy = strdup(path);
  interface_copy = strdup(interface);
  property_copy = strdup(property);
  type_copy = strdup(type);

  if(!destination_copy || !path_copy || !interface_copy || !property_copy || !type_copy) {
    free(destination_copy);
    free(path_copy);
    free(interface_copy);
    free(property_copy);
    free(type_copy);
    destination_copy = NULL;
    path_copy = NULL;
    interface_copy = NULL;
    property_copy = NULL;
    type_copy = NULL;
    return PyErr_NoMemory();
  }
  // clang-format off
Py_BEGIN_ALLOW_THREADS
  r = get_property(bus->bus, destination_copy, path_copy, interface_copy, property_copy, type_copy, &val, errbuf, sizeof(errbuf));

Py_END_ALLOW_THREADS
  //clang-format on

  free(destination_copy);
  free(path_copy);
  free(interface_copy);
  free(property_copy);
  free(type_copy);
  destination_copy = NULL;
  path_copy = NULL;
  interface_copy = NULL;
  property_copy = NULL;
  type_copy = NULL;
  if (r < 0) {
    PyErr_SetString(SystemdDBusError, errbuf);
    return NULL;
  }
  return dbus_val_to_python(&val);
}

// Similar to py_get_unit_property but allows the caller to specify the 
// interface and D-Bus type explicitly, so it can be used for properties
// that are not known ahead of time
static PyObject *py_get_unit_property_raw(PyObject *self, PyObject *args) {
  BusObject *bus;
  const char *unit_name;
  const char *interface;
  const char *property;
  const char *type;
  char *unit_name_copy = NULL;
  char *interface_copy = NULL;
  char *property_copy = NULL;
  char *type_copy = NULL;

  char errbuf[1024] = {0};
  DBusValue val = {0};
  int r;

  if (!PyArg_ParseTuple(
    args,
    "O!ssss",
    &BusType,
    &bus,
    &unit_name,
    &interface,
    &property,
    &type)) {
      return NULL;
  }

  if (!bus->bus) {
    PyErr_SetString(SystemdDBusError, "Bus connection is not connected");
    return NULL;
  }

  if (!valid_property_type(type[0])) {
      PyErr_Format(PyExc_ValueError, "Unsupported D-Bus type: %s", type);
      return NULL;
  }
  unit_name_copy = strdup(unit_name);
  interface_copy = strdup(interface);
  property_copy = strdup(property);
  type_copy = strdup(type);


  if (!unit_name_copy || !interface_copy || !property_copy || !type_copy) {
    free(unit_name_copy);
    free(interface_copy);
    free(property_copy);
    free(type_copy);
    unit_name_copy = NULL;
    interface_copy = NULL;
    property_copy = NULL;
    type_copy = NULL;
    return PyErr_NoMemory();
  }
  // clang-format off
  Py_BEGIN_ALLOW_THREADS
    r = get_unit_property_raw(
        bus->bus,
        unit_name_copy,
        property_copy,
        interface,
        type,
        &val,
        errbuf,
        sizeof(errbuf)
    );
  Py_END_ALLOW_THREADS

  free(unit_name_copy);
  free(interface_copy);
  free(property_copy);
  free(type_copy);
  unit_name_copy = NULL;
  interface_copy = NULL;
  property_copy = NULL;
  type_copy = NULL;

  // clang-format on
  if (r < 0) {
    PyErr_SetString(SystemdDBusError, errbuf);
    return NULL;
  }

  return dbus_val_to_python(&val);
}

// Build a Python list of the changes made by enabling a unit file.
static PyObject *build_changes_list(const UnitChange *changes,
                                    size_t num_changes) {
  PyObject *list = PyList_New(num_changes);
  if (!list)
    return NULL;

  for (size_t i = 0; i < num_changes; i++) {
    PyObject *type_str = PyUnicode_FromString(changes[i].type);
    PyObject *sym_str = PyUnicode_FromString(changes[i].symlink_path);
    PyObject *dest_str = PyUnicode_FromString(changes[i].dest);

    // Something went horribly wrong
    if (!type_str || !sym_str || !dest_str) {
      Py_XDECREF(type_str);
      Py_XDECREF(sym_str);
      Py_XDECREF(dest_str);
      Py_DECREF(list);
      return NULL;
    }

    // Build a tuple out of what was changed
    PyObject *tuple = PyTuple_Pack(3, type_str, sym_str, dest_str);
    Py_DECREF(type_str);
    Py_DECREF(sym_str);
    Py_DECREF(dest_str);

    if (!tuple) {
      Py_DECREF(list);
      return NULL;
    }
    PyList_SET_ITEM(list, i, tuple);
  }
  return list;
}

// Enable a unit file using the persistent dbus connection.
static PyObject *py_enable_unit(PyObject *self, PyObject *args) {
  const char *unit;
  char *unit_copy = NULL;
  char errbuf[1024] = {0};
  // carries_install_info reports back true if there is an [Install] section in
  // the unit file
  int r, carries_install_info = 0;
  UnitChange *changes = NULL;
  size_t num_changes = 0;

  BusObject *bus;

  if (!PyArg_ParseTuple(args, "O!s", &BusType, &bus, &unit)) {
    return NULL;
  }

  if (!bus->bus) {
    PyErr_SetString(SystemdDBusError, "Bus connection is not connected");
    return NULL;
  }
  unit_copy = strdup(unit);
  if (!unit_copy) {
    return PyErr_NoMemory();
  }
  // clang-format off
  Py_BEGIN_ALLOW_THREADS
    r = enable_unit(bus->bus, unit_copy, &carries_install_info, &changes, &num_changes,
        errbuf, sizeof(errbuf));
  Py_END_ALLOW_THREADS
  //clang-format on
  
  free(unit_copy);
  unit_copy = NULL;
  if (r < 0) {
    free_unit_changes(changes, num_changes);
    PyErr_SetString(SystemdDBusError, errbuf);
    return NULL;
  }

  PyObject *changes_list = build_changes_list(changes, num_changes);
  free_unit_changes(changes, num_changes);
  if (!changes_list) return NULL;

  PyObject *carries_obj = PyBool_FromLong(carries_install_info);
  if (!carries_obj) {
    Py_DECREF(changes_list);
    return NULL;
  }


  PyObject *result = PyTuple_Pack(2, carries_obj, changes_list);
  Py_DECREF(carries_obj);
  Py_DECREF(changes_list);
  return result;
}
// Disable a unit file using the persistent dbus connection
static PyObject *py_disable_unit(PyObject *self, PyObject *args) {
  const char *unit;
  char *unit_copy = NULL;
  char errbuf[1024] = {0};
  int r;
  UnitChange *changes = NULL;
  size_t num_changes = 0;

  BusObject *bus;

  if (!PyArg_ParseTuple(args, "O!s", &BusType, &bus, &unit)){
    return NULL;
  }

  if(!bus->bus) {
    PyErr_SetString(SystemdDBusError, "Bus connection is not connected");
    return NULL;
  }
  unit_copy = strdup(unit);
  if (!unit_copy) {
    return PyErr_NoMemory();
  }
  // clang-format off
  Py_BEGIN_ALLOW_THREADS
    r = disable_unit(bus->bus, unit_copy, &changes, &num_changes, errbuf, sizeof(errbuf));
  Py_END_ALLOW_THREADS
  // clang-format off
  free(unit_copy);
  unit_copy = NULL;

  if (r < 0) {
    free_unit_changes(changes, num_changes);
    PyErr_SetString(SystemdDBusError, errbuf);
    return NULL;
  }

  PyObject *changes_list = build_changes_list(changes, num_changes);
  free_unit_changes(changes, num_changes);
  return changes_list;
}

// Reload the systemd daemon. Must be used after making any changes to a unit file
static PyObject *py_daemon_reload(PyObject *self, PyObject *args) {
  char errbuf[1024] = {0};
  int r;
  BusObject *bus;

  if (!PyArg_ParseTuple(args, "O!", &BusType, &bus)) {
    return NULL;
  }

  if(!bus->bus) {
    PyErr_SetString(SystemdDBusError, "Bus connection is not connected");
    return NULL;
  }

  // clang-format off
  Py_BEGIN_ALLOW_THREADS
    r = daemon_reload(bus->bus, errbuf, sizeof(errbuf));
  Py_END_ALLOW_THREADS

  if (r < 0) {
    PyErr_SetString(SystemdDBusError, errbuf);
    return NULL;
  }
  // clang-format on
  // This is actually returning, ignore any static checkers
  Py_RETURN_NONE;
}

// Define the functions available to Python. These will be exposed to the
// runtime

static struct PyMethodDef sdbus_methods[] = {
    {"start_unit", py_start_unit, METH_VARARGS,
     "start_unit(bus, unit_name) -> None\n\nStart a Systemd unit file on the "
     "system bus.\n Raises SystemdDbusError on failure.\n\n :param unit_name: "
     "The name of the unit (eg. sshd.service)\n\n:raises SystemdDbusError: If "
     "some error occurs while trying to start the service.\n "},
    {"stop_unit", py_stop_unit, METH_VARARGS,
     "stop_unit(bus, unit_name) -> None\n\nStop a Systemd unit file on the "
     "system "
     "bus.\n :param unit_name: The name of the service to stop (eg "
     "'sshd.service')\n:raises SystemdDbusError: If some error occurs while "
     "trying to stop the service.\n "},
    {"restart_unit", py_restart_unit, METH_VARARGS,
     "restart_unit(bus, unit_name) -> None\n\nRestart a Systemd unit file on "
     "the "
     "system bus.\n\n :param unit_name: The name of the service to restart (eg "
     "'sshd.service')\n:raises SystemdDbusError: If some error occurs while "
     "restarting the service.\n"},
    {"get_unit_property", py_get_unit_property, METH_VARARGS,
     "get_unit_property(bus, unit_name, property) -> str | int | bool\n\n Get "
     "a known"
     "property of a Systemd service on the system bus.\n\n"},
    {"get_unit_property_raw", py_get_unit_property_raw, METH_VARARGS,
     "get_unit_property_raw(bus, unit_name, interface, property, type) -> str "
     "| int | bool\n"
     "\n"
     "Get any property of a systemd unit by specifying the interface and "
     "D-Bus\n"
     "type explicitly. Use get_unit_property for known properties instead.\n"
     "\n"},
    {"enable_unit", py_enable_unit, METH_VARARGS,
     "enable_unit(bus, unit_name) -> None\n\nEnable a Systemd unit file on the "
     "system bus.\n\n :param unit_name: The name of the service to "
     "enable\n:raises SystemdDbusError: If some error occurs while enabling "
     "the service.\n"},
    {"disable_unit", py_disable_unit, METH_VARARGS,
     "disable_unit(bus, unit_name) -> None\n\nDisable a systemd unit file on "
     "the "
     "system bus\n\n :param unit_name: The name of the service to "
     "disable\n:raises SystemdDbusError: If some error occurs while disabling "
     "the service.\n"},
    {"daemon_reload", py_daemon_reload, METH_VARARGS,
     "daemon_reload(bus) -> None\nReload the Systemd daemon.\n\n:raises "
     "SystemDBusError: If some error occurs while reloading the Systemd "
     "daemon.\n"},
    {"check_dbus_available", py_check_dbus_available, METH_NOARGS,
     "check_dbus_available() -> bool\n Check if DBus is available to use\n\n:"},
    {"get_property", py_get_property, METH_VARARGS, "Get a Systemd Property"},
    {"ping_dbus", py_ping_dbus, METH_VARARGS,
     "Ping the system bus to check if it's connected"},
    // The last one must always be NULL
    {NULL, NULL, 0, NULL}};

// Initialize the module in ways that work for both Python 2 and Python 3.
static PyObject *_init_module(void) {
  PyObject *m;
#if PY_MAJOR_VERSION >= 3
  static struct PyModuleDef moduledef = {PyModuleDef_HEAD_INIT, "_sdbus", NULL,
                                         -1, sdbus_methods};
  m = PyModule_Create(&moduledef);
#else
  m = Py_InitModule("_sdbus", sdbus_methods);
#endif

  if (!m) {
    return NULL;
  }

  SystemdDBusError = PyErr_NewException("systemd_dbus._sdbus.SystemdDBusError",
                                        PyExc_OSError, NULL);

  if (!SystemdDBusError) {
    return NULL;
  }
  Py_INCREF(SystemdDBusError);
  PyModule_AddObject(m, "SystemdDBusError", SystemdDBusError);

  if (PyType_Ready(&BusType) < 0) {
    return NULL;
  }
  Py_INCREF(&BusType);
  PyModule_AddObject(m, "Bus", (PyObject *)&BusType);

  return m;
}

#if PY_MAJOR_VERSION >= 3
// cppcheck-suppress unusedFunction
PyMODINIT_FUNC PyInit__sdbus(void) { return _init_module(); }
#else
// cppcheck-suppress unusedFunction
PyMODINIT_FUNC init_sdbus(void) { _init_module(); }
#endif
