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
#include <Python.h>
#include <assert.h>
#include <errno.h>
#include <inttypes.h>
#include <stddef.h>
#include <stdio.h>
#include <string.h>
/* TODO: Replace with the actual systemd header before pushing */
// #include <systemd/sd-bus.h>
#include "sd-bus.h"

// Some of the C api changed between Python 2 and 3
#if PY_MAJOR_VERSION < 3
#define PyLong_FromLong PyInt_FromLong
#define PyUnicode_FromString PyString_FromString
#endif

typedef struct {
  const char *property;
  const char *interface;
  const char *type;
} PropertyInfo;

typedef struct {
  char type;
  union {
    const char *s;
    uint32_t u;
    int32_t i;
    uint64_t t;
    int64_t x;
    int b;
  } val;
  char s_buf[1024];
} DBusValue;

typedef struct {
  char *type;
  char *symlink_path;
  char *dest;
} UnitChange;

static const PropertyInfo known_properties[] = {
    {"ActiveState", "org.freedesktop.systemd1.Unit", "s"},
    {"SubState", "org.freedesktop.systemd1.Unit", "s"},
    {"LoadState", "org.freedesktop.systemd1.Unit", "s"},
    {"UnitFileState", "org.freedesktop.systemd1.Unit", "s"},
    {"MainPID", "org.freedesktop.systemd1.Service", "u"},
    {"ExecMainCode", "org.freedesktop.systemd1.Service", "i"},
    {"ExecMainStatus", "org.freedesktop.systemd1.Service", "i"},
    {NULL, NULL, NULL}};

// Define a generic holder for Systemd errors
static PyObject *SystemdDBusError = NULL;

static const PropertyInfo *lookup_property(const char *property) {
  for (const PropertyInfo *p = known_properties; p->property; p++) {
    if (strcmp(p->property, property) == 0) {
      return p;
    }
  }
  return NULL;
}

static void free_unit_changes(UnitChange *changes, size_t num) {
  if (!changes)
    return;

  for (size_t i = 0; i < num; i++) {
    free(changes[i].type);
    free(changes[i].symlink_path);
    free(changes[i].dest);
    changes[i].type = NULL;
    changes[i].symlink_path = NULL;
    changes[i].dest = NULL;
  }
  free(changes);
}

static int read_unit_changes(sd_bus_message *reply, UnitChange **changes_out,
                             size_t *num_changes_out, char *errbuf,
                             size_t errbuf_len) {
  UnitChange *changes = NULL;
  size_t num_changes = 0;
  int r;

  r = sd_bus_message_enter_container(reply, 'a', "(sss)");
  if (r < 0) {
    snprintf(errbuf, errbuf_len, "Failed to enter changes array: %s",
             strerror(-r));
    return r;
  }

  while ((r = sd_bus_message_enter_container(reply, 'r', "sss")) > 0) {
    const char *ctype, *symlink_path, *dest;
    r = sd_bus_message_read(reply, "sss", &ctype, &symlink_path, &dest);
    if (r < 0) {
      snprintf(errbuf, errbuf_len, "Failed to read change entry: %s",
               strerror(-r));
      free_unit_changes(changes, num_changes);
      return r;
    }
    UnitChange *tmp = realloc(changes, (num_changes + 1) * sizeof(UnitChange));
    if (!tmp) {
      snprintf(errbuf, errbuf_len, "Failed to allocate memory for changes");
      free_unit_changes(changes, num_changes);
      return -ENOMEM;
    }
    changes = tmp;

    // Make it safe to free the last element, if anything goes wrong
    changes[num_changes - 1] = (UnitChange){NULL, NULL, NULL};
    changes[num_changes - 1].type = strdup(ctype);
    changes[num_changes - 1].symlink_path = strdup(symlink_path);
    changes[num_changes - 1].dest = strdup(dest);

    num_changes++;

    if (!changes[num_changes - 1].type ||
        !changes[num_changes - 1].symlink_path ||
        !changes[num_changes - 1].dest) {
      snprintf(errbuf, errbuf_len, "Out of memory");
      free_unit_changes(changes, num_changes);
      return -ENOMEM;
    }

    r = sd_bus_message_exit_container(reply);
    if (r < 0) {
      snprintf(errbuf, errbuf_len, "Failed to exit change entry container: %s",
               strerror(-r));
      free_unit_changes(changes, num_changes);
      return r;
    }
  }
  // r = 0 means we've hit the end of the array, anything less than 0 is an
  // error
  if (r < 0) {
    snprintf(errbuf, errbuf_len, "Error iterating through changes: %s",
             strerror(-r));
    free_unit_changes(changes, num_changes);
    return r;
  }
  r = sd_bus_message_exit_container(reply);
  if (r < 0) {
    snprintf(errbuf, errbuf_len, "Failed to exit changes array: %s",
             strerror(-r));
    free_unit_changes(changes, num_changes);
    return r;
  }

  *changes_out = changes;
  *num_changes_out = num_changes;
  return 0;
}

static int check_dbus_available(char *errbuf, size_t errbuf_len) {
  sd_bus *bus = NULL;
  sd_bus_error err = SD_BUS_ERROR_NULL;
  sd_bus_message *reply = NULL;
  int r;
  memset(errbuf, 0, errbuf_len);

  r = sd_bus_open_system(&bus);
  if (r < 0) {
    snprintf(errbuf, errbuf_len, "Cannot open system bus: %s", strerror(-r));
    goto cleanup;
  }

  r = sd_bus_call_method(bus, "org.freedesktop.systemd1",
                         "/org/freedesktop/systemd1",
                         "org.freedesktop.DBus.Peer", "Ping", &err, &reply, "");
  if (r < 0) {
    if (errbuf && err.message) {
      snprintf(errbuf, errbuf_len, "%s", err.message);
    } else if (errbuf) {
      snprintf(errbuf, errbuf_len, "systemd1 not available on bus: %s",
               strerror(-r));
    }
  }

cleanup:
  sd_bus_error_free(&err);
  sd_bus_message_unref(reply);
  sd_bus_unref(bus);
  return r < 0 ? r : 0;
}

PyObject *py_check_dbus_available(PyObject *self, PyObject *args) {
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

static int call_method(const char *method, const char *unit, char *errbuf,
                       size_t errbuf_len) {
  sd_bus *bus = NULL;
  sd_bus_error err = SD_BUS_ERROR_NULL;
  sd_bus_message *reply = NULL;

  // Return code for any issues
  int r;
  memset(errbuf, 0, errbuf_len);

  r = sd_bus_open_system(&bus);
  if (r < 0) {
    snprintf(errbuf, errbuf_len, "Failed to connect to system bus: %s",
             strerror(-r));

    goto cleanup;
  }

  r = sd_bus_call_method(bus, "org.freedesktop.systemd1",
                         "/org/freedesktop/systemd1",
                         "org.freedesktop.systemd1.Manager", method, &err,
                         &reply, "ss", unit, "replace");

  if (r < 0) {
    snprintf(errbuf, errbuf_len, "%s",
             err.message ? err.message : strerror(-r));
  }

cleanup:
  sd_bus_error_free(&err);
  sd_bus_message_unref(reply);
  sd_bus_unref(bus);
  return r < 0 ? r : 0;
}

static PyObject *py_start_unit(PyObject *self, PyObject *args) {
  const char *unit;
  char errbuf[1024] = {0};
  int r;

  if (!PyArg_ParseTuple(args, "s", &unit)) {
    return NULL;
  }

  // clang-format off
  Py_BEGIN_ALLOW_THREADS
    r = call_method("StartUnit", unit, errbuf, sizeof(errbuf));
  Py_END_ALLOW_THREADS

  if (r < 0) {
    PyErr_SetString(SystemdDBusError, errbuf);
    return NULL;
  }
  // clang-format on
  // This is actually returning, ignore any static checkers
  Py_RETURN_NONE;
}

static PyObject *py_stop_unit(PyObject *self, PyObject *args) {
  const char *unit;
  char errbuf[1024] = {0};
  int r;

  if (!PyArg_ParseTuple(args, "s", &unit)) {
    return NULL;
  }

  // clang-format off
  Py_BEGIN_ALLOW_THREADS
    r = call_method("StopUnit", unit, errbuf, sizeof(errbuf));
  Py_END_ALLOW_THREADS

  if (r < 0) {
    PyErr_SetString(SystemdDBusError, errbuf);
    return NULL;
  }
  // clang-format on
  // This is actually returning, ignore any static checkers
  Py_RETURN_NONE;
}

static PyObject *py_restart_unit(PyObject *self, PyObject *args) {
  const char *unit;
  char errbuf[1024] = {0};
  int r;

  if (!PyArg_ParseTuple(args, "s", &unit)) {
    return NULL;
  }

  // clang-format off
  Py_BEGIN_ALLOW_THREADS
    r = call_method("RestartUnit", unit, errbuf, sizeof(errbuf));
  Py_END_ALLOW_THREADS

  if (r < 0) {
    PyErr_SetString(SystemdDBusError, errbuf);
    return NULL;
  }
  // clang-format on
  // This is actually returning, ignore any static checkers
  Py_RETURN_NONE;
}

static PyObject *dbus_val_to_python(const DBusValue *val) {
  switch (val->type) {
  case 's':
    return PyUnicode_FromString(val->s_buf);
  case 'u':
    return PyLong_FromUnsignedLong(val->val.u);
  case 'i':
    return PyLong_FromLong(val->val.i);
  case 't':
    return PyLong_FromUnsignedLongLong(val->val.t);
  case 'x':
    return PyLong_FromLongLong(val->val.x);
  case 'b':
    return PyBool_FromLong(val->val.b);
  default:
    PyErr_Format(SystemdDBusError, "Unsupported DBus type: %c", val->type);
    Py_RETURN_NONE;
  }
}

static int read_message_value(sd_bus_message *reply, const char *type,
                              DBusValue *out, char *errbuf, size_t errbuf_len) {
  int r;
  memset(errbuf, 0, errbuf_len);
  out->type = type[0];

  switch (type[0]) {
  case 's': {
    const char *val = NULL;
    r = sd_bus_message_read(reply, "s", &val);
    if (r >= 0)
      snprintf(out->s_buf, sizeof(out->s_buf), "%s", val ? val : "");
    break;
  }
  case 'u':
    r = sd_bus_message_read(reply, "u", &out->val.u);
    break;
  case 'i':
    r = sd_bus_message_read(reply, "i", &out->val.i);
    break;
  case 't':
    r = sd_bus_message_read(reply, "t", &out->val.t);
    break;
  case 'x':
    r = sd_bus_message_read(reply, "x", &out->val.x);
    break;
  case 'b':
    r = sd_bus_message_read(reply, "b", &out->val.b);
    break;
  default:
    snprintf(errbuf, errbuf_len, "Unsupported type: %c", type[0]);
    return -EINVAL;
  }

  if (r < 0 && !errbuf[0])
    snprintf(errbuf, errbuf_len, "Failed to read value: %s", strerror(-r));
  return r;
}

static int get_unit_property_raw(const char *unit_name, const char *property,
                                 const char *interface, const char *type,
                                 DBusValue *out, char *errbuf,
                                 size_t errbuf_len) {

  sd_bus *bus = NULL;
  sd_bus_error err = SD_BUS_ERROR_NULL;
  sd_bus_message *reply = NULL;
  char *unit_path = NULL;
  int r;

  memset(errbuf, 0, errbuf_len);

  r = sd_bus_open_system(&bus);

  if (r < 0) {
    snprintf(errbuf, errbuf_len, "Failed to connect to system bus: %s",
             strerror(-r));
    goto cleanup;
  }

  r = sd_bus_call_method(bus, "org.freedesktop.systemd1",
                         "/org/freedesktop/systemd1",
                         "org.freedesktop.systemd1.Manager", "GetUnit", &err,
                         &reply, "s", unit_name);

  if (r < 0) {
    snprintf(errbuf, errbuf_len, "%s",
             err.message ? err.message : strerror(-r));
    goto cleanup;
  }

  const char *path = NULL;

  r = sd_bus_message_read(reply, "o", &path);

  if (r < 0) {
    snprintf(errbuf, errbuf_len, "Failed to read unit path: %s", strerror(-r));
    goto cleanup;
  }

  unit_path = strdup(path);
  if (!unit_path) {
    snprintf(errbuf, errbuf_len, "Failed to allocate memory for unit path");
    r = -ENOMEM;
    goto cleanup;
  }

  sd_bus_message_unref(reply);
  reply = NULL;
  sd_bus_error_free(&err);
  err = SD_BUS_ERROR_NULL;

  r = sd_bus_get_property(bus, "org.freedesktop.systemd1", unit_path, interface,
                          property, &err, &reply, type);

  if (r < 0) {
    snprintf(errbuf, errbuf_len, "%s",
             err.message ? err.message : strerror(-r));
    goto cleanup;
  }

  r = read_message_value(reply, type, out, errbuf, errbuf_len);

  if (r < 0 && !errbuf[0]) {
    snprintf(errbuf, errbuf_len, "Failed to read value: %s", strerror(-r));
  }

cleanup:
  sd_bus_error_free(&err);
  sd_bus_message_unref(reply);
  sd_bus_unref(bus);
  free(unit_path);
  return r < 0 ? r : 0;
}

static PyObject *py_get_unit_property(PyObject *self, PyObject *args) {
  const char *unit_name;
  const char *property;
  const char *interface;
  const char *type;

  char errbuf[1024] = {0};
  DBusValue val = {0};
  int r;

  if (!PyArg_ParseTuple(args, "ss", &unit_name, &property)) {
    return NULL;
  }
  const PropertyInfo *info = lookup_property(property);
  if (!info) {
    PyErr_Format(PyExc_ValueError, "Unknown property: %s", property);
    return NULL;
  }

  // clang-format off
Py_BEGIN_ALLOW_THREADS
  r = get_unit_property_raw(
    unit_name,
    info->interface,
    property,
    info->type,
    &val, errbuf,
    sizeof(errbuf)
  );
Py_END_ALLOW_THREADS
  if (r < 0) {
    PyErr_SetString(SystemdDBusError, errbuf);
    return NULL;
  }

  return dbus_val_to_python(&val);
  //clang-format on
}

static int get_property(const char *destination, const char *path,
                        const char *interface, const char *property,
                        const char *type, DBusValue *out,
                        char *errbuf, size_t errbuf_len) {
    sd_bus *bus = NULL;
    sd_bus_error err = SD_BUS_ERROR_NULL;
    sd_bus_message *reply = NULL;
    int r;

    memset(errbuf, 0, errbuf_len);

    r = sd_bus_open_system(&bus);
    if (r < 0) {
        snprintf(errbuf, errbuf_len, "Failed to connect to system bus: %s",
                 strerror(-r));
        goto cleanup;
    }

    r = sd_bus_get_property(bus, destination, path, interface, property,
                            &err, &reply, type);
    if (r < 0) {
        snprintf(errbuf, errbuf_len, "%s",
                 err.message ? err.message : strerror(-r));
        goto cleanup;
    }

    r = read_message_value(reply, type, out, errbuf, errbuf_len);

cleanup:
    sd_bus_error_free(&err);
    sd_bus_message_unref(reply);
    sd_bus_unref(bus);
    return r < 0 ? r : 0;
}

static PyObject *py_get_property(PyObject *self, PyObject *args) {
  const char *destination;
  const char *path;
  const char *interface;
  const char *property;
  const char *type;

  DBusValue val = {0};

  char errbuf[1024] = {0};
  int r;

  if (!PyArg_ParseTuple(args, "sssss", &destination, &path, &interface,
                        &property, &type)) {
    return NULL;
  }
  switch (type[0]) {
    case 's': case 'u': case 'i': case 't': case 'x': case 'b':
      break;
    default:
      PyErr_Format(PyExc_ValueError, "Unsupported D-Bus type: %s", type);
      return NULL;
  }
  // clang-format off
Py_BEGIN_ALLOW_THREADS
  r = get_property(destination, path, interface, property, type, &val, errbuf, sizeof(errbuf));

Py_END_ALLOW_THREADS
  if (r < 0) {
    PyErr_SetString(SystemdDBusError, errbuf);
    return NULL;
  }
  return dbus_val_to_python(&val);
  //clang-format on
}
static int enable_unit(const char *unit_name, int *carries_install_info,
                       UnitChange **changes_out, size_t *num_changes_out,
                       char *errbuf, size_t errbuf_len) {
  sd_bus *bus = NULL;
  sd_bus_error err = SD_BUS_ERROR_NULL;
  sd_bus_message *msg = NULL;
  sd_bus_message *reply = NULL;
  int r;

  memset(errbuf, 0, errbuf_len);
  *carries_install_info = 0;
  *changes_out = NULL;
  *num_changes_out = 0;

  r = sd_bus_open_system(&bus);
  if (r < 0) {
    snprintf(errbuf, errbuf_len, "Failed to connect to system bus: %s", strerror(-r));
    goto cleanup;
  }

  r = sd_bus_message_new_method_call(bus, &msg, "org.freedesktop.systemd1",
                                       "/org/freedesktop/systemd1",
                                       "org.freedesktop.systemd1.Manager",
                                       "EnableUnitFiles");
  if (r < 0) {
    snprintf(errbuf, errbuf_len, "Failed to create message: %s", strerror(-r));
    goto cleanup;
  }

  r = sd_bus_message_open_container(msg, 'a', "s");
  if (r < 0) {
    snprintf(errbuf, errbuf_len, "Failed to open array container: %s", strerror(-r));
    goto cleanup;
  }

  r = sd_bus_message_append_basic(msg, 's', unit_name);
  if (r < 0) {
    snprintf(errbuf, errbuf_len, "Failed to append unit name: %s", strerror(-r));
    goto cleanup;
  }

  r = sd_bus_message_close_container(msg);
  if (r < 0) {
    snprintf(errbuf, errbuf_len, "Failed to close array container: %s", strerror(-r));
    goto cleanup;
  }

  // runtime=false, force=true
  r = sd_bus_message_append(msg, "bb", 0, 1);
  if (r < 0) {
    snprintf(errbuf, errbuf_len, "Failed to append arguments: %s", strerror(-r));
    goto cleanup;
  }

  r = sd_bus_call(bus, msg, 0, &err, &reply);
  if (r < 0) {
    snprintf(errbuf, errbuf_len, "%s", err.message ? err.message : strerror(-r));
    goto cleanup;
  }

  int carries = 0;
  r = sd_bus_message_read(reply, "b", &carries);
  if (r < 0) {
    snprintf(errbuf, errbuf_len, "Failed to read carries_install_info: %s", strerror(-r));
    goto cleanup;
  }
  *carries_install_info = carries;

  r = read_unit_changes(reply, changes_out, num_changes_out, errbuf, errbuf_len);

cleanup:
  sd_bus_error_free(&err);
  sd_bus_message_unref(msg);
  sd_bus_message_unref(reply);
  sd_bus_unref(bus);
  return r < 0 ? r : 0;
}

static PyObject *build_changes_list(UnitChange *changes, size_t num_changes) {
  PyObject *list = PyList_New(num_changes);
  if (!list) return NULL;

  for (size_t i = 0; i < num_changes; i++) {
    PyObject *type_str = PyUnicode_FromString(changes[i].type);
    PyObject *sym_str  = PyUnicode_FromString(changes[i].symlink_path);
    PyObject *dest_str = PyUnicode_FromString(changes[i].dest);

    if (!type_str || !sym_str || !dest_str) {
      Py_XDECREF(type_str);
      Py_XDECREF(sym_str);
      Py_XDECREF(dest_str);
      Py_DECREF(list);
      return NULL;
    }

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

static int disable_unit(const char *unit_name, UnitChange **changes_out,
                        size_t *num_changes_out, char *errbuf,
                        size_t errbuf_len) {
  sd_bus *bus = NULL;
  sd_bus_error err = SD_BUS_ERROR_NULL;
  sd_bus_message *msg = NULL;
  sd_bus_message *reply = NULL;
  int r;

  memset(errbuf, 0, errbuf_len);
  *changes_out = NULL;
  *num_changes_out = 0;

  r = sd_bus_open_system(&bus);
  if (r < 0) {
    snprintf(errbuf, errbuf_len, "Failed to connect to system bus: %s",
             strerror(-r));
    goto cleanup;
  }

  r = sd_bus_message_new_method_call(
      bus, &msg, "org.freedesktop.systemd1", "/org/freedesktop/systemd1",
      "org.freedesktop.systemd1.Manager", "DisableUnitFiles");
  if (r < 0) {
    snprintf(errbuf, errbuf_len, "Failed to create message: %s", strerror(-r));
    goto cleanup;
  }

  r = sd_bus_message_open_container(msg, 'a', "s");
  if (r < 0) {
    snprintf(errbuf, errbuf_len, "Failed to open array container: %s",
             strerror(-r));
    goto cleanup;
  }

  r = sd_bus_message_append_basic(msg, 's', unit_name);
  if (r < 0) {
    snprintf(errbuf, errbuf_len, "Failed to append unit name: %s",
             strerror(-r));
    goto cleanup;
  }

  r = sd_bus_message_close_container(msg);
  if (r < 0) {
    snprintf(errbuf, errbuf_len, "Failed to close array container: %s",
             strerror(-r));
    goto cleanup;
  }

  // runtime=false only — DisableUnitFiles has no force parameter
  r = sd_bus_message_append(msg, "b", 0);
  if (r < 0) {
    snprintf(errbuf, errbuf_len, "Failed to append arguments: %s",
             strerror(-r));
    goto cleanup;
  }

  r = sd_bus_call(bus, msg, 0, &err, &reply);
  if (r < 0) {
    snprintf(errbuf, errbuf_len, "%s",
             err.message ? err.message : strerror(-r));
    goto cleanup;
  }

  r = read_unit_changes(reply, changes_out, num_changes_out, errbuf, errbuf_len);

cleanup:
  sd_bus_error_free(&err);
  sd_bus_message_unref(msg);
  sd_bus_message_unref(reply);
  sd_bus_unref(bus);
  return r < 0 ? r : 0;
}

static PyObject *py_enable_unit(PyObject *self, PyObject *args) {
  const char *unit;
  char errbuf[1024] = {0};
  // carries_install_info reports back true if there is an [Install] section in the unit file
  int r, carries_install_info = 0;
  UnitChange *changes = NULL;
  size_t num_changes = 0;

  if (!PyArg_ParseTuple(args, "s", &unit)){
    return NULL;
  }
  // clang-format off
  Py_BEGIN_ALLOW_THREADS
    r = enable_unit(unit, &carries_install_info, &changes, &num_changes,
        errbuf, sizeof(errbuf));
  Py_END_ALLOW_THREADS
  //clang-format on
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

static PyObject *py_disable_unit(PyObject *self, PyObject *args) {
  const char *unit;
  char errbuf[1024] = {0};
  int r;
  UnitChange *changes = NULL;
  size_t num_changes = 0;

  if (!PyArg_ParseTuple(args, "s", &unit)){
    return NULL;
  }
  // clang-format off
  Py_BEGIN_ALLOW_THREADS
    r = disable_unit(unit, &changes, &num_changes, errbuf, sizeof(errbuf));
  Py_END_ALLOW_THREADS
  // clang-format off

  if (r < 0) {
    free_unit_changes(changes, num_changes);
    PyErr_SetString(SystemdDBusError, errbuf);
    return NULL;
  }

  PyObject *changes_list = build_changes_list(changes, num_changes);
  free_unit_changes(changes, num_changes);
  return changes_list;
}

static int daemon_reload(char *errbuf, size_t errbuf_len) {
  sd_bus *bus = NULL;
  sd_bus_error err = SD_BUS_ERROR_NULL;
  sd_bus_message *reply = NULL;

  memset(errbuf, 0, errbuf_len);

  int r = sd_bus_open_system(&bus);
  if (r < 0) {
    snprintf(errbuf, errbuf_len, "Failed to connect to system bus: %s",
             strerror(-r));
    goto cleanup;
  }

  r = sd_bus_call_method(
      bus, "org.freedesktop.systemd1", "/org/freedesktop/systemd1",
      "org.freedesktop.systemd1.Manager", "Reload", &err, &reply, "");

  if (r < 0) {
    if (err.message) {
      snprintf(errbuf, errbuf_len, "%s", err.message);
    } else {
      snprintf(errbuf, errbuf_len, "Daemon reload failed: %s", strerror(-r));
    }
  }
cleanup:
  sd_bus_error_free(&err);
  sd_bus_message_unref(reply);
  sd_bus_unref(bus);
  return r < 0 ? r : 0;
}

PyObject *py_daemon_reload(PyObject *self, PyObject *args) {
  char errbuf[1024] = {0};
  int r;

  // clang-format off
  Py_BEGIN_ALLOW_THREADS
    r = daemon_reload(errbuf, sizeof(errbuf));
  Py_END_ALLOW_THREADS

  if (r < 0) {
    PyErr_SetString(SystemdDBusError, errbuf);
    return NULL;
  }
  // clang-format on
  // This is actually returning, ignore any static checkers
  Py_RETURN_NONE;
}

static struct PyMethodDef sdbus_methods[] = {
    {"start_unit", py_start_unit, METH_VARARGS,
     "Start a Systemd unit file on the system bus"},
    {"stop_unit", py_stop_unit, METH_VARARGS,
     "Stop a Systemd unit file on the system bus"},
    {"restart_unit", py_restart_unit, METH_VARARGS,
     "Restart a Systemd unit file on the system bus"},
    {"get_unit_property", py_get_unit_property, METH_VARARGS,
     "Get a property of a Systemd unit on the system bus"},
    {"enable_unit", py_enable_unit, METH_VARARGS,
     "Enable a Systemd unit file on the system bus"},
    {"disable_unit", py_disable_unit, METH_VARARGS,
     "Disable a systemd unit file on the system bus"},
    {"daemon_reload", py_daemon_reload, METH_NOARGS,
     "Reload the Systemd daemon"},
    {"check_dbus_available", py_check_dbus_available, METH_NOARGS,
     "Check if DBus is available to use"},
    {"get_property", py_get_property, METH_VARARGS, "Get a Systemd Property"},
    // The last one must always be NULL
    {NULL, NULL, 0, NULL}};

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

  SystemdDBusError =
      PyErr_NewException("_sdbus.SystemdDBusError", PyExc_OSError, NULL);

  if (!SystemdDBusError) {
    return NULL;
  }
  Py_INCREF(SystemdDBusError);
  PyModule_AddObject(m, "SystemdDBusError", SystemdDBusError);
  return m;
}

#if PY_MAJOR_VERSION >= 3
PyMODINIT_FUNC PyInit__sdbus(void) { return _init_module(); }
#else
PyMODINIT_FUNC init_sdbus(void) { _init_module(); }
#endif
