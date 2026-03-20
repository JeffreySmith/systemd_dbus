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
#include <assert.h>
#include <errno.h>
#include <inttypes.h>
#include <stddef.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
// #include <systemd/sd-bus.h>
#include "sd-bus.h"

const PropertyInfo known_properties[] = {
    {"ActiveState", "org.freedesktop.systemd1.Unit", "s"},
    {"SubState", "org.freedesktop.systemd1.Unit", "s"},
    {"LoadState", "org.freedesktop.systemd1.Unit", "s"},
    {"UnitFileState", "org.freedesktop.systemd1.Unit", "s"},
    {"MainPID", "org.freedesktop.systemd1.Service", "u"},
    {"ExecMainCode", "org.freedesktop.systemd1.Service", "i"},
    {"ExecMainStatus", "org.freedesktop.systemd1.Service", "i"},
    {NULL, NULL, NULL}};

/**
 * @brief A lookup function to find the interface and type for a given property
 * name, if it's one of the known properties we care about. This is used to
 * avoid hardcoding the interface and type in multiple places and to provide a
 * single source of truth for the properties we support.
 * @param property The name of the property to look up
 *
 * @return Either a pointer to a PropertyInfo struct containing the interface
 * and type for the property, or NULL if not found
 */
const PropertyInfo *lookup_property(const char *property) {
  for (const PropertyInfo *p = known_properties; p->property; p++) {
    if (strcmp(p->property, property) == 0) {
      return p;
    }
  }
  return NULL;
}

/**
 * @brief Frees an array of UnitChange structs, including any dynamically
 * allocated strings inside
 * @param[in] changes The array to free
 * @param[in] num The number of elements in the array
 */
void free_unit_changes(UnitChange *changes, size_t num) {
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

/**
 *  @brief Reads an array of UnitChange structs from a dbus message reply, which
 * comes from enabling and disabling units. The message is expected to have the
 * signature "ab(sss)", where the first element is a boolean indicating whether
 * the changes carry install info, and the second element is an array of structs
 * with three strings (type, symlink path, and destination).
 *
 * @param[in] reply The dbus message to read from
 * @param[out] changes_out Output parameter to write the allocated array of
 * UnitChange structs
 * @param[in] num_changes_out The number of changes read and written to
 * changes_out
 * @param[out] errbuf Buffer to write error message to if an error occurs
 * @param[in] errbuf_len Length of the error buffer
 *
 * @return 0 on success, negative errno value on failure
 */

int read_unit_changes(sd_bus_message *reply, UnitChange **changes_out,
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
    changes[num_changes] = (UnitChange){NULL, NULL, NULL};
    changes[num_changes].type = strdup(ctype);
    changes[num_changes].symlink_path = strdup(symlink_path);
    changes[num_changes].dest = strdup(dest);

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

/**
 * @brief Checks if dbus is accessible and systemd1 is available on the bus by
 * pinging it. This can be used before initiating a persistent connection to
 * dbus
 * @param[out] errbuf Buffer to write error message to if dbus is not available
 * or systemd1 is not on the bus
 * @param[in] errbuf_len Length of the error buffer
 * @return 0 if dbus is available and systemd1 is on the bus, negative errno
 * otherwise
 */
int check_dbus_available(char *errbuf, size_t errbuf_len) {
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
/**
 * @brief Checks if dbus is accessible and systemd1 is available on the bus by
 * pinging it. This should be used after a persistent connection has been
 * established.
 * @param[in] bus The dbus connection to use
 * @param[out] errbuf Buffer to write error message to if dbus is not available
 * or systemd1 is not on the bus
 * @param[in] errbuf_len Length of the error buffer
 * @return 0 if dbus is available and systemd1 is on the bus, negative errno
 * otherwise
 */

int ping_dbus(sd_bus *bus, char *errbuf, size_t errbuf_len) {
  sd_bus_error err = SD_BUS_ERROR_NULL;
  sd_bus_message *reply = NULL;
  int r;
  memset(errbuf, 0, errbuf_len);
  if (bus == NULL) {
    snprintf(errbuf, errbuf_len, "Bus connection is not initialized");
    return -ENOTCONN;
  }

  r = sd_bus_call_method(bus, "org.freedesktop.systemd1",
                         "/org/freedesktop/systemd1",
                         "org.freedesktop.DBus.Peer", "Ping", &err, &reply, "");

  if (r < 0) {
    if (errbuf && err.message) {
      snprintf(errbuf, errbuf_len, "Failed to ping dbus: %s", err.message);
    } else {
      snprintf(errbuf, errbuf_len, "Failed to ping dbus: %s", strerror(-r));
    }
  }

  sd_bus_error_free(&err);
  sd_bus_message_unref(reply);
  return r < 0 ? r : 0;
}

/**
 * @brief Calls a method (StartUnit, StopUnit, RestartUnit) on the systemd1
 * Manager interface for a particular unit
 * @param[in] bus The dbus connection to use
 * @param[in] method The method to call (e.g. "StartUnit")
 * @param[in] unit The unit to call the method on (e.g. "nginx.service")
 * @param[out] errbuf Buffer to write error message to if an error occurs
 * @param[in] errbuf_len Length of the error buffer
 * @return 0 on success, negative errno on failure
 *
 */
int call_method(sd_bus *bus, const char *method, const char *unit, char *errbuf,
                size_t errbuf_len) {
  sd_bus_error err = SD_BUS_ERROR_NULL;
  sd_bus_message *reply = NULL;

  // Return code for any issues
  int r;
  memset(errbuf, 0, errbuf_len);
  if (bus == NULL) {
    snprintf(errbuf, errbuf_len, "Bus connection is not initialized");
    return -ENOTCONN;
  }

  r = sd_bus_call_method(bus, "org.freedesktop.systemd1",
                         "/org/freedesktop/systemd1",
                         "org.freedesktop.systemd1.Manager", method, &err,
                         &reply, "ss", unit, "replace");

  if (r < 0) {
    snprintf(errbuf, errbuf_len, "%s",
             err.message ? err.message : strerror(-r));
  }

  sd_bus_error_free(&err);
  sd_bus_message_unref(reply);
  return r < 0 ? r : 0;
}

/**
 * @brief Read a message from dbus and extract a value of a given type into a
 * DBusValue struct
 *
 * @param[in] reply The dbus message to read from
 * @param[in] type The expected type of the value (e.g. "s" for string, "u" for
 * uint32_t)
 * @param[out] out Pointer to a DBusValue struct to write the result into
 * @param[out] errbuf Buffer to write error message to if an error occurs
 * @param[in] errbuf_len Length of the error buffer
 * @return 0 on success, negative errno on failure
 *
 */
int read_message_value(sd_bus_message *reply, const char *type, DBusValue *out,
                       char *errbuf, size_t errbuf_len) {
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

/**
 * @brief Get a property of a systemd unit file
 *
 * @param[in] bus The dbus connection to use
 * @param[in] unit_name The name of the unit (e.g. "nginx.service")
 * @param[in] property The name of the property to get (e.g. "MainPID")
 * @param[in] interface The D-Bus interface the property belongs to
 * @param[in] type The expected D-Bus type of the property value (e.g. "u" for
 * uint32_t)
 * @param[out] out Pointer to a DBusValue struct to write the result into
 * @param[out] errbuf Buffer to write error message to if an error occurs
 * @param[in] errbuf_len Length of the error buffer
 * @return 0 on success, negative errno on failure
 */

int get_unit_property_raw(sd_bus *bus, const char *unit_name,
                          const char *property, const char *interface,
                          const char *type, DBusValue *out, char *errbuf,
                          size_t errbuf_len) {

  sd_bus_error err = SD_BUS_ERROR_NULL;
  sd_bus_message *reply = NULL;
  char *unit_path = NULL;
  int r;

  memset(errbuf, 0, errbuf_len);
  if (bus == NULL) {
    snprintf(errbuf, errbuf_len, "Bus connection is not initialized");
    return -ENOTCONN;
  }

  r = sd_bus_call_method(bus, "org.freedesktop.systemd1",
                         "/org/freedesktop/systemd1",
                         "org.freedesktop.systemd1.Manager", "GetUnit", &err,
                         &reply, "s", unit_name);

  if (r < 0) {
    snprintf(errbuf, errbuf_len, "GetUnit failed: %s",
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
    snprintf(errbuf, errbuf_len, "get_property(%s, %s) failed [%s]: %s",
             interface, property, err.name ? err.name : "no error name",
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
  free(unit_path);
  return r < 0 ? r : 0;
}

/**
 * @brief Get a property of a D-Bus object at an arbitrary destination and path
 *
 * @param[in] bus The dbus connection to use
 * @param[in] destination The D-Bus destination (e.g.
 * "org.freedesktop.systemd1")
 * @param[in] path The D-Bus object path (e.g. "/org/freedesktop/timedate1")
 * @param[in] interface The D-Bus interface the property belongs to (e.g.
 * "org.freedesktop.timedate1")
 * @param[in] property The name of the property to get (e.g. "Timezone")
 * @param[in] type The expected D-Bus type of the property value (e.g. "s" for
 * string)
 * @param[out] out Pointer to a DBusValue struct to write the result into
 * @param[out] errbuf Buffer to write error message to if an error occurs
 * @param[in] errbuf_len Length of the error buffer
 * @return 0 on success, negative errno on failure
 */

int get_property(sd_bus *bus, const char *destination, const char *path,
                 const char *interface, const char *property, const char *type,
                 DBusValue *out, char *errbuf, size_t errbuf_len) {
  sd_bus_error err = SD_BUS_ERROR_NULL;
  sd_bus_message *reply = NULL;
  int r;

  memset(errbuf, 0, errbuf_len);
  if (bus == NULL) {
    snprintf(errbuf, errbuf_len, "Bus connection is not initialized");
    return -ENOTCONN;
  }

  r = sd_bus_get_property(bus, destination, path, interface, property, &err,
                          &reply, type);
  if (r < 0) {
    snprintf(errbuf, errbuf_len, "%s",
             err.message ? err.message : strerror(-r));
    goto cleanup;
  }

  r = read_message_value(reply, type, out, errbuf, errbuf_len);

cleanup:
  sd_bus_error_free(&err);
  sd_bus_message_unref(reply);
  return r < 0 ? r : 0;
}

/**
 *
 * @brief Enables a systemd unit file by calling the EnableUnitFiles method on
 * the system bus
 *
 * @param[in] bus The dbus connection to use
 * @param[in] unit_name The name of the unit file to enable (e.g.
 * "nginx.service")
 * @param[out] carries_install_info Output parameter that will be set if the
 * unit has an install section
 * @param[out] changes_out Output parameter that will point to an allocated
 * array of UnitChange structs describing the changes made by enabling the unit
 * (e.g. symlinks created)
 * @param[out] num_changes_out Output parameter that will be set to the number
 * of changes in the changes_out array
 * @param[out] errbuf Buffer to write error message to if an error occurs
 * @param[in] errbuf_len Length of the error buffer
 *
 * @return 0 on success, negative error code on failure
 */
int enable_unit(sd_bus *bus, const char *unit_name, int *carries_install_info,
                UnitChange **changes_out, size_t *num_changes_out, char *errbuf,
                size_t errbuf_len) {
  sd_bus_error err = SD_BUS_ERROR_NULL;
  sd_bus_message *msg = NULL;
  sd_bus_message *reply = NULL;
  int r;

  memset(errbuf, 0, errbuf_len);
  *carries_install_info = 0;
  *changes_out = NULL;
  *num_changes_out = 0;
  if (bus == NULL) {
    snprintf(errbuf, errbuf_len, "Bus connection is not initialized");
    return -ENOTCONN;
  }

  r = sd_bus_message_new_method_call(
      bus, &msg, "org.freedesktop.systemd1", "/org/freedesktop/systemd1",
      "org.freedesktop.systemd1.Manager", "EnableUnitFiles");
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

  // runtime=false, force=true
  r = sd_bus_message_append(msg, "bb", 0, 1);
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

  int carries = 0;
  r = sd_bus_message_read(reply, "b", &carries);
  if (r < 0) {
    snprintf(errbuf, errbuf_len, "Failed to read carries_install_info: %s",
             strerror(-r));
    goto cleanup;
  }
  *carries_install_info = carries;

  r = read_unit_changes(reply, changes_out, num_changes_out, errbuf,
                        errbuf_len);

cleanup:
  sd_bus_error_free(&err);
  sd_bus_message_unref(msg);
  sd_bus_message_unref(reply);
  return r < 0 ? r : 0;
}
/**
 * @brief Disables a systemd unit file by calling the DisableUnitFiles method on
 * the system bus
 *
 * @param[in] bus The dbus connection to use
 * @param[in] unit_name The name of the unit file to disable (e.g.
 * "nginx.service")
 * @param[out] changes_out Output parameter that will point to an allocated
 * array of UnitChange structs describing the changes made by disabling the unit
 * (e.g. symlinks removed)
 * @params[out] num_changes_out Output parameter that will be set to the number
 * of changes in the changes_out array
 * @param[out] errbuf Buffer to write error message to if an error occurs
 * @param[in] errbuf_len
 * @return 0 on success, negative error code on failure
 */
int disable_unit(sd_bus *bus, const char *unit_name, UnitChange **changes_out,
                 size_t *num_changes_out, char *errbuf, size_t errbuf_len) {
  sd_bus_error err = SD_BUS_ERROR_NULL;
  sd_bus_message *msg = NULL;
  sd_bus_message *reply = NULL;
  int r;

  memset(errbuf, 0, errbuf_len);
  if (bus == NULL) {
    snprintf(errbuf, errbuf_len, "Bus connection is not initialized");
    return -ENOTCONN;
  }

  *changes_out = NULL;
  *num_changes_out = 0;

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

  r = read_unit_changes(reply, changes_out, num_changes_out, errbuf,
                        errbuf_len);

cleanup:
  sd_bus_error_free(&err);
  sd_bus_message_unref(msg);
  sd_bus_message_unref(reply);
  return r < 0 ? r : 0;
}

/**
 * @brief Reloads the systemd manager configuration by calling the Reload method
 * on the system bus
 *
 * @param[in] bus The dbus connection to use
 * @param[out] errbuf Buffer to write error message to if an error occurs
 * @param[in] errbuf_len Length of the error buffer
 *
 * @return 0 on success, negative error code on failure
 */
int daemon_reload(sd_bus *bus, char *errbuf, size_t errbuf_len) {
  sd_bus_error err = SD_BUS_ERROR_NULL;
  sd_bus_message *reply = NULL;

  int r;
  memset(errbuf, 0, errbuf_len);
  if (bus == NULL) {
    snprintf(errbuf, errbuf_len, "Bus connection is not initialized");
    return -ENOTCONN;
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
  sd_bus_error_free(&err);
  sd_bus_message_unref(reply);
  return r < 0 ? r : 0;
}
