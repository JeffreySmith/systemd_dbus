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
#include <assert.h>
#include <errno.h>
#include <inttypes.h>
#include <stddef.h>
#include <stdio.h>
#include <systemd/sd-bus.h>

static int call_method(const char *method, const char *types, const char *arg1,
                       const char *arg2, char *errbuf, size_t errbuf_len) {
  sd_bus *bus = NULL;
  sd_bus_error err = SD_BUS_ERROR_NULL;
  sd_bus_message *reply = NULL;
  int r;

  r = sd_bus_open_system(&bus);
  if (r < 0) {
    if (errbuf)
      snprintf(errbuf, errbuf_len, "Failed to connect to system bus: %s",
               strerror(-r));
    goto cleanup;
  }

  r = sd_bus_call_method(bus, "org.freedesktop.systemd1",
                         "/org/freedesktop/systemd1",
                         "org.freedesktop.systemd1.Manager", method, &err,
                         &reply, types, arg1, arg2);
  if (r < 0) {
    if (errbuf && err.message)
      snprintf(errbuf, errbuf_len, "%s", err.message);
    else if (errbuf)
      snprintf(errbuf, errbuf_len, "%s failed: %s", method, strerror(-r));
  }

cleanup:
  sd_bus_error_free(&err);
  sd_bus_message_unref(reply);
  sd_bus_unref(bus);
  return r < 0 ? r : 0;
}

int check_dbus_available(char *errbuf, size_t errbuf_len) {
  sd_bus *bus = NULL;
  sd_bus_error err = SD_BUS_ERROR_NULL;
  sd_bus_message *reply = NULL;
  int r;

  r = sd_bus_open_system(&bus);
  if (r < 0) {
    if (errbuf) {
      snprintf(errbuf, errbuf_len, "Cannot open system bus: %s", strerror(-r));
    }
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

int start_unit(const char *unit, char *errbuf, size_t errbuf_len) {
  return call_method("StartUnit", "ss", unit, "replace", errbuf, errbuf_len);
}
int stop_unit(const char *unit, char *errbuf, size_t errbuf_len) {
  return call_method("StopUnit", "ss", unit, "replace", errbuf, errbuf_len);
}
int restart_unit(const char *unit, char *errbuf, size_t errbuf_len) {
  return call_method("RestartUnit", "ss", unit, "replace", errbuf, errbuf_len);
}

static int read_message_value(sd_bus_message *reply, const char *type,
                              char *result, size_t result_len, char *errbuf,
                              size_t errbuf_len) {
  int r;

  switch (type[0]) {
  case 's': {
    const char *val = NULL;
    r = sd_bus_message_read(reply, "s", &val);
    if (r >= 0) {
      snprintf(result, result_len, "%s", val ? val : "");
    }
    break;
  }
  case 'u': {
    uint32_t val = 0;
    r = sd_bus_message_read(reply, type, &val);
    if (r >= 0) {
      snprintf(result, result_len, "%u", val);
    }
    break;
  }
  case 'i': {
    int32_t val = 0;
    r = sd_bus_message_read(reply, type, &val);
    if (r >= 0) {
      snprintf(result, result_len, "%d", val);
    }
  }
  case 't': {
    uint64_t val = 0;
    r = sd_bus_message_read(reply, type, &val);
    if (r >= 0) {
      snprintf(result, result_len, "%" PRIu64, val);
    }
    break;
  }
  case 'x': {
    int64_t val = 0;
    r = sd_bus_message_read(reply, type, &val);
    if (r >= 0) {
      snprintf(result, result_len, "%" PRId64, val);
    }
    break;
  }
  case 'b': {
    int val = 0;
    r = sd_bus_message_read(reply, "b", &val);
    if (r >= 0) {
      snprintf(result, result_len, "%d", val);
    }
    break;
  }
  default:
    snprintf(errbuf, errbuf_len, "Unsupported type: %s", type);
    r = -EINVAL;
    break;
  }

  if (r < 0 && errbuf && !errbuf[0])
    snprintf(errbuf, errbuf_len, "Failed to read value: %s", strerror(-r));
  return r;
}

int get_unit_property(const char *unit_name, const char *interface,
                      const char *property, const char *type, char *result,
                      size_t result_len, char *errbuf, size_t errbuf_len) {
  sd_bus *bus = NULL;
  sd_bus_error err = SD_BUS_ERROR_NULL;
  sd_bus_message *reply = NULL;
  int r;

  r = sd_bus_open_system(&bus);
  if (r < 0) {
    snprintf(errbuf, errbuf_len, "Failed to connect to system bus: %s",
             strerror(-r));
    goto cleanup;
  }

  char unit_path[256] = {0};

  r = sd_bus_call_method(bus, "org.freedesktop.systemd1",
                         "/org/freedesktop/systemd1",
                         "org.freedesktop.systemd1.Manager", "GetUnit", &err,
                         &reply, "s", unit_name);
  if (r < 0) {
    if (err.message) {
      snprintf(errbuf, errbuf_len, "%s", err.message);
    } else {
      snprintf(errbuf, errbuf_len, "Failed to get unit %s: %s", unit_name,
               strerror(-r));
    }
    goto cleanup;
  }

  const char *path = NULL;
  r = sd_bus_message_read(reply, "o", &path);
  if (r < 0) {
    snprintf(errbuf, errbuf_len, "Failed to read unit path: %s", strerror(-r));
    goto cleanup;
  }
  snprintf(unit_path, sizeof(unit_path), "%s", path);

  // Release before reusing
  sd_bus_message_unref(reply);
  reply = NULL;
  sd_bus_error_free(&err);
  err = SD_BUS_ERROR_NULL;

  r = sd_bus_get_property(bus, "org.freedesktop.systemd1", unit_path, interface,
                          property, &err, &reply, type);

  if (r < 0) {
    if (err.message) {
      snprintf(errbuf, errbuf_len, "%s", err.message);
    } else {
      snprintf(errbuf, errbuf_len, "Failed to get property %s: %s", property,
               strerror(-r));
    }
    goto cleanup;
  }

  r = read_message_value(reply, type, result, result_len, errbuf, errbuf_len);

cleanup:
  sd_bus_error_free(&err);
  sd_bus_message_unref(reply);
  sd_bus_unref(bus);
  return r < 0 ? r : 0;
}

int get_property(
    const char *destination, const char *path, const char *interface,
    const char *property,
    const char *type, // single dbus type char: "s", "u", "b", "t", etc.
    char *result, size_t result_len, char *errbuf, size_t errbuf_len) {

  // If the buffers are given a length of zero, they will not be null
  // terminated.
  assert(result_len > 0);
  assert(errbuf_len > 0);

  sd_bus *bus = NULL;
  sd_bus_error err = SD_BUS_ERROR_NULL;
  sd_bus_message *reply = NULL;
  int r;

  r = sd_bus_open_system(&bus);
  if (r < 0) {
    if (errbuf) {
      snprintf(errbuf, errbuf_len, "Failed to connect to system bus: %s",
               strerror(-r));
    }
    goto cleanup;
  }

  r = sd_bus_get_property(bus, destination, path, interface, property, &err,
                          &reply, type);
  if (r < 0) {
    if (errbuf && err.message) {
      snprintf(errbuf, errbuf_len, "%s", err.message);
    } else if (errbuf) {
      snprintf(errbuf, errbuf_len, "Failed to get %s: %s", property,
               strerror(-r));
    }
    goto cleanup;
  }

  r = read_message_value(reply, type, result, result_len, errbuf, errbuf_len);

cleanup:
  sd_bus_error_free(&err);
  sd_bus_message_unref(reply);
  sd_bus_unref(bus);
  return r < 0 ? r : 0;
}

int enable_unit(const char *unit_name, char *errbuf, size_t errbuf_len) {
  sd_bus *bus = NULL;
  sd_bus_error err = SD_BUS_ERROR_NULL;
  sd_bus_message *msg = NULL;
  sd_bus_message *reply = NULL;

  int r;
  r = sd_bus_open_system(&bus);
  if (r < 0) {
    snprintf(errbuf, errbuf_len, "Failed to connect to system bus: %s",
             strerror(-r));
    goto cleanup;
  }

  r = sd_bus_message_new_method_call(
      bus, &msg, "org.freedesktop.systemd1", "/org/freedesktop/systemd1",
      "org.freedesktop.systemd1.Manager", "EnableUnitFiles");

  if (r < 0) {
    snprintf(errbuf, errbuf_len, "Failed to create message: %s", strerror(-r));
    goto cleanup;
  }
  // Start an array
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

  // runtime=false, force=true for the magic numbers
  r = sd_bus_message_append(msg, "bb", 0, 1);

  if (r < 0) {
    snprintf(errbuf, errbuf_len, "Failed to append arguments: %s",
             strerror(-r));
    goto cleanup;
  }

  r = sd_bus_call(bus, msg, 0, &err, &reply);
  if (r < 0) {
    if (err.message) {
      snprintf(errbuf, errbuf_len, "%s", err.message);
    } else {
      snprintf(errbuf, errbuf_len, "Enable unit file failed: %s", strerror(-r));
    }
    goto cleanup;
  }

cleanup:
  sd_bus_error_free(&err);
  sd_bus_message_unref(msg);
  sd_bus_message_unref(reply);
  sd_bus_unref(bus);
  return r < 0 ? r : 0;
}

int enable_unit(const char *unit_name, char *errbuf, size_t errbuf_len) {
  sd_bus *bus = NULL;
  sd_bus_error err = SD_BUS_ERROR_NULL;
  sd_bus_message *msg = NULL;
  sd_bus_message *reply = NULL;

  int r;
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
  // Start an array
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

  // runtime=false, force=true for the magic numbers
  r = sd_bus_message_append(msg, "bb", 0, 1);

  if (r < 0) {
    snprintf(errbuf, errbuf_len, "Failed to append arguments: %s",
             strerror(-r));
    goto cleanup;
  }

  r = sd_bus_call(bus, msg, 0, &err, &reply);
  if (r < 0) {
    if (err.message) {
      snprintf(errbuf, errbuf_len, "%s", err.message);
    } else {
      snprintf(errbuf, errbuf_len, "Disable unit file failed: %s",
               strerror(-r));
    }
    goto cleanup;
  }

cleanup:
  sd_bus_error_free(&err);
  sd_bus_message_unref(msg);
  sd_bus_message_unref(reply);
  sd_bus_unref(bus);
  return r < 0 ? r : 0;
}
