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
#pragma once

#include <stddef.h>
#include <stdint.h>
#include <stdbool.h>
#include <systemd/sd-bus.h>

typedef struct {
  const char *property;
  const char *interface;
  const char *type;
} PropertyInfo;


typedef struct {
  char type;
  union {
    uint8_t  y;   // BYTE
    int16_t  n;   // INT16
    uint16_t q;   // UINT16
    int32_t  i;   // INT32
    uint32_t u;   // UINT32
    int64_t  x;   // INT64
    uint64_t t;   // UINT64
    double   d;   // DOUBLE
    int      b;   // BOOLEAN
    int      h;   // UNIX_FD
  } val;
  char s_buf[1024]; // STRING ('s'), OBJECT PATH ('o'), SIGNATURE ('g')
} DBusValue;

typedef struct {
  char *type;
  char *symlink_path;
  char *dest;
} UnitChange;

extern const PropertyInfo known_properties[];

int check_dbus_available(char *errbuf, size_t errbuf_len);

int ping_dbus(sd_bus *bus, char *errbuf, size_t errbuf_len);

int call_method(sd_bus *bus, const char *method, const char *unit, char *errbuf,
                       size_t errbuf_len);

int get_unit_property_raw(sd_bus *bus, const char *unit_name, const char *property,
                                 const char *interface, const char *type,
                                 DBusValue *out, char *errbuf,
                                 size_t errbuf_len);

int get_property(sd_bus *bus, const char *destination, const char *path,
                        const char *interface, const char *property,
                        const char *type, DBusValue *out,
                        char *errbuf, size_t errbuf_len);

int enable_unit(sd_bus *bus, const char *unit_name, int *carries_install_info,
                       UnitChange **changes_out, size_t *num_changes_out,
                       char *errbuf, size_t errbuf_len);

int disable_unit(sd_bus *bus, const char *unit_name, UnitChange **changes_out,
                        size_t *num_changes_out, char *errbuf,
                        size_t errbuf_len);

int daemon_reload(sd_bus *bus, char *errbuf, size_t errbuf_len);

const PropertyInfo *lookup_property(const char *property);

bool valid_property_type(char type);

void free_unit_changes(UnitChange *changes, size_t num);
