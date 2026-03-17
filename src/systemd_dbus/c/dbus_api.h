#pragma once

#include <stddef.h>
#include <stdint.h>
#include <systemd/sd-bus.h>

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

const PropertyInfo known_properties[] = {
    {"ActiveState", "org.freedesktop.systemd1.Unit", "s"},
    {"SubState", "org.freedesktop.systemd1.Unit", "s"},
    {"LoadState", "org.freedesktop.systemd1.Unit", "s"},
    {"UnitFileState", "org.freedesktop.systemd1.Unit", "s"},
    {"MainPID", "org.freedesktop.systemd1.Service", "u"},
    {"ExecMainCode", "org.freedesktop.systemd1.Service", "i"},
    {"ExecMainStatus", "org.freedesktop.systemd1.Service", "i"},
    {NULL, NULL, NULL}};

int check_dbus_available(char *errbuf, size_t errbuf_len);

int call_method(const char *method, const char *unit, char *errbuf,
                       size_t errbuf_len);

int read_message_value(sd_bus_message *reply, const char *type,
                              DBusValue *out, char *errbuf, size_t errbuf_len);

int get_unit_property_raw(const char *unit_name, const char *property,
                                 const char *interface, const char *type,
                                 DBusValue *out, char *errbuf,
                                 size_t errbuf_len);

int get_property(const char *destination, const char *path,
                        const char *interface, const char *property,
                        const char *type, DBusValue *out,
                        char *errbuf, size_t errbuf_len);

int enable_unit(const char *unit_name, int *carries_install_info,
                       UnitChange **changes_out, size_t *num_changes_out,
                       char *errbuf, size_t errbuf_len);

int disable_unit(const char *unit_name, UnitChange **changes_out,
                        size_t *num_changes_out, char *errbuf,
                        size_t errbuf_len);

int daemon_reload(char *errbuf, size_t errbuf_len);
