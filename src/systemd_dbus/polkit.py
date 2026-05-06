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

from ambari_jinja2.environment import Environment
from resource_management.core import sudo
from subprocess import Popen, PIPE
from resource_management.core.logger import Logger

import os
import re

class PolkitRule:
    """Generate polkit js rules from a jinja2 template."""

    def __init__(self, name, manual_rules=None, prefix_num="40", **kwargs):
        self.name = "{}-".format(prefix_num) + name + ".rules"
        self.values = kwargs
        self.rendered_template = None
        self.env = Environment(trim_blocks=True)
        if manual_rules is not None:
            self.template = self.env.from_string(manual_rules)
        else:
            self.template = self.env.from_string(
                """polkit.addRule(function(action, subject) {
  if(action.id === "org.freedesktop.systemd1.manage-units") {
    var permitted_users = [{% for user in users %}{% if not loop.last %}"{{user}}", {% else %}"{{user}}"{% endif %}{% endfor %}];
    if (action.lookup("unit") === "{{unit_file}}") {
      var verb = action.lookup("verb");
      if (subject.active && permitted_users.indexOf(subject.user) !== -1{% if group %} && subject.isInGroup("{{group}}"){%endif%}) {
        if (verb === "start" || verb === "stop" || verb === "restart") {
          polkit.log("User '" + subject.user + "' was permitted to '" + verb + "' unit file '{{unit_file}}'")
          return polkit.Result.YES;
        }
      } {% if ambari_user -%}
      else if (subject.user === "{{ambari_user}}"{% if ambari_group %} && subject.isInGroup("{{ambari_group}}") {% endif %}) {
        if (verb === "start" || verb === "stop" || verb === "restart" || verb === "enable" || verb === "disable") {
          polkit.log("User '" + subject.user + "' was permitted to '" + verb + "' unit file '{{unit_file}}'")
          return polkit.Result.YES;
        }
      } {% endif -%}
      else {
        return polkit.Result.NO;
      }
    }
  }
});
"""
            )

    def render(self):
        """Render the template with the provided values."""
        self.rendered_template = self.template.render(
            trim_blocks=True, lstrip_blocks=True, **self.values
        )
        return self.rendered_template

    def write(self, output_dir="/usr/share/polkit-1/rules.d/"):
        """Write the polkit rule to a file."""
        version = self.version()
        if version is None or version < 106:
            Logger.info(
                "Polkit not installed, or does not support JS rules, skipping writing rule"
            )
            return False
        filename = os.path.join(output_dir, self.name)
        sudo.create_file(filename, self.render(), encoding="utf-8")
        return True

    def version(self):
        """Get the version of polkit installed on the system or None if not available."""
        command = ["pkaction", "--version"]
        try:
            process = Popen(command, stdout=PIPE, stderr=PIPE)
        except OSError as e:
            Logger.info(
                "Unable to execute pkcheck to get polkit version: {0}".format(e)
            )
            return None
        stdout, stderr = process.communicate()
        if stderr:
            Logger.error(stderr.decode())
            return None

        version = stdout.decode().strip()
        matches = re.search(r"([0-9]+)$", version)
        if matches:
            return int(matches.group(0))

        return None

    def __str__(self):
        return self.render()
