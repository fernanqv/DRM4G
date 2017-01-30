#
# Copyright 2016 Universidad de Cantabria
#
# Licensed under the EUPL, Version 1.1 only (the
# "Licence");
# You may not use this work except in compliance with the
# Licence.
# You may obtain a copy of the Licence at:
#
# http://ec.europa.eu/idabc/eupl
#
# Unless required by applicable law or agreed to in
# writing, software distributed under the Licence is
# distributed on an "AS IS" basis,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either
# express or implied.
# See the Licence for the specific language governing
# permissions and limitations under the Licence.
#

import os.path
import time
import logging
from math           import ceil
from drm4g.managers import logger

#logger = logging.getLogger(__name__)

class Instance(object):

    DEFAULT_USER = "drm4gadm"
    SECURITY_GROUP_NAME = "drm4g_group"
    TIMEOUT = 600 # seconds
    WAIT_PERIOD = 3 # seconds
    instance_pricing = 0.0
    start_time = 0.0

    def __init__(self, basic_data=None):
        pass

    def create(self):
        pass

    def destroy(self):
        pass

    def get_public_ip(self):
        pass

    def get_private_ip(self):
        pass

    def create_security_group(self):
        pass

    def wait_until_running(self):
        pass

    def _start_time(self):
        self.start_time = time.time()

    def running_time(self):
        if not self.start_time:
            return 0
        else:
            return (time.time() - self.start_time)/3600

    def current_balance(self):
        running_hours = ceil(self.running_time())
        return running_hours * self.instance_pricing

    def generate_cloud_config(self, public_key, user=None, user_cloud_config=None):
        """
        Generate the cloud-config file to be used in the user_data
        """
        if not user:
            user = self.DEFAULT_USER
        config = """#cloud-config
users:
  - name: %s
    sudo: ALL=(ALL) NOPASSWD:ALL
    lock-passwd: true
    ssh-import-id: %s
    ssh-authorized-keys:
      - %s
""" % (user, user, public_key)
        if user_cloud_config:
            config += "\n%s\n\n" % user_cloud_config.replace("\\n", "\n")
        return config