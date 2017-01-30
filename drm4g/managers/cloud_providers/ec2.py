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

import os
import time
import threading
from libcloud.compute.types          import Provider
from libcloud.compute.providers      import get_driver
from libcloud.compute.types          import NodeState
from drm4g.managers.cloud_providers  import Instance, logger

class EC2(Instance):

    def __init__(self, basic_data):
        self.node = None
        self._lock = threading.Lock()
        try :
            access_id = basic_data[ 'access_id' ]
            secret_key = basic_data[ 'secret_key' ]
        except :
            raise Exception( "No correct auth data has been specified."
                             "Please review your 'access_id' and 'secret_key'" )

        region = basic_data.get('region', 'eu-west-1')
        cls = get_driver(Provider.EC2)
        try:
            self.driver = cls( access_id, secret_key, region = region )
        except:
            raise Exception( "No correct region has been specified."
                             "Please review your 'region'" )

        size_id = basic_data.get('size', 't2.micro')
        try:
            self.size = [s for s in self.driver.list_sizes() if s.id == size_id ][0]
        except:
            raise Exception( "No correct size_id has been specified."
                             "Please review your 'size_id'" )
        self.instance_pricing = float(basic_data.get('pricing', self.size.price))

        image_id = basic_data.get('image', 'ami-c51e3eb6')
        try:
            self.image = self.driver.get_image( image_id )
        except:
            raise Exception( "No correct image_id has been specified."
                             "Please review your 'image_id'" )

        # Note: This key will be added to the authorized keys for the user admin
        try:
            public_key_path = os.path.expanduser(basic_data.get('public_key', '~/.ssh/id_rsa.pub'))
        except :
            raise Exception( "No correct public_key has been specified."
                             "Please review your 'public_key'" )
        with open(public_key_path) as fp:
            public_key = fp.read().strip()
        self.deployment = self.generate_cloud_config(public_key, user=basic_data.get('cloud_user', self.DEFAULT_USER),
                                                     user_cloud_config=basic_data.get('cloud_config_script'))

    def create_security_group(self):
        if not self.SECURITY_GROUP_NAME in self.driver.ex_list_security_groups():
            self.driver.ex_create_security_group(name=self.SECURITY_GROUP_NAME, description="DRM4G group open ssh port")
            self.driver.ex_authorize_security_group(name=self.SECURITY_GROUP_NAME, protocol='tcp', from_port=22,
                                                    to_port=22, cidr_ip='0.0.0.0/0')

    def create(self):
        with self._lock:
            self.create_security_group()
        self.node = self.driver.create_node(name='drm4g_VM', image=self.image, size=self.size,
                    ex_security_groups=[self.SECURITY_GROUP_NAME], ex_userdata=self.deployment)
        self._start_time()

    def wait_until_running(self):
        start = time.time()
        end = start + self.TIMEOUT

        while time.time() < end:
            node = self.driver.list_nodes([self.node.id])[0]
            if node.state == NodeState.RUNNING:
                return node.public_ips[0]
            else:
                time.sleep(self.WAIT_PERIOD)
                continue

        raise Exception("Timed out after %s seconds" % (self.TIMEOUT))

    def destroy(self):
        self.driver.destroy_node(self.node)

    def get_public_ip(self):
        if self.node:
            node = self.driver.list_nodes([self.node.id])[0]
            if node.state == NodeState.RUNNING:
                return node.public_ips[0]
            else:
                return None
        else:
            return None

    def get_private_ip(self):
        if self.node:
            return self.node.private_ips[0]
        else:
            return None
