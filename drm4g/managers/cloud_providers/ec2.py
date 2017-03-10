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

import time
import threading
from libcloud.compute.types           import Provider
from libcloud.compute.providers       import get_driver
from libcloud.compute.types           import NodeState
from os.path                          import expanduser, join
from drm4g.managers.cloud_providers   import Instance, logger
from drm4g.utils.importlib            import import_module
from drm4g                            import ( COMMUNICATORS,
                                               REMOTE_JOBS_DIR,
                                               REMOTE_VOS_DIR,
                                               DRM4G_DIR )

class Instance(Instance):
    
    _lock = threading.Lock()
    
    def __init__(self, basic_data):
        self.node = None
        #self._lock = threading.Lock()
        try :
            self.access_id = basic_data[ 'access_id' ]
            self.secret_key = basic_data[ 'secret_key' ]
        except :
            raise Exception( "No correct auth data has been specified."
                             "Please review your 'access_id' and 'secret_key'" )

        self.region = basic_data.get('region', 'eu-west-1')
        cls = get_driver(Provider.EC2)
        try:
            self.driver = cls( self.access_id, self.secret_key, region = self.region )
        except:
            raise Exception( "No correct region has been specified."
                             "Please review your 'region'" )

        size_id = basic_data.get('size', 't2.micro') #http://docs.aws.amazon.com/AWSEC2/latest/UserGuide/instance-types.html
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
            public_key_path = expanduser(basic_data.get('public_key', '~/.ssh/id_rsa.pub'))
        except :
            raise Exception( "No correct public_key has been specified."
                             "Please review your 'public_key'" )
        with open(public_key_path) as fp:
            public_key = fp.read().strip()
        #self.deployment = self.generate_cloud_config(public_key, user=basic_data.get('cloud_user', self.DEFAULT_USER),
        #                                             user_cloud_config=basic_data.get('cloud_config_script'))
        
        self.data = basic_data
        self.vm_user = basic_data.get('vm_user', self.DEFAULT_USER)
        self.myproxy_server = basic_data.get('myproxy_server', '')
        self.private_key = expanduser(basic_data['private_key'])
        self.lrms = basic_data.get('lrms')
        self.comm = basic_data[ 'communicator' ]
        self.max_jobs_running = basic_data['max_jobs_running']
        self.vm_comm = basic_data.get('vm_communicator', self.comm)
        if self.vm_comm == 'local':
            self.vm_comm = 'pk_ssh'
        self.cloud_contextualisation_file = basic_data.get('vm_config', join(DRM4G_DIR, "etc", "cloud_config.conf"))
        
        self.deployment = self.generate_cloud_config(public_key, user=self.vm_user,
                                                     user_cloud_config=basic_data.get('cloud_config_script'))
            
        if 'pricing' in basic_data.keys():
            self.instance_pricing = float(basic_data[ 'pricing' ])
            self.soft_billing = float(basic_data.get('soft_billing'))
            self.hard_billing = float(basic_data.get('hard_billing'))
            self.node_safe_time = int(basic_data.get('node_safe_time', 5))
            if not self.soft_billing and not self.hard_billing :
                self.soft_billing = self.hard_billing = 0
            elif self.hard_billing and not self.soft_billing :
                self.soft_billing = self.hard_billing
            elif self.soft_billing and not self.hard_billing :
                self.hard_billing = self.soft_billing
        
        communicator = import_module(COMMUNICATORS[ basic_data[ 'communicator' ] ] )
        com_obj = getattr( communicator , 'Communicator' ) ()
        com_obj.username       = basic_data['username']
        com_obj.frontend       = basic_data['frontend']
        com_obj.private_key    = self.private_key
        com_obj.public_key     = basic_data.get('public_key', self.private_key+'.pub')
        com_obj.work_directory = basic_data.get('scratch', REMOTE_JOBS_DIR)
        self.com_object = com_obj
        
    def __getstate__(self):
        odict = self.__dict__.copy()
        #This is here to avoid having the error "TypeError: can't pickle lock objects" when creating the pickled file
        del odict['com_object']
        #And from here to avoid the error "TypeError: an integer is required"
        del odict['image']
        del odict['size']
        del odict['driver']
        del odict['node']
        return odict

    def __setstate__(self, dict):
        self.__dict__.update(dict)
        communicator = import_module(COMMUNICATORS[ self.data[ 'communicator' ] ] )
        com_obj = getattr( communicator , 'Communicator' ) ()
        com_obj.username       = self.data['username']
        com_obj.frontend       = self.data['frontend']
        com_obj.private_key    = self.private_key
        com_obj.public_key     = self.data.get('public_key', self.private_key+'.pub')
        com_obj.work_directory = self.data.get('scratch', REMOTE_JOBS_DIR)
        self.com_object=com_obj
        cls = get_driver(Provider.EC2)
        self.driver = cls( self.access_id, self.secret_key, region = self.region )
        self.node = self.driver.list_nodes([self.node_id])[0]
        
    def create_security_group(self):
        if not self.SECURITY_GROUP_NAME in self.driver.ex_list_security_groups():
            self.driver.ex_create_security_group(name=self.SECURITY_GROUP_NAME, description="DRM4G group open ssh port")
            self.driver.ex_authorize_security_group(name=self.SECURITY_GROUP_NAME, protocol='tcp', from_port=22,
                                                    to_port=22, cidr_ip='0.0.0.0/0')

    def create(self):
        logger.info( "Creating new EC2 resource" )
        with self._lock:
            self.create_security_group()
        self.node = self.driver.create_node(name='drm4g_VM', image=self.image, size=self.size,
                    ex_security_groups=[self.SECURITY_GROUP_NAME], ex_userdata=self.deployment)
        self.node_id = self.node.id
        self._start_time()
        logger.info( "    EC2 resource '%s' has been successfully created" % self.node_id )

    def wait_until_running(self):
        logger.info( "Waiting until EC2 resource is active" )
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
        logger.info( "Deleting EC2 resource %s" % self.node_id )
        self.driver.destroy_node(self.node)
        logger.info( "    EC2 resource '%s' has been successfully created" % self.node_id )

    def get_ip(self):
        logger.info( "Getting EC2 resource's IP direction" )
        #self.ext_ip = self.get_public_ip()
        self.ext_ip = self.wait_until_running()
        #self.int_ip = self.ext_ip
        logger.info( "    Public IP: %s" % self.ext_ip )
    
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
