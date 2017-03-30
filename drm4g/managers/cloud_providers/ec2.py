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

import uuid
import time
import threading
from libcloud.compute.types           import Provider
from libcloud.compute.providers       import get_driver
from libcloud.compute.types           import NodeState, StorageVolumeState
from os.path                          import expanduser, join
from libcloud.common.exceptions       import BaseHTTPError
from drm4g.managers.cloud_providers   import Instance, logger
from drm4g                            import DRM4G_DIR
from utils                            import read_key 

class Instance(Instance):
    
    DEFAULT_VOLUME_TYPE = 'gp2'
    #the following DEFAULT values correspond to amazon's temporary free tier
    DEFAULT_REGION = 'eu-west-1'
    DEFAULT_SIZE = 't2.micro'
    DEFAULT_IMAGE = 'ami-c51e3eb6'
    
    _lock = threading.Lock()
    
    def __init__(self, basic_data):
        super(Instance, self).__init__()
        self.node = None
        self.volume = None

        try :
            self.access_id = basic_data[ 'access_id' ]
            self.secret_key = basic_data[ 'secret_key' ]
        except :
            raise Exception( "No correct auth data has been specified."
                             "Please review your 'access_id' and 'secret_key'" )

        self.region = basic_data.get('region', self.DEFAILT_REGION)
        cls = get_driver(Provider.EC2)
        try:
            self.driver = cls( self.access_id, self.secret_key, region = self.region )
            self.location = [l for l in self.driver.list_locations() if l.availability_zone.region_name == self.region][0]
        except:
            raise Exception( "No correct region has been specified."
                             "Please review your 'region'" )

        size_id = basic_data.get('size', self.DEFAILT_SIZE) #http://docs.aws.amazon.com/AWSEC2/latest/UserGuide/instance-types.html
        try:
            self.size = [s for s in self.driver.list_sizes() if s.id == size_id ][0]
        except:
            raise Exception( "No correct size_id has been specified."
                             "Please review your 'size_id'" )

        image_id = basic_data.get('image', self.DEFAILT_IMAGE)
        try:
            self.image = self.driver.get_image( image_id )
        except:
            raise Exception( "No correct image_id has been specified."
                             "Please review your 'image_id'" )

        self.data = basic_data
        self.vm_user = basic_data.get('vm_user', self.DEFAULT_VM_USER)
        self.vm_comm = basic_data.get('vm_communicator', self.DEFAULT_VM_COMMUNICATOR)
        self.lrms = basic_data.get('lrms', self.DEFAULT_LRMS)
        self.private_key = expanduser(basic_data.get('private_key', self.DEFAULT_PRIVATE_KEY))
        # Note: This key will be added to the authorized keys for the user admin
        self.public_key = expanduser(basic_data.get('public_key', self.private_key+'.pub'))
        self.node_safe_time = int(basic_data.get('node_safe_time', self.DEFAULT_NODE_SAFE_TIME))         
        self.volume_capacity = int(basic_data.get('volume', self.DEFAULT_VOLUME))
        self.volume_type = basic_data.get('volume_type', self.DEFAULT_VOLUME_TYPE)
        self.max_jobs_running = basic_data['max_jobs_running']
        self.cloud_contextualisation_file = basic_data.get('vm_config', self.DEFAULT_VM_CONFIG)
        public_key_content = read_key( self.public_key )
        self.deployment = self.generate_cloud_config(public_key_content, user=self.vm_user,
                                                     user_cloud_config=basic_data.get('cloud_config_script'))
        #self.instance_pricing = float(basic_data.get('pricing', self.size.price))
        self.instance_pricing = float(basic_data.get('pricing', self.DEFAULT_PRICING))
        self.soft_billing = float(basic_data.get('soft_billing'))
        self.hard_billing = float(basic_data.get('hard_billing'))
        if not self.soft_billing and not self.hard_billing :
            self.soft_billing = self.hard_billing = self.DEFAULT_HARD_BILLING
        elif self.hard_billing and not self.soft_billing :
            self.soft_billing = self.hard_billing
        elif self.soft_billing and not self.hard_billing :
            self.hard_billing = self.soft_billing
    
    #This is here to avoid the error "TypeError: an integer is required"
    def __getstate__(self):
        odict = self.__dict__.copy()
        del odict['image']
        del odict['size']
        del odict['driver']
        del odict['node']
        del odict['location']
        if odict['volume_capacity']:
            del odict['volume']
        return odict

    def __setstate__(self, dict):
        self.__dict__.update(dict)
        cls = get_driver(Provider.EC2)
        self.driver = cls( self.access_id, self.secret_key, region = self.region )
        self.node = self.driver.list_nodes([self.node_id])[0]
        self.location = [l for l in self.driver.list_locations() if l.availability_zone.region_name == self.region][0]
        if self.data['volume']:
            self.volume = [v for v in self.driver.list_volumes() if v.id == self.volume_id][0]
        size_id = self.data.get('size', self.DEFAILT_SIZE)
        self.size = [s for s in self.driver.list_sizes() if s.id == size_id ][0]
        self.image = self.driver.get_image( self.data.get('image', self.DEFAILT_IMAGE) )
        
    def create_security_group(self):
        if not self.SECURITY_GROUP_NAME in self.driver.ex_list_security_groups():
            self.driver.ex_create_security_group(name=self.SECURITY_GROUP_NAME, description="DRM4G group open ssh port")
            self.driver.ex_authorize_security_group(name=self.SECURITY_GROUP_NAME, protocol='tcp', from_port=22,
                                                    to_port=22, cidr_ip='0.0.0.0/0')

    def create(self):
        logger.debug( "Running ec2's  create function" )
        if self.volume_capacity:
            self._create_volume()
            self._wait_storage()
        self._create_resource()
        logger.info( "Waiting until resource is active" )
        self._wait_resource()
        if self.volume_capacity :
            if not self._create_link():
                raise Exception("Could not create link between the resource '%s' and the volume '%s'" % (self.node_id, self.volume_id))
        #self._start_time()
        logger.debug( "Ending rocci's create function" )
    
    def _create_resource(self):
        logger.info( "Creating new EC2 resource" )
        with self._lock:
            self.create_security_group()
        self.node = self.driver.create_node(name='%s_DRM4G_VM_%s' % (self.image.extra['owner_alias'], uuid.uuid4().hex), image=self.image, size=self.size,
                    ex_security_groups=[self.SECURITY_GROUP_NAME], ex_userdata=self.deployment)
        self.node_id = self.node.id
        self._start_time()
        logger.info( "    EC2 resource '%s' has been successfully created" % self.node_id )

    def _create_volume(self):
        logger.info( "Creating new EC2 volume" )
        self.volume = self.driver.create_volume(size=self.volume_capacity, name='%s_DRM4G_VM_Storage_%s' % (self.image.extra['owner_alias'], uuid.uuid4().hex),
                                                 ex_volume_type=self.volume_type, location=self.location)
        self.volume_id = self.volume.id
        logger.info( "    EC2 volume '%s' has been successfully created" % self.volume_id )

    def is_resource_active(self):
        self.node = self.driver.list_nodes([self.node_id])[0]
        if self.volume_capacity:
            self.volume = [v for v in self.driver.list_volumes() if v.id == self.volume_id][0]
            if self.volume.state != StorageVolumeState.AVAILABLE :
                return False
        if self.node.state == NodeState.RUNNING :
            if self.volume_capacity:
                return self._create_link()
            return True
        return False

    def _create_link(self):
        #For Amazon EC2, to attach an EBS volume to an instance, it is necessary to specify
        #the device name by which the volume will be exposed to the instance
        for letter in range(ord('f'), ord('p')+1):
            for i in range(7):
                #This will assign an Amazon recommended name for EBS Volumes
                device = '/dev/sd%s' % chr(letter) + str([num for num in [i]][0] or '')
                try:
                    attached = self.driver.attach_volume(self.node, self.volume, device)
                    logger.info( "EC2 volume '%s' has been attached to node '%s' in " )
                    return attached
                except BaseHTTPError as err:
                    if not ("Attachment point %s is already in use" % device in str(err) or "%s is not a valid EBS device name" % device in str(err)) :
                        raise
        return False
    
    def _wait_storage(self):
        logger.info( "Waiting until EC2 volume is active" )
        start = time.time()
        end = start + self.TIMEOUT

        while time.time() < end:
            volume = [v for v in self.driver.list_volumes() if v.id == self.volume_id][0]
            if volume.state == StorageVolumeState.AVAILABLE:
                return
            else:
                time.sleep(self.WAIT_PERIOD)
        raise Exception("Timed out after waiting for volume '%s' to be active for %s seconds" % (self.volume_id, self.TIMEOUT))
    
    def _wait_resource(self):
        logger.info( "Waiting until EC2 resource is active" )
        start = time.time()
        end = start + self.TIMEOUT

        while time.time() < end:
            node = self.driver.list_nodes([self.node_id])[0]
            if node.state == NodeState.RUNNING:
                return
            else:
                time.sleep(self.WAIT_PERIOD)
        raise Exception("Timed out after waiting for resource '%s' to be active for %s seconds" % (self.node_id, self.TIMEOUT))

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
        if self.volume_capacity:
            if self.volume.state == StorageVolumeState.INUSE :
                self.driver.detach_volume(self.volume)
            self.driver.destroy_volume(self.volume)
        self.driver.destroy_node(self.node)
        logger.info( "    EC2 resource '%s' has been successfully deleted" % self.node_id )

    def get_ip(self):
        logger.info( "Getting EC2 resource's IP direction" )
        #self.ext_ip = self.wait_until_running()
        self.ext_ip = self.node.public_ips[0]
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
