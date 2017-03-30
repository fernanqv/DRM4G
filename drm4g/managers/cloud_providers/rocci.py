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

import re
import time
import uuid
from datetime                                   import timedelta, datetime
from os.path                                    import join, basename, expanduser
from drm4g.utils.importlib                      import import_module
from drm4g.managers.cloud_providers             import Instance, logger, CloudSetup
from drm4g.utils.proxy_certificate              import _renew_voms_proxy  
from utils                                      import load_json, read_key, is_ip_private
from drm4g                                      import ( COMMUNICATORS,
                                                         REMOTE_JOBS_DIR,
                                                         REMOTE_VOS_DIR,
                                                         DRM4G_DIR )

#logger = logging.getLogger(__name__)

cloud_setup_file = join(DRM4G_DIR, "etc", "cloudsetup.json")
generic_cloud_cfg = """
#cloud-config
users:
  - name: %s
    shell: /bin/bash
    sudo: ALL=(ALL) NOPASSWD:ALL
    lock-passwd: true+
    ssh-import-id: %s
    ssh-authorized-keys:
      - %s
"""

class Instance(Instance):

    DEFAULT_COMMUNICATOR = 'local'
    DEFAULT_MYPROXY_SERVER = 'myproxy1.egee.cesnet.cz'
    DEFAULT_SCRATCH = REMOTE_JOBS_DIR
    DEFAULT_REGION = 'EGI FedCloud - CESNET-METACLOUD'
    DEFAULT_SIZE ='Small'
    DEFAULT_IMAGE = 'Ubuntu-14.04'
    TIMEOUT = 3600.0 # seconds
    WAIT_PERIOD = 10.0 # seconds
    
    def __init__(self, basic_data):
        super(Instance, self).__init__()
        self.data = basic_data
        self.link_id = None
        self.int_ip = None
        self.comm = basic_data.get('communicator', self.DEFAULT_COMMUNICATOR)
        self.vm_user = basic_data.get('vm_user', self.DEFAULT_VM_USER)
        self.vm_comm = basic_data.get('vm_communicator', self.DEFAULT_VM_COMMUNICATOR)
        self.lrms = basic_data.get('lrms', self.DEFAULT_LRMS)
        self.private_key = expanduser(basic_data.get('private_key', self.DEFAULT_PRIVATE_KEY))
        self.public_key = expanduser(basic_data.get('public_key', self.private_key+'.pub'))
        self.myproxy_server = basic_data.get('myproxy_server', self.DEFAULT_MYPROXY_SERVER)
        self.node_safe_time = int(basic_data.get('node_safe_time', self.DEFAULT_NODE_SAFE_TIME))  
        self.volume_capacity = int(basic_data.get('volume', self.DEFAULT_VOLUME))
        self.max_jobs_running = basic_data[ 'max_jobs_running' ]
        self.context_file = basename(self.private_key) + '.login'
        self.cloud_contextualisation_file = basic_data.get('vm_config', self.DEFAULT_VM_CONFIG)
        pub = read_key( self.public_key )
        
        #'pricing', 'soft_billing' and 'hard_billing' should always be 0 for a rocci VM
        self.instance_pricing = float(basic_data.get('pricing', self.DEFAULT_PRICING))
        self.soft_billing = float(basic_data.get('soft_billing'))
        self.hard_billing = float(basic_data.get('hard_billing'))
        if not self.soft_billing and not self.hard_billing :
            self.soft_billing = self.hard_billing = self.DEFAULT_HARD_BILLING
        elif self.hard_billing and not self.soft_billing :
            self.soft_billing = self.hard_billing
        elif self.soft_billing and not self.hard_billing :
            self.hard_billing = self.soft_billing

        try :
            cloud_setup = {}
            for name, features in load_json( cloud_setup_file ).items() :
                cloud_setup[ name ] =  CloudSetup(name, features)
        except Exception as err :
            logger.error( "Error reading the cloud setup file: " + str( err ) )

        infra_cfg = cloud_setup[ basic_data['cloud_connector'] ]
        cloud_cfg = infra_cfg.cloud_providers[ basic_data.get('region', self.DEFAULT_REGION) ]
        self.vo = infra_cfg.vo
        self.endpoint = cloud_cfg[ 'endpoint' ]
        self.flavour = cloud_cfg[ 'flavours' ][ basic_data.get('size', self.DEFAULT_SIZE) ]
        self.app_name = basic_data.get('image', self.DEFAULT_IMAGE)
        self.app = cloud_cfg[ 'apps' ][ self.app_name ]

        communicator = import_module(COMMUNICATORS[ self.comm ] )
        com_obj = getattr( communicator , 'Communicator' ) ()
        if self.comm != 'local' :
            com_obj.username       = basic_data['username']
            com_obj.frontend       = basic_data['frontend']
            com_obj.private_key    = self.private_key
            com_obj.public_key     = self.public_key
            com_obj.work_directory = basic_data.get('scratch', self.DEFAULT_SCRATCH)
        self.com_object = com_obj

        self.proxy_file = join( REMOTE_VOS_DIR , 'x509up.%s' ) % self.vo

        '''
        commented so that the context file is created everytime
        just in case the user changed something in the contextualisation file
        #cmd = "ls %s" % self.context_file #to check if it exists
        out,err = self.com_object.execCommand( cmd )
        if err:
        '''
        with open( self.cloud_contextualisation_file, "r" ) as contex_file :
            cloud_config = contex_file.read()
            if 'vm_config' not in basic_data.keys():
                content = cloud_config % (self.vm_user, self.vm_user, pub)
            else:
                content = cloud_config
            logger.debug("Your contextualisation file %s :\n%s" % (self.cloud_contextualisation_file, content))
            #content = generic_cloud_cfg % (self.vm_user, self.vm_user, pub)
            cmd = "echo '%s' > %s" % (content, self.context_file)
            out,err = self.com_object.execCommand( cmd )
            if err:
                raise Exception("Wasn't able to create the context file %s." % self.context_file + err)

        cmd = "ls %s" % self.proxy_file #to check if it exists
        out,err = self.com_object.execCommand( cmd )
        if err:
            _renew_voms_proxy(self.com_object, self.myproxy_server, self.vo)

    #This is here to avoid the error "TypeError: can't pickle lock objects" when creating the pickled file
    def __getstate__(self):
        odict = self.__dict__.copy()
        del odict['com_object']
        return odict

    def __setstate__(self, dict):
        self.__dict__.update(dict)
        communicator = import_module(COMMUNICATORS[ self.data[ 'communicator' ] ] )
        com_obj = getattr( communicator , 'Communicator' ) ()
        if self.data[ 'communicator' ] != 'local' :
            com_obj.username       = self.data['username']
            com_obj.frontend       = self.data['frontend']
            com_obj.private_key    = self.private_key
            com_obj.public_key     = self.data.get('public_key', self.private_key+'.pub')
            com_obj.work_directory = self.data.get('scratch', self.DEFAULT_SCRATCH)
        self.com_object=com_obj

    def _exec_remote_cmd(self, command):
        logger.debug("~~~~~~~~~~~~~~~~ Going to execute remote command: ~~~~~~~~~~~~~~~~")
        out, err = self.com_object.execCommand( command )
        logger.debug("~~~~~~~~~~~~~~~~         Command executed         ~~~~~~~~~~~~~~~~")
        return out, err

    def create(self):
        logger.debug( "Running rocci's create function" )
        if self.volume_capacity:
            self._create_volume()
            self._wait_storage()
        self._create_resource()
        logger.info( "Waiting until resource is active" )
        self._wait_resource()
        if self.volume_capacity :
            self._create_link()
        #self._start_time()
        logger.debug( "Ending rocci's create function" )

    def _wait_storage(self):
        logger.debug( "Running rocci's _wait_storage function" )
        now = datetime.now()
        end = now + timedelta( minutes = self.TIMEOUT/60 )

        while now <= end :
            logger.debug( "  * _wait_storage - waited for %s minutes" % (timedelta( minutes = self.TIMEOUT/60 ) - (end-now)) )
            out = self.get_description(self.volume_id)
            pattern = re.compile( "occi.storage.state\s*=\s*(.*)" )
            mo = pattern.findall( out )
            if mo and mo[ 0 ] == "online" :
                logger.debug( "Ending rocci's _wait_storage function" )
                return
            time.sleep(self.WAIT_PERIOD)
            now += timedelta( seconds = self.WAIT_PERIOD )
        raise Exception("Timed out after after waiting for storage '%s' to be active for %s seconds" % (self.volume_id, self.TIMEOUT))

    def _wait_resource(self):
        logger.debug( "Running rocci's _wait_resource function" )
        now = datetime.now()
        end = now + timedelta( minutes = self.TIMEOUT/60 )

        while now <= end :
            logger.debug( "  * _wait_resource - waited for %s minutes" % (timedelta( minutes = self.TIMEOUT/60 ) - (end-now)) )
            out = self.get_description(self.node_id)
            pattern = re.compile( "occi.compute.state\s*=\s*(.*)" )
            mo = pattern.findall( out )
            logger.debug( "The resource's state is %s" % mo[ 0 ] )
            if mo and mo[ 0 ] == "active" :
                logger.debug( "Ending rocci's _wait_resource function" )
                return
            time.sleep(self.WAIT_PERIOD)
            now += timedelta( seconds = self.WAIT_PERIOD )
        raise Exception("Timed out after waiting for resource '%s' to be active for %s seconds" % (self.node_id, self.TIMEOUT))
        
    def is_resource_active(self):
        logger.debug( "Running rocci's is_resource_active function" )
        if self.volume_capacity:
            out = self.get_description(self.volume_id)
            pattern = re.compile( "occi.storage.state\s*=\s*(.*)" )
            mo = pattern.findall( out )
            if mo and mo[ 0 ] != "online" :
                return False
        out = self.get_description(self.node_id)
        pattern = re.compile( "occi.compute.state\s*=\s*(.*)" )
        mo = pattern.findall( out )
        logger.debug( "The resource's state is %s" % mo[ 0 ] )
        logger.debug( "Ending rocci's is_resource_active function" )
        if mo and mo[ 0 ] == "active" :
            if self.volume_capacity:
                self._create_link()
            return True
        return False

    def _create_link(self):
        logger.debug( "Running rocci's _create_link function" )
        logger.info( "Linking volume %s to resource %s" % (self.volume_id, self.node_id) )
        cmd = 'occi --endpoint %s --auth x509 --user-cred %s --voms --action link ' \
              '--resource %s -j %s' % (self.endpoint, self.proxy_file, self.node_id, self.volume_id )
        out, err = self._exec_remote_cmd( cmd )
        self.log_output("_create_link", out, err)

        if 'certificate expired' in err :
            _renew_voms_proxy(self.com_object, self.myproxy_server, self.vo)
            out, err = self._exec_remote_cmd( cmd )
            self.log_output("_create_link 2", out, err)
        elif err :
            logger.error( "Ending rocci's _create_link function with an error" )
            raise Exception( "Error linking resource and volume: %s" % out )
        self.link_id = out.rstrip('\n')
        logger.debug( "Ending rocci's _create_link function" )

    def _create_resource(self):
        try:
            logger.debug( "Running rocci's _create_resource function" )
            logger.info( "Creating new resource" )
            cmd = 'occi --endpoint %s --auth x509 --user-cred %s --voms --action create --attribute occi.core.title="%s_DRM4G_VM_%s" ' \
                      '--resource compute --mixin %s --mixin %s --context user_data="file://$PWD/%s"' % (
                             self.endpoint, self.proxy_file, str(self.app_name).lower(), uuid.uuid4().hex, self.app, self.flavour, self.context_file )
            out, err = self._exec_remote_cmd( cmd )
            self.log_output("_create_resource", out, err)

            if 'certificate expired' in err :
                _renew_voms_proxy(self.com_object, self.myproxy_server, self.vo)
                logger.debug( "After executing _renew_voms_proxy - Going to execute cmd again" )
                out, err = self._exec_remote_cmd( cmd )
                self.log_output("_create_resource 2", out, err)
            elif err :
                logger.error( "Ending rocci's  _create_resource function with an error" )
                raise Exception( "An error occurred while creating a rocci VM : %s" % err )
            self.node_id = out.rstrip('\n')
            self._start_time()
            logger.info( "    Resource '%s' has been successfully created" % self.node_id )
            logger.debug( "Ending rocci's  _create_resource function" )
        except Exception as err:
            raise Exception("Most likely the issue is being caused by a timeout error:\n    "+str(err))

    def _create_volume(self):
        logger.debug( "Running rocci's _create_volume function" )
        logger.info( "Creating volume for resource %s" % self.node_id )
        cmd = "occi --endpoint %s --auth x509 --user-cred %s --voms --action create --resource storage --attribute " \
              "occi.storage.size='num(%s)' --attribute occi.core.title=%s_DRM4G_VM_Storage_%s" % (
                     self.endpoint, self.proxy_file, str( self.volume_capacity ), str(self.app_name).lower(), uuid.uuid4().hex )
        out, err = self._exec_remote_cmd( cmd )
        self.log_output("_create_volume", out, err)

        if 'certificate expired' in err :
            _renew_voms_proxy(self.com_object, self.myproxy_server, self.vo)
            out, err = self._exec_remote_cmd( cmd )
            self.log_output("_create_volume 2", out, err)
        elif err :
            logger.error( "Ending rocci's _create_volume function with an error" )
            raise Exception( "Error creating volume : %s" % out )
        self.volume_id = out.rstrip('\n')
        logger.debug( "Ending rocci's _create_volume function" )

    def destroy(self):
        logger.debug( "Running rocci's  destroy function" )
        if self.node_id:
            logger.info( "Deleting resource %s" % self.node_id )
            if self.volume_capacity :
                cmd = "occi --endpoint %s --auth x509 --user-cred %s --voms --action unlink --resource %s" % (
                                 self.endpoint, self.proxy_file, self.link_id )
                out, err = self._exec_remote_cmd( cmd )
                self.log_output("destroy (unlink)", out, err)
    
                if 'certificate expired' in err :
                    _renew_voms_proxy(self.com_object, self.myproxy_server, self.vo)
                    logger.debug( "After executing _renew_voms_proxy - Going to execute cmd again" )
                    out, err = self._exec_remote_cmd( cmd )
                    self.log_output("destroy (unlink) 2", out, err)
                elif err :
                    logger.error( "Error unlinking volume '%s': %s" % ( self.volume_id, out ) )
                time.sleep( 20 )
                cmd = "occi --endpoint %s --auth x509 --user-cred %s --voms --action delete --resource %s" % (
                                 self.endpoint, self.proxy_file, self.volume_id )
                out, err = self._exec_remote_cmd( cmd )
                self.log_output("destroy (volume)", out, err)
    
                if 'certificate expired' in err :
                    _renew_voms_proxy(self.com_object, self.myproxy_server, self.vo)
                    logger.debug( "After executing _renew_voms_proxy - Going to execute cmd again" )
                    out, err = self._exec_remote_cmd( cmd )
                    self.log_output("destroy (volume) 2", out, err)
                elif err :
                    logger.error( "Error deleting volume '%s': %s" % ( self.volume_id, out ) )
            cmd = "occi --endpoint %s --auth x509 --user-cred %s --voms --action delete --resource %s" % (
                                 self.endpoint, self.proxy_file, self.node_id )
            out, err = self._exec_remote_cmd( cmd )
            self.log_output("destroy (resource)", out, err)
    
            if 'certificate expired' in err :
                _renew_voms_proxy(self.com_object, self.myproxy_server, self.vo)
                logger.debug( "After executing _renew_voms_proxy - Going to execute cmd again" )
                out, err = self._exec_remote_cmd( cmd )
                self.log_output("destroy (resource) 2", out, err)
            elif err :
                logger.error( "Error deleting node '%s': %s" % ( self.node_id, out ) )
            else:
                logger.info( "    Resource '%s' has been successfully deleted" % self.node_id )
        else:
            logger.debug( "    The resource didn't manage to get created" )
        logger.debug( "Ending rocci's destroy function" )

    def get_description(self, id):
        logger.debug( "Running rocci's  get_description function" )
        cmd = "occi --endpoint %s --auth x509 --user-cred %s --voms --action describe --resource %s" % (
                             self.endpoint, self.proxy_file, id )
        out, err = self._exec_remote_cmd( cmd )
        self.log_output("get_description", out, err)

        if 'certificate expired' in err :
            _renew_voms_proxy(self.com_object, self.myproxy_server, self.vo)
            logger.debug( "After executing _renew_voms_proxy - Going to execute cmd again" )
            out, err = self._exec_remote_cmd( cmd )
            self.log_output("get_description 2", out, err)
        elif err and "Insecure world writable dir" not in err:
            logger.error( "Ending rocci's get_description function with an error" )
            raise Exception( "Error getting description node '%s': %s" % ( id, out ) )
        logger.debug( "Ending rocci's get_description function" )
        return out

    def get_floating_ips(self):
        logger.debug( "Running rocci's  get_floating_ips function" )
        cmd = "occi --endpoint %s --auth x509 --user-cred %s --voms --dump-model | grep 'http://schemas.openstack.org/network/floatingippool'" % (
                      self.endpoint, self.proxy_file )
        out, err = self._exec_remote_cmd( cmd )
        self.log_output("get_floating_ips (floating check)", out, err)

        if 'certificate expired' in err :
            _renew_voms_proxy(self.com_object, self.myproxy_server, self.vo)
            out, err = self._exec_remote_cmd( cmd )
            self.log_output("get_floating_ips (floating check) 2", out, err)
        elif err :
            logger.error( "Ending rocci's get_floating_ips function with an error" )
        logger.debug( "Ending rocci's get_floating_ips function" )
        return out

    def get_public_ip(self):
        logger.debug( "Running rocci's  get_public_ip function" )
        network_interfaces = self.get_network_interfaces()
        network_interface=""
        for n in network_interfaces[::-1] :
            if basename(n).lower() == 'public':
                network_interface = n
        if network_interface:
            cmd = "occi --endpoint %s --auth x509 --user-cred %s --voms --action link --resource %s --link %s" % (
                      self.endpoint, self.proxy_file, self.node_id, network_interface )
            out, err = self._exec_remote_cmd( cmd )
            self.log_output("get_public_ip", out, err)

            if 'certificate expired' in err :
                _renew_voms_proxy(self.com_object, self.myproxy_server, self.vo)
                out, err = self._exec_remote_cmd( cmd )
                self.log_output("get_public_ip 2", out, err)

                if err:
                    raise Exception(str(err))
            elif err:
                raise Exception(str(err))
        else:
            floating_pools = self.get_floating_ips()
            if floating_pools:
                contents = floating_pools.split('\n')

                cont=0
                cond=False
                while cont<len(contents) and not cond:
                    items = contents[cont].split(';')
                    pairs = [item.split('=',1) for item in items]
                    d=dict((k,eval(v,{},{})) for (k,v) in pairs[1:])
                    mixin=d['scheme']+d['title']

                    cmd = "occi --endpoint %s --auth x509 --user-cred %s --voms --action link --resource %s --link %s --mixin %s" % (
                          self.endpoint, self.proxy_file, self.node_id, network_interface, mixin )
                    out, err = self._exec_remote_cmd( cmd )
                    self.log_output("get_public_ip", out, err)

                    if 'No more floating ips in pool' in str(err):
                        cont+=1
                    elif 'certificate expired' in str(err) :
                        _renew_voms_proxy(self.com_object, self.myproxy_server, self.vo)
                        continue
                    elif err:
                        logger.debug("An unexpected error occurred:\n"+str(err))
                        cont+=1
                    elif out:
                        #I'm assuming that out will have something that resembles this:
                        #http://stack-server-02.ct.infn.it:8787/network/interface/c6886e72-86bd-4a08-ab2b-f0769854a38a_90.147.16.53
                        #which is what the link commmand returns without that last "mixin" option
                        cond=True
                        logger.debug("\n\n\nI don't know if I will ever get to this point since I'm still not sure what's returned by" \
                                     " the last executed command (since it's never worked until now)\n\n\n")
                    else:
                        logger.debug("There wasn't either an output or an error for the execution of the 'get_public_ip' function")
            else:
                raise Exception("Error trying to get a public IP")

        time.sleep( 10 )
        out = self.get_description(self.node_id)
        pattern = re.compile( "occi.networkinterface.address\s*=\s*(.*)" )
        mo = pattern.findall( out )
        if mo :
            for ip in mo :
                if is_ip_private( ip  ) :
                    self.int_ip = ip
                else :
                    self.ext_ip = ip
        if not self.ext_ip :
            logger.error( "Ending rocci's get_public_ip function with an error" )
            raise Exception( "Error trying to get a public IP" )
        logger.debug( "Ending rocci's get_public_ip function" )

    def get_network_interfaces(self):
        logger.debug( "Running rocci's  get_network_interfaces function" )
        cmd = "occi --endpoint %s --auth x509 --user-cred %s --voms --action list --resource network" % (
                             self.endpoint, self.proxy_file )
        out, err = self._exec_remote_cmd( cmd )
        self.log_output("get_network_interfaces", out, err)

        if 'certificate expired' in err :
            _renew_voms_proxy(self.com_object, self.myproxy_server, self.vo)
            out, err = self._exec_remote_cmd( cmd )
            self.log_output("get_network_interfaces 2", out, err)
        elif err :
            logger.error( "Ending rocci's get_network_interfaces function with an error" )
            raise Exception( "Error getting network list" )
        logger.debug( "Ending rocci's get_network_interfaces function" )
        return out.strip().split()

    def get_ip(self):
        logger.debug( "Running rocci's  get_ip function" )
        logger.info( "Getting resource's IP direction" )
        out = self.get_description(self.node_id)
        pattern = re.compile( "occi.networkinterface.address\s*=\s*(.*)" )
        mo = pattern.findall( out )
        if mo :
            ip = mo[ 0 ]
            if not is_ip_private( ip ) :
                self.ext_ip = ip
                self.int_ip = ip
            else :
                self.get_public_ip()
            logger.info( "    Public IP: %s" % self.ext_ip )
        else :
            logger.error( "Ending rocci's get_ip function with an error" )
            raise Exception( "Error getting IP" )

        logger.debug( "*********** get_ip -- self.int_ip ***********" )
        logger.debug( str(self.int_ip) )
        logger.debug( "*********** get_ip -- self.ext_ip ***********" )
        logger.debug( str(self.ext_ip) )
        logger.debug( "*********** get_ip -- ip ***********" )
        logger.debug( str(ip) )
        logger.debug( "*********** get_ip -- end ***********" )
        logger.debug( "Ending rocci's get_ip function" )

    def log_output(self, msg, out, err, extra=None):
        logger.debug( "Command return:" )
        logger.debug( "*********** %s -- out ***********" % msg )
        logger.debug( str(out) )
        logger.debug( "*********** %s -- err ***********" % msg )
        logger.debug( str(err) )
        logger.debug( "*********** %s -- end ***********" % msg )
