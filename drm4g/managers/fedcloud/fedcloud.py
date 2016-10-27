import re
import time
import uuid
import socket
import logging
from datetime                   import timedelta, datetime
from os.path                    import join, basename, expanduser
from drm4g.utils.importlib      import import_module
from drm4g.managers.fedcloud    import CloudSetup
from utils                      import load_json, read_key, is_ip_private
from drm4g                      import ( COMMUNICATORS,
                                         REMOTE_JOBS_DIR,
                                         REMOTE_VOS_DIR,
                                         DRM4G_DIR )

__version__  = '0.1.0'
__author__   = 'Carlos Blanco'
__revision__ = "$Id$"

logger = logging.getLogger(__name__)

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

class Instance(object):

    def __init__(self, basic_data):
        self.data=basic_data # this is just here to be able to do the "__setstate__" which needs to be defined in order to avoid having the error "TypeError: can't pickle lock objects"
        self.id = None
        self.id_volume = None
        self.id_link = None
        self.int_ip = None
        self.ext_ip = None
        self.volume = int(basic_data['volume'])
        self.myproxy_server = basic_data.get('myproxy_server', '')
        self.private_key = expanduser(basic_data['private_key'])
        self.context_file = basename(self.private_key)+".login"
        self.vo_user = basic_data.get('vo_user', 'drm4g_admin')
        pub = read_key( self.private_key + ".pub" )

        try :
            cloud_setup = {}
            for name, features in load_json( cloud_setup_file ).items() :
                cloud_setup[ name ] =  CloudSetup(name, features)
        except Exception as err :
            logging.error( "Error reading the cloud setup file: " + str( err ) )

        infra_cfg = cloud_setup[ basic_data['lrms'] ]
        cloud_cfg = infra_cfg.clouds[ basic_data['cloud'] ]
        self.vo = infra_cfg.vo
        self.endpoint = cloud_cfg[ "endpoint" ]
        self.flavour = cloud_cfg[ "flavours" ][ basic_data['flavour'] ]
        self.app_name = basic_data['virtual_image']
        self.app = cloud_cfg[ "apps" ][ self.app_name ]

        communicator = import_module(COMMUNICATORS[ basic_data[ 'communicator' ] ] ) #this can be 'local'
        com_obj = getattr( communicator , 'Communicator' ) ()
        com_obj.username       = basic_data['username'] #username
        com_obj.frontend       = basic_data['frontend']
        com_obj.private_key    = self.private_key
        com_obj.public_key     = basic_data.get('public_key', self.private_key+'.pub')
        com_obj.work_directory = basic_data.get('scratch', REMOTE_JOBS_DIR)
        self.com_object=com_obj

        self.proxy_file = join( REMOTE_VOS_DIR , "x509up.%s" ) % self.vo
        
        cmd = "ls %s" % self.context_file #to check if it exists
        out1,err1 = self.com_object.execCommand( cmd )
        if err1:
            #cmd = "mkdir -p %s" % #create it's directory
            content= generic_cloud_cfg % (self.vo_user, self.vo_user, pub)
            cmd = "echo '%s' > %s" % (content, self.context_file)
            out1_1,err1_1=self.com_object.execCommand( cmd )
            if err1_1:
                raise Exception("Wasnt't able to create the context file %s.\n" % self.context_file + err1_1)

        cmd = "ls %s" % self.proxy_file #to check if it exists
        out2,err2 = self.com_object.execCommand( cmd )
        if err2:
            self._renew_voms_proxy()

    def __getstate__(self):
        odict = self.__dict__.copy() # copy the dict since we change it
        del odict['com_object']      # remove communicator entry
        return odict

    def __setstate__(self, dict):
        self.__dict__.update(dict)
        communicator = import_module(COMMUNICATORS[ self.data[ 'communicator' ] ] ) #could this be 'local'
        com_obj = getattr( communicator , 'Communicator' ) ()
        com_obj.username       = self.data['username']
        com_obj.frontend       = self.data['frontend']
        com_obj.private_key    = self.private_key
        com_obj.public_key     = self.data.get('public_key', self.private_key+'.pub')
        com_obj.work_directory = self.data.get('scratch', REMOTE_JOBS_DIR)
        self.com_object=com_obj

    def _renew_voms_proxy(self, cont=0):
        try:
            logger.debug( "Running fedcloud's _renew_voms_proxy function" )
            logger.debug("_renew_voms_proxy cont = %s" % str(cont))

            cmd = "rm %s" % self.proxy_file
            self.com_object.execCommand( cmd )
            
            if self.myproxy_server:
                LOCAL_X509_USER_PROXY = "X509_USER_PROXY=%s" % join ( REMOTE_VOS_DIR , self.myproxy_server )
            else :
                LOCAL_X509_USER_PROXY = "X509_USER_PROXY=%s/${MYPROXY_SERVER}" % ( REMOTE_VOS_DIR )
            cmd = "%s voms-proxy-init -ignorewarn " \
            "-timeout 30 -valid 24:00 -q -voms %s -noregen -out %s --rfc" % (
                LOCAL_X509_USER_PROXY ,
                self.vo ,
                self.proxy_file )

            ##uncomment logger.error( "Executing command: %s" % cmd )
            out, err = self.com_object.execCommand( cmd )
            ##uncomment logger.error( out + err )

            logger.debug( "Command return:" )
            logger.debug( "*********** _renew_voms_proxy -- out ***********" )
            logger.debug( str(out) )
            logger.debug( "*********** _renew_voms_proxy -- err ***********" )
            logger.debug( str(err) )
            logger.debug( "*********** _renew_voms_proxy -- end ***********\n\n" )

            if err:
                logger.debug( "Ending  fedcloud's _renew_voms_proxy function with an error" )
                output = "Error renewing the proxy(%s): %s" % ( cmd , err )
                logger.error( output )
                raise Exception("Most probably the proxy certificate hasn't been created. Be sure to run the the following command before trying again:" \
                    "\n    \033[93mdrm4g id <resource_name> init\033[0m") #puede que tenga que modificarlo a <host_name>
            logger.debug( "Ending  fedcloud's _renew_voms_proxy" )
        except socket.timeout:
            logger.debug("\nCaptured the socket.time exception\n")
            if cont<3:
                self._renew_voms_proxy(cont+1)
            else:
                raise

    def create(self):
        logger.debug( "\nRunning fedcloud's  create function" )
        if self.volume:
            self._create_volume()
            self._wait_storage()
        self._create_resource()
        self._wait_compute()
        if self.volume :
            self._create_link()
        logger.debug( "Ending  fedcloud's create function\n" )

    def _wait_storage(self):
        logger.debug( "Running fedcloud's _wait_storage function" )
        now = datetime.now()
        end = now + timedelta( minutes = 60 )

        while now <= end :
            logger.debug( "  * _wait_storage - waited for %s minutes" % (timedelta( minutes = 60 ) - (end-now)) )
            out = self.get_description(self.id_volume)
            pattern = re.compile( "occi.storage.state\s*=\s*(.*)" )
            mo = pattern.findall( out )
            if mo and mo[ 0 ] == "online" :
                break
            time.sleep(10)
            now += timedelta( seconds = 10 )
        logger.debug( "Ending  fedcloud's _wait_storage function\n" )

    def _wait_compute(self):
        logger.debug( "Running fedcloud's _wait_compute function" )
        now = datetime.now()
        end = now + timedelta( minutes = 60 )

        while now <= end :
            logger.debug( "  * _wait_compute - waited for %s minutes" % (timedelta( minutes = 60 ) - (end-now)) )
            out = self.get_description(self.id)
            pattern = re.compile( "occi.compute.state\s*=\s*(.*)" )
            mo = pattern.findall( out )
            logger.debug( "%s and %s == 'active' : %s" % (mo, mo[0], (mo and mo[ 0 ] == "active")))
            if mo and mo[ 0 ] == "active" :
                break
            time.sleep(10)
            now += timedelta( seconds = 10 )
        logger.debug( "Ending  fedcloud's _wait_compute function\n" )

    def _create_link(self):
        logger.debug( "Running fedcloud's _create_link function" )
        cmd = 'occi --endpoint %s --auth x509 --user-cred %s --voms --action link ' \
              '--resource /compute/%s -j %s' % (self.endpoint, self.proxy_file, self.id, self.id_volume )
        out, err = self.com_object.execCommand( cmd )
        
        if 'certificate expired' in err :
            self._renew_voms_proxy()
            out, err = self.com_object.execCommand( cmd )

        elif err :
            logger.debug( "Ending  fedcloud's _create_link function with an error" )
            raise Exception( "Error linking resource and volume: %s" % out )
        self.id_link = out.rstrip('\n')
        logger.debug( "Ending  fedcloud's _create_link function\n" )

    def _create_resource(self):
        logger.debug( "Running fedcloud's _create_resource function" )
        cmd = 'occi --endpoint %s --auth x509 --user-cred %s --voms --action create --attribute occi.core.title="%s_DRM4G_VM_%s" ' \
                  '--resource compute --mixin %s --mixin %s --context user_data="file://$PWD/%s"' % (
                         self.endpoint, self.proxy_file, str(self.app_name).lower(), uuid.uuid4().hex, self.app, self.flavour, self.context_file )
        out, err = self.com_object.execCommand( cmd )

        logger.debug( "Command return:" )
        logger.debug( "*********** _create_resource -- out ***********" )
        logger.debug( str(out) )
        logger.debug( "*********** _create_resource -- err ***********" )
        logger.debug( str(err) )
        logger.debug( "*********** _create_resource -- end ***********\n\n" )

        if 'certificate expired' in err :
            self._renew_voms_proxy()
            logger.debug( "After executing _renew_voms_proxy - Going to execute cmd again" )
            out, err = self.com_object.execCommand( cmd )

            logger.debug( "Command return:" )
            logger.debug( "*********** _create_resource 2 -- out ***********" )
            logger.debug( str(out) )
            logger.debug( "*********** _create_resource 2 -- err ***********" )
            logger.debug( str(err) )
            logger.debug( "*********** _create_resource 2 -- end ***********\n\n" )

        elif err :
            logger.debug( "Ending  fedcloud's  _create_resource function with an error" )
            raise Exception( "Error creating VM : %s" % out )
        self.id = out.rstrip('\n')
        logger.debug( "Ending  fedcloud's  _create_resource function\n" )

    def _create_volume(self):
        logger.debug( "Running fedcloud's _create_volume function" )
        cmd = "occi --endpoint %s --auth x509 --user-cred %s --voms --action create --resource storage --attribute " \
              "occi.storage.size='num(%s)' --attribute occi.core.title=%s_DRM4G_Workspace_%s" % (
                     self.endpoint, self.proxy_file, str( self.volume ), str(self.app_name).lower(), uuid.uuid4().hex )
        out, err = self.com_object.execCommand( cmd )
        
        if 'certificate expired' in err :
            self._renew_voms_proxy()
            out, err = self.com_object.execCommand( cmd )
        elif err :
            logger.debug( "Ending  fedcloud's _create_volume function with an error" )
            raise Exception( "Error creating volume : %s" % out )
        self.id_volume = out.rstrip('\n')
        logger.debug( "Ending  fedcloud's _create_volume function\n" )

    def delete(self):
        logger.debug( "\nRunning fedcloud's  delete function" )
        if self.volume :
            cmd = "occi --endpoint %s --auth x509 --user-cred %s --voms --action unlink --resource /compute/%s" % (
                             self.endpoint, self.proxy_file, basename(self.id_link) )
            out, err = self.com_object.execCommand( cmd )

            logger.debug( "Command return:" )
            logger.debug( "*********** delete (unlink) -- out ***********" )
            logger.debug( str(out) )
            logger.debug( "*********** delete (unlink) -- err ***********" )
            logger.debug( str(err) )
            logger.debug( "*********** delete (unlink) -- end ***********\n\n" )

            if 'certificate expired' in err :
                self._renew_voms_proxy()
                logger.debug( "After executing _renew_voms_proxy - Going to execute cmd again" )
                out, err = self.com_object.execCommand( cmd )

                logger.debug( "Command return:" )
                logger.debug( "*********** delete (unlink) 2 -- out ***********" )
                logger.debug( str(out) )
                logger.debug( "*********** delete (unlink) 2 -- err ***********" )
                logger.debug( str(err) )
                logger.debug( "*********** delete (unlink) 2 -- end ***********\n\n" )

            elif err :
                logging.debug( "Error unlinking volume '%s': %s" % ( self.id_volume, out ) )
            time.sleep( 20 )
            cmd = "occi --endpoint %s --auth x509 --user-cred %s --voms --action delete --resource /compute/%s" % (
                             self.endpoint, self.proxy_file, basename(self.id_volume) )
            out, err = self.com_object.execCommand( cmd )

            logger.debug( "Command return:" )
            logger.debug( "*********** delete (volume) -- out ***********" )
            logger.debug( str(out) )
            logger.debug( "*********** delete (volume) -- err ***********" )
            logger.debug( str(err) )
            logger.debug( "*********** delete (volume) -- end ***********\n\n" )
        
            if 'certificate expired' in err :
                self._renew_voms_proxy()
                logger.debug( "After executing _renew_voms_proxy - Going to execute cmd again" )
                out, err = self.com_object.execCommand( cmd )

                logger.debug( "Command return:" )
                logger.debug( "*********** delete (volume) 2 -- out ***********" )
                logger.debug( str(out) )
                logger.debug( "*********** delete (volume) 2 -- err ***********" )
                logger.debug( str(err) )
                logger.debug( "*********** delete (volume) 2 -- end ***********\n\n" )

            elif err :
                logging.debug( "Error deleting volume '%s': %s" % ( self.id_volume, out ) )
        cmd = "occi --endpoint %s --auth x509 --user-cred %s --voms --action delete --resource /compute/%s" % (
                             self.endpoint, self.proxy_file, basename(self.id) )
        out, err = self.com_object.execCommand( cmd )
        
        logger.debug( "Command return:" )
        logger.debug( "*********** delete (resource) -- out ***********" )
        logger.debug( str(out) )
        logger.debug( "*********** delete (resource) -- err ***********" )
        logger.debug( str(err) )
        logger.debug( "*********** delete (resource) -- end ***********\n\n" )

        if 'certificate expired' in err :
            self._renew_voms_proxy()
            logger.debug( "After executing _renew_voms_proxy - Going to execute cmd again" )
            out, err = self.com_object.execCommand( cmd )

            logger.debug( "Command return:" )
            logger.debug( "*********** delete (resource) 2 -- out ***********" )
            logger.debug( str(out) )
            logger.debug( "*********** delete (resource) 2 -- err ***********" )
            logger.debug( str(err) )
            logger.debug( "*********** delete (resource) 2 -- end ***********\n\n" )

        elif err :
            logging.error( "Error deleting node '%s': %s" % ( self.id, out ) )
        logger.debug( "Ending  fedcloud's delete function\n" )

    def get_description(self, id):
        logger.debug( "\nRunning fedcloud's  get_description function" )
        cmd = "occi --endpoint %s --auth x509 --user-cred %s --voms --action describe --resource /compute/%s" % (
                             self.endpoint, self.proxy_file, basename(id) )
        out, err = self.com_object.execCommand( cmd )
        logger.debug( "Command return:" )
        logger.debug( "*********** get_description -- out ***********" )
        logger.debug( str(out) )
        logger.debug( "*********** get_description -- err ***********" )
        logger.debug( str(err) )
        logger.debug( "*********** get_description -- end ***********\n\n" )

        if 'certificate expired' in err :
            self._renew_voms_proxy()
            logger.debug( "After executing _renew_voms_proxy - Going to execute cmd again" )
            out, err = self.com_object.execCommand( cmd )

            logger.debug( "Command return:" )
            logger.debug( "*********** get_description 2 -- out ***********" )
            logger.debug( str(out) )
            logger.debug( "*********** get_description 2 -- err ***********" )
            logger.debug( str(err) )
            logger.debug( "*********** get_description 2 -- end ***********\n\n" )

        elif err and "Insecure world writable dir" not in err:
            logger.debug( "Ending  fedcloud's get_description function with an error" )
            raise Exception( "Error getting description node '%s': %s" % ( id, out ) )
        logger.debug( "Ending  fedcloud's get_description function\n" )
        return out

    def get_public_ip(self):
        logger.debug( "\nRunning fedcloud's  get_public_ip function" )
        network_interfaces = self.get_network_interfaces()
        for network_interface in network_interfaces[::-1] :
            cmd = "occi --endpoint %s --auth x509 --user-cred %s --voms --action link --resource /compute/%s --link %s" % (
                      self.endpoint, self.proxy_file, basename(self.id), network_interface )
            out, err = self.com_object.execCommand( cmd )

            if 'certificate expired' in err :
                self._renew_voms_proxy()
                out, err = self.com_object.execCommand( cmd )

            elif not out :
                time.sleep( 10 )
                out = self.get_description(self.id)
                pattern = re.compile( "occi.networkinterface.address\s*=\s*(.*)" )
                mo = pattern.findall( out )
                if mo :
                    for ip in mo :
                        if is_ip_private( ip  ) :
                            self.int_ip = ip
                        else :
                            self.ext_ip = ip
                    if self.ext_ip :
                        break
        if not self.ext_ip :
            logger.debug( "Ending  fedcloud's get_public_ip function with an error" )
            raise Exception( "Impossible to get a public IP" )
        logger.debug( "Ending  fedcloud's get_public_ip function\n" )

    def get_network_interfaces(self):
        logger.debug( "\nRunning fedcloud's  get_network_interfaces function" )
        cmd = "occi --endpoint %s --auth x509 --user-cred %s --voms --action list --resource network" % (
                             self.endpoint, self.proxy_file )
        out, err = self.com_object.execCommand( cmd )

        if 'certificate expired' in err :
            self._renew_voms_proxy()
            out, err = self.com_object.execCommand( cmd )

        elif err :
            logger.debug( "Ending  fedcloud's get_network_interfaces function with an error" )
            raise Exception( "Error getting network list" )
        logger.debug( "Ending  fedcloud's get_network_interfaces function\n" )
        return out.strip().split()

    def get_ip(self):
        logger.debug( "\nRunning fedcloud's  get_ip function" )
        out = self.get_description(self.id)
        pattern = re.compile( "occi.networkinterface.address\s*=\s*(.*)" )
        mo = pattern.findall( out )
        if mo :
            ip = mo[ 0 ]
            if not is_ip_private( ip ) :
                self.ext_ip = ip
                self.int_ip = ip
            else :
                self.get_public_ip()
        else :
            logger.debug( "Ending  fedcloud's get_ip function with an error" )
            raise Exception( "Error getting IP" )

        logger.debug( "*********** get_ip -- self.int_ip ***********" )
        logger.debug( str(self.int_ip) )
        logger.debug( "*********** get_ip -- self.ext_ip ***********" )
        logger.debug( str(self.ext_ip) )
        logger.debug( "*********** get_ip -- ip ***********" )
        logger.debug( str(ip) )
        logger.debug( "*********** get_ip -- end ***********\n\n" )
        logger.debug( "Ending  fedcloud's get_ip function\n" )