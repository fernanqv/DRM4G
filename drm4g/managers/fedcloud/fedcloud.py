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
        self.data=basic_data
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
        self.comm = basic_data[ 'communicator' ]
        self.max_jobs_running = basic_data['max_jobs_running']
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

        communicator = import_module(COMMUNICATORS[ basic_data[ 'communicator' ] ] )
        com_obj = getattr( communicator , 'Communicator' ) ()
        com_obj.username       = basic_data['username']
        com_obj.frontend       = basic_data['frontend']
        com_obj.private_key    = self.private_key
        com_obj.public_key     = basic_data.get('public_key', self.private_key+'.pub')
        com_obj.work_directory = basic_data.get('scratch', REMOTE_JOBS_DIR)
        self.com_object=com_obj

        self.proxy_file = join( REMOTE_VOS_DIR , "x509up.%s" ) % self.vo

        cmd = "ls %s" % self.context_file #to check if it exists
        out,err = self.com_object.execCommand( cmd )
        if err:
            content= generic_cloud_cfg % (self.vo_user, self.vo_user, pub)
            cmd = "echo '%s' > %s" % (content, self.context_file)
            out,err=self.com_object.execCommand( cmd )
            if err:
                raise Exception("Wasnt't able to create the context file %s." % self.context_file + err)

        cmd = "ls %s" % self.proxy_file #to check if it exists
        out,err = self.com_object.execCommand( cmd )
        if err:
            self._renew_voms_proxy()

    #This is here to avoid having the error "TypeError: can't pickle lock objects" when creating the pickled file
    def __getstate__(self):
        odict = self.__dict__.copy()
        del odict['com_object']
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

    def _exec_remote_cmd(self, command):
        logger.debug("~~~~~~~~~~~~~~~~ Going to execute remote command: ~~~~~~~~~~~~~~~~\n"+command)
        out, err = self.com_object.execCommand( command )
        logger.debug("~~~~~~~~~~~~~~~~         Command executed         ~~~~~~~~~~~~~~~~")
        return out, err

    def _renew_voms_proxy(self, cont=0):
        try:
            logger.debug( "Running fedcloud's _renew_voms_proxy function" )
            logger.debug( "_renew_voms_proxy count = %s" % str( cont ) )
            logger.error( "The proxy '%s' has probably expired" %  self.proxy_file )

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

            out, err = self._exec_remote_cmd( cmd )
            self.log_output("_renew_voms_proxy", out, err)

            if err:
                logger.debug( "Ending  fedcloud's _renew_voms_proxy function with an error" )
                logger.error( "Error renewing the proxy(%s): %s" % ( cmd , err ) )
                raise Exception("Most probably the proxy certificate hasn't been created. Be sure to run the the following command before trying again:" \
                    "\n    \033[93mdrm4g id <resource_name> init\033[0m")
            logger.debug( "Ending  fedcloud's _renew_voms_proxy" )
        except socket.timeout:
            logger.debug("Captured a socket.time exception")
            if cont<3:
                self._renew_voms_proxy(cont+1)
            else:
                raise

    def create(self):
        logger.debug( "Running fedcloud's  create function" )
        if self.volume:
            self._create_volume()
            self._wait_storage()
        self._create_resource()
        self._wait_compute()
        if self.volume :
            self._create_link()
        logger.debug( "Ending  fedcloud's create function" )

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
        logger.debug( "Ending  fedcloud's _wait_storage function" )

    def _wait_compute(self):
        logger.debug( "Running fedcloud's _wait_compute function" )
        now = datetime.now()
        end = now + timedelta( minutes = 60 )

        while now <= end :
            logger.debug( "  * _wait_compute - waited for %s minutes" % (timedelta( minutes = 60 ) - (end-now)) )
            out = self.get_description(self.id)
            pattern = re.compile( "occi.compute.state\s*=\s*(.*)" )
            mo = pattern.findall( out )
            logger.debug( "The resource's state is %s" % mo[ 0 ] )
            if mo and mo[ 0 ] == "active" :
                break
            time.sleep(10)
            now += timedelta( seconds = 10 )
        logger.debug( "Ending  fedcloud's _wait_compute function" )

    def _create_link(self):
        logger.debug( "Running fedcloud's _create_link function" )
        cmd = 'occi --endpoint %s --auth x509 --user-cred %s --voms --action link ' \
              '--resource %s -j %s' % (self.endpoint, self.proxy_file, self.id, self.id_volume )
        out, err = self._exec_remote_cmd( cmd )
        self.log_output("_create_link", out, err)
        
        if 'certificate expired' in err :
            self._renew_voms_proxy()
            out, err = self._exec_remote_cmd( cmd )
            self.log_output("_create_link 2", out, err)
        elif err :
            logger.error( "Ending  fedcloud's _create_link function with an error" )
            raise Exception( "Error linking resource and volume: %s" % out )
        self.id_link = out.rstrip('\n')
        logger.debug( "Ending  fedcloud's _create_link function" )

    def _create_resource(self):
        try:
            logger.debug( "Running fedcloud's _create_resource function" )
            cmd = 'occi --endpoint %s --auth x509 --user-cred %s --voms --action create --attribute occi.core.title="%s_DRM4G_VM_%s" ' \
                      '--resource compute --mixin %s --mixin %s --context user_data="file://$PWD/%s"' % (
                             self.endpoint, self.proxy_file, str(self.app_name).lower(), uuid.uuid4().hex, self.app, self.flavour, self.context_file )
            out, err = self._exec_remote_cmd( cmd )
            self.log_output("_create_resource", out, err)
            
            if 'certificate expired' in err :
                self._renew_voms_proxy()
                logger.debug( "After executing _renew_voms_proxy - Going to execute cmd again" )
                out, err = self._exec_remote_cmd( cmd )
                self.log_output("_create_resource 2", out, err)
            elif err :
                logger.error( "Ending  fedcloud's  _create_resource function with an error" )
                raise Exception( "Error creating VM : %s" % out )
            self.id = out.rstrip('\n')
            logger.debug( "Ending  fedcloud's  _create_resource function" )
        except Exception as err:
            ####################################al probarlo con el IFCA por ejemplo, aparece el mensaje, pero no aparece ninguna exception############################################
            '''
            haciendolo manualmente aparece este error:
                F, [2016-11-10T15:25:31.150145 #7314] FATAL -- : [rOCCI-cli] Connection to "https://cloud.ifca.es:8787/occi1.1" timed out!
            '''
            logger.error("It's probably a timeout error:\n"+str(err))

    def _create_volume(self):
        logger.debug( "Running fedcloud's _create_volume function" )
        cmd = "occi --endpoint %s --auth x509 --user-cred %s --voms --action create --resource storage --attribute " \
              "occi.storage.size='num(%s)' --attribute occi.core.title=%s_DRM4G_Workspace_%s" % (
                     self.endpoint, self.proxy_file, str( self.volume ), str(self.app_name).lower(), uuid.uuid4().hex )
        out, err = self._exec_remote_cmd( cmd )
        self.log_output("_create_volume", out, err)

        if 'certificate expired' in err :
            self._renew_voms_proxy()
            out, err = self._exec_remote_cmd( cmd )
            self.log_output("_create_volume 2", out, err)
        elif err :
            logger.error( "Ending  fedcloud's _create_volume function with an error" )
            raise Exception( "Error creating volume : %s" % out )
        self.id_volume = out.rstrip('\n')
        logger.debug( "Ending  fedcloud's _create_volume function" )

    def delete(self):
        logger.debug( "Running fedcloud's  delete function" )
        if self.volume :
            #cmd = "occi --endpoint %s --auth x509 --user-cred %s --voms --action unlink --resource /compute/%s" % (
            #                 self.endpoint, self.proxy_file, basename(self.id_link) )
            cmd = "occi --endpoint %s --auth x509 --user-cred %s --voms --action unlink --resource %s" % (
                             self.endpoint, self.proxy_file, self.id_link )
            out, err = self._exec_remote_cmd( cmd )
            self.log_output("delete (unlink)", out, err)
        
            #if ( 'The proxy has EXPIRED' in out ) or ( 'is not accessible' in err ) :
            if 'certificate expired' in err :
                self._renew_voms_proxy()
                logger.debug( "After executing _renew_voms_proxy - Going to execute cmd again" )
                out, err = self._exec_remote_cmd( cmd )
                self.log_output("delete (unlink) 2", out, err)
            elif err :
                logging.error( "Error unlinking volume '%s': %s" % ( self.id_volume, out ) )
            time.sleep( 20 )
            cmd = "occi --endpoint %s --auth x509 --user-cred %s --voms --action delete --resource %s" % (
                             self.endpoint, self.proxy_file, self.id_volume )
            out, err = self._exec_remote_cmd( cmd )
            self.log_output("delete (volume)", out, err)

            if 'certificate expired' in err :
                self._renew_voms_proxy()
                logger.debug( "After executing _renew_voms_proxy - Going to execute cmd again" )
                out, err = self._exec_remote_cmd( cmd )
                self.log_output("delete (volume) 2", out, err)
            elif err :
                logging.error( "Error deleting volume '%s': %s" % ( self.id_volume, out ) )
        cmd = "occi --endpoint %s --auth x509 --user-cred %s --voms --action delete --resource %s" % (
                             self.endpoint, self.proxy_file, self.id )
        out, err = self._exec_remote_cmd( cmd )        
        self.log_output("delete (resource)", out, err)

        if 'certificate expired' in err :
            self._renew_voms_proxy()
            logger.debug( "After executing _renew_voms_proxy - Going to execute cmd again" )
            out, err = self._exec_remote_cmd( cmd )
            self.log_output("delete (resource) 2", out, err)
        elif err :
            logging.error( "Error deleting node '%s': %s" % ( self.id, out ) )
        logger.debug( "Ending  fedcloud's delete function" )

    def get_description(self, id):
        logger.debug( "Running fedcloud's  get_description function" )
        cmd = "occi --endpoint %s --auth x509 --user-cred %s --voms --action describe --resource %s" % (
                             self.endpoint, self.proxy_file, id )
        out, err = self._exec_remote_cmd( cmd )
        self.log_output("get_description", out, err)

        if 'certificate expired' in err :
            self._renew_voms_proxy()
            logger.debug( "After executing _renew_voms_proxy - Going to execute cmd again" )
            out, err = self._exec_remote_cmd( cmd )
            self.log_output("get_description 2", out, err)
        elif err and "Insecure world writable dir" not in err:
            logger.error( "Ending  fedcloud's get_description function with an error" )
            raise Exception( "Error getting description node '%s': %s" % ( id, out ) )
        logger.debug( "Ending  fedcloud's get_description function" )
        return out

    def get_floating_ips(self):
        logger.debug( "Running fedcloud's  get_floating_ips function" )
        cmd = "occi --endpoint %s --auth x509 --user-cred %s --voms --dump-model | grep 'http://schemas.openstack.org/network/floatingippool'" % (
                      self.endpoint, self.proxy_file )
        out, err = self._exec_remote_cmd( cmd )
        self.log_output("get_floating_ips (floating check)", out, err)

        if 'certificate expired' in err :
            self._renew_voms_proxy()
            out, err = self._exec_remote_cmd( cmd )
            self.log_output("get_floating_ips (floating check) 2", out, err)
        elif err :
            logger.error( "Ending  fedcloud's get_floating_ips function with an error" )
        logger.debug( "Ending  fedcloud's get_floating_ips function" )
        return out

    def get_public_ip(self):
        logger.debug( "Running fedcloud's  get_public_ip function" )
        network_interfaces = self.get_network_interfaces()
        network_interface=""
        for n in network_interfaces[::-1] :
            if basename(n).lower() == 'public':
                network_interface = n
        if network_interface:
            cmd = "occi --endpoint %s --auth x509 --user-cred %s --voms --action link --resource %s --link %s" % (
                      self.endpoint, self.proxy_file, self.id, network_interface )
            out, err = self._exec_remote_cmd( cmd )
            self.log_output("get_public_ip", out, err)
        
            if 'certificate expired' in err :
                self._renew_voms_proxy()
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
                          self.endpoint, self.proxy_file, self.id, network_interface, mixin )
                    out, err = self._exec_remote_cmd( cmd )
                    self.log_output("get_public_ip", out, err)

                    if 'No more floating ips in pool' in str(err):
                        cont+=1
                    elif 'certificate expired' in str(err) :
                        self._renew_voms_proxy()
                        continue
                    elif err:
                        logger.debug("An unexpected error occurred:\n"+str(err))
                        cont+=1
                    elif out:
                        #I'm assuming that out will have something that resembles this:
                        #http://stack-server-02.ct.infn.it:8787/network/interface/c6886e72-86bd-4a08-ab2b-f0769854a38a_90.147.16.53
                        #which is what the link commmand returns without that last "mixin" option
                        cond=True
                        logger.debug("\n\n\nI don't know if I will ever get to this point since I'm still not sure what's returned by the last executed command (since it's never worked until now)\n\n\n")
                    else:
                        logger.debug("There wasn't either an output or an error for the execution of the 'get_public_ip' function")
            else:
                raise Exception("Error trying to get a public IP")
         
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
        if not self.ext_ip :
            logger.error( "Ending  fedcloud's get_public_ip function with an error" )
            raise Exception( "Error trying to get a public IP" )
        logger.debug( "Ending  fedcloud's get_public_ip function" )

    def get_network_interfaces(self):
        logger.debug( "Running fedcloud's  get_network_interfaces function" )
        cmd = "occi --endpoint %s --auth x509 --user-cred %s --voms --action list --resource network" % (
                             self.endpoint, self.proxy_file )
        out, err = self._exec_remote_cmd( cmd )
        self.log_output("get_network_interfaces", out, err)
        
        if 'certificate expired' in err :
            self._renew_voms_proxy()
            out, err = self._exec_remote_cmd( cmd )
            self.log_output("get_network_interfaces 2", out, err)
        elif err :
            logger.error( "Ending  fedcloud's get_network_interfaces function with an error" )
            raise Exception( "Error getting network list" )
        logger.debug( "Ending  fedcloud's get_network_interfaces function" )
        return out.strip().split()

    def get_ip(self):
        logger.debug( "Running fedcloud's  get_ip function" )
        out = self.get_description(self.id)
        pattern = re.compile( "occi.networkinterface.address\s*=\s*(.*)" )
        mo = pattern.findall( out )
        if mo :
            ip = mo[ 0 ]
            if not is_ip_private( ip ) :
                #logger.error ("\n\n\nIdentification: "+str(self.id)+"\n    ip: "+str(ip)+"\n\n\n")
                self.ext_ip = ip
                self.int_ip = ip
            else :
                self.get_public_ip()
            #logger.error ("\n\n\nIdentification: "+str(self.id)+"\n    ext_ip: "+str(self.ext_ip)+"\n\n\n")
        else :
            logger.error( "Ending  fedcloud's get_ip function with an error" )
            raise Exception( "Error getting IP" )

        logger.debug( "*********** get_ip -- self.int_ip ***********" )
        logger.debug( str(self.int_ip) )
        logger.debug( "*********** get_ip -- self.ext_ip ***********" )
        logger.debug( str(self.ext_ip) )
        logger.debug( "*********** get_ip -- ip ***********" )
        logger.debug( str(ip) )
        logger.debug( "*********** get_ip -- end ***********" )
        logger.debug( "Ending  fedcloud's get_ip function" )

    def log_output(self, msg, out, err, extra=None):        
        logger.debug( "Command return:" )
        logger.debug( "*********** %s -- out ***********" % msg )
        logger.debug( str(out) )
        logger.debug( "*********** %s -- err ***********" % msg )
        logger.debug( str(err) )
        logger.debug( "*********** %s -- end ***********" % msg )
