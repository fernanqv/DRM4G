import re
import time
import uuid
import logging
from datetime       import timedelta, datetime
from os.path        import exists, join
from hdcloud.utils  import ( exec_cmd, generate_key, 
                            read_key, download_proxy,
                            is_ip_private )
from drm4g          import DRM4G_DIR

__version__  = '0.1.0'
__author__   = 'Carlos Blanco'
__revision__ = "$Id$"

generic_cloud_cfg = """
#cloud-config
users:
  - name: drm4g_admin
    shell: /bin/bash
    sudo: ALL=(ALL) NOPASSWD:ALL
    lock-passwd: true+
    ssh-import-id: drm4g_admin
    ssh-authorized-keys:
      - %s
"""

class Contextualization(object):

    def __init__(self, basic_data):
        infra_cfg = basic_data.cloud_setup[ basic_data.cluster_setup.infrastructure ]
        self.vo = infra_cfg.vo
        self.proxy_file = join(DRM4G_DIR, "var", "x509up")
        try :
            self.myproxy_server = basic_data.cluster_setup.credentials[ "myproxy_server" ]
            self.myproxy_password = basic_data.cluster_setup.credentials[ "myproxy_password" ] 
            self.username = basic_data.cluster_setup.credentials[ "username" ]
        except KeyError :
            raise Exception( "There is an issue regarding credentials parameters" )
        self.pub_file = "id_rsa" 
    
    def _create_contex(self, pub):
        with open( self.pub_file + ".login", "w" ) as file :
            file.writelines( generic_cloud_cfg % pub )

    def create(self):
        code, out = generate_key( self.pub_file )
        if code :
            raise Exception( "Error generating keys: %s" % out )
        try :
            pub = read_key( self.pub_file + ".pub" )
            self._create_contex( pub )
        except Exception as err :
            raise Exception( "Error writing contextualization file: " + str( err ) )
        if not exists( self.proxy_file ) :
            #code, out = download_proxy( self.myproxy_server, self.myproxy_password, self.vo, self.proxy_file, self.username )
            #if code :
            #    raise Exception( "Error downloading generating keys: %s" % out )
            raise Exception( "The proxy file could not be found under $DRM4G_DIR/etc.\n" \
                "   - To generate it run the following command:\n" \
                "       voms-proxy-init -voms fedcloud.egi.eu --rfc --out $DRM4G_DIR/var/x509up")
       
class Instance(object):

    def __init__(self, basic_data):
        self.id = None
        self.volume_id = None
        self.id_link = None
        self.int_ip = None
        self.ext_ip = None
        self.context_file = "id_rsa.login"
        self.volume = basic_data.cluster_setup.volume
        self.app_name = basic_data.cluster_setup.app
        infra_cfg = basic_data.cloud_setup[ basic_data.cluster_setup.infrastructure ]
        cloud_cfg = infra_cfg.clouds[ basic_data.cluster_setup.cloud ]
        self.endpoint = cloud_cfg[ "endpoint" ]
        self.flavour = cloud_cfg[ "flavours" ] [ basic_data.cluster_setup.flavour ]
        self.app = cloud_cfg[ "apps" ] [ self.app_name ]
        self.proxy_file = "x509up"

    def create(self):
        if self.volume:
            self._create_volume()
            self._wait_storage()
        self._create_resource()
        self._wait_compute()
        if self.volume :
            self._create_link()

    def _wait_storage(self):
        now = datetime.now()
        end = now + timedelta( minutes = 60 )

        while now <= end :
            out = self.get_description(self.id_volume)
            pattern = re.compile( "occi.storage.state\s*=\s*(.*)" )
            mo = pattern.findall( out )
            if mo and mo[ 0 ] == "online" :
                break
            now += timedelta( seconds = 10 )

    def _wait_compute(self):
        now = datetime.now()
        end = now + timedelta( minutes = 60 )

        while now <= end :
            out = self.get_description(self.id)
            pattern = re.compile( "occi.compute.state\s*=\s*(.*)" )
            mo = pattern.findall( out )
            if mo and mo[ 0 ] == "active" :
                break
            now += timedelta( seconds = 10 )

    def _create_link(self):
        cmd = 'occi --endpoint %s --auth x509 --user-cred %s --voms --action link ' \
              '--resource %s -j %s' % (self.endpoint, self.proxy_file, self.id, self.id_volume )
        code, out = exec_cmd( cmd )
        if code :
            raise Exception( "Error linking resource and volume: %s" % out )
        self.id_link = out.rstrip('\n')

    def _create_resource(self):
        cmd = 'occi --endpoint %s --auth x509 --user-cred %s --voms --action create --attribute occi.core.title="%sVM_%s" ' \
                  '--resource compute --mixin %s --mixin %s --context user_data="file://$PWD/%s"' % (
                         self.endpoint, self.proxy_file, str(self.app_name).lower(), uuid.uuid4().hex, self.app, self.flavour, self.context_file )
        code, out = exec_cmd( cmd )
        if code :
            raise Exception( "Error creating VM : %s" % out )
        self.id = out.rstrip('\n')

    def _create_volume(self):
        cmd = "occi --endpoint %s --auth x509 --user-cred %s --voms --action create --resource storage --attribute " \
              "occi.storage.size='num(%s)' --attribute occi.core.title=%s_workspace_%s" % (
                     self.endpoint, self.proxy_file, str( self.volume ), str(self.app_name).lower(), uuid.uuid4().hex )
        code, out = exec_cmd( cmd )
        if code :
            raise Exception( "Error creating volume : %s" % out )
        self.id_volume = out.rstrip('\n')

    def delete(self):
        if self.volume :
            cmd = "occi --endpoint %s --auth x509 --user-cred %s --voms --action unlink --resource %s" % (
                             self.endpoint, self.proxy_file, self.id_link )
            code, out = exec_cmd( cmd )
            if code :
                logging.error( "Error unlinking volume '%s': %s" % ( self.id_volume, out ) )
            time.sleep( 20 )
            cmd = "occi --endpoint %s --auth x509 --user-cred %s --voms --action delete --resource %s" % (
                             self.endpoint, self.proxy_file, self.id_volume )
            code, out = exec_cmd( cmd )
            if code :
                logging.error( "Error deleting volume '%s': %s" % ( self.id_volume, out ) )
        cmd = "occi --endpoint %s --auth x509 --user-cred %s --voms --action delete --resource %s" % (
                             self.endpoint, self.proxy_file, self.id )
        code, out = exec_cmd( cmd )
        if code :
            logging.error( "Error deleting node '%s': %s" % ( self.id, out ) )

    def get_description(self, id):
        cmd = "occi --endpoint %s --auth x509 --user-cred %s --voms --action describe --resource %s" % (
                             self.endpoint, self.proxy_file, id )
        code, out = exec_cmd( cmd )
        if code :
            raise Exception( "Error getting description node '%s': %s" % ( id, out ) )
        return out

    def get_public_ip(self):
        network_interfaces = self.get_network_interfaces()
        for network_interface in network_interfaces[::-1] :
            cmd = "occi --endpoint %s --auth x509 --user-cred %s --voms --action link --resource %s --link %s" % (
                      self.endpoint, self.proxy_file, self.id, network_interface )
            code, out = exec_cmd( cmd )
            if not code :
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
            raise Exception( "Impossible to get a public IP" )

    def get_network_interfaces(self):
        cmd = "occi --endpoint %s --auth x509 --user-cred %s --voms --action list --resource network" % (
                             self.endpoint, self.proxy_file )
        code, out = exec_cmd( cmd )
        if code :
            raise Exception( "Error getting network list" )
        return out.strip().split()

    def get_ip(self):
        out = self.get_description(self.id)
        pattern = re.compile( "occi.networkinterface.address\s*=\s*(.*)" )
        mo = pattern.findall( out )
        if mo :
            ip = mo[ 0 ]
            if not is_ip_private( ip ) :
                self.int_ip = self.ext_ip = ip
            else :
                self.get_public_ip()
        else :
            raise Exception( "Error getting IP" )
