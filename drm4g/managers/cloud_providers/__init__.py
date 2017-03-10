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
import threading
import pickle
import sqlite3
import drm4g.managers
import drm4g.managers.fork
from math                     import ceil
from drm4g.managers           import logger
from drm4g.utils.importlib    import import_module
from drm4g                    import DRM4G_DIR, CLOUD_CONNECTORS
from os.path                  import exists, join

lock = threading.RLock()
#logger = logging.getLogger(__name__)
pickled_file = join(DRM4G_DIR, "var", "%s_pickled")
resource_conf_db = os.path.join(DRM4G_DIR, "var", "resource_conf.db")

def pickle_read(resource_name, cloud_connector):
    '''
    Reads the pickled file and returns all of the VMs created for the specified resource
    @param resource_name: name of the resource 
    @return: instances : list of VMs
    '''
    instances = []
    with open(pickled_file % cloud_connector + "_" + resource_name, "r") as pf:
        while True:
            try:
                instances.append(pickle.load(pf))
            except EOFError:
                break    
    if not instances:
        logger.error("There are no VMs defined in '%s' or the file is not well formed." % (pickled_file % cloud_connector + "_" + resource_name))
        exit(1)
    return instances

def pickle_remove(inst, resource_name, cloud_connector):
    '''
    Deletes a VM from the pickled file 
    @param: inst : VMs to be eliminated
    @param resource_name: name of the resource
    '''
    with lock:
        try:
            instances = pickle_read(resource_name, cloud_connector)

            with open( pickled_file % cloud_connector + "_" + resource_name, "w" ) as pf :
                for instance in instances:
                    if instance.ext_ip != inst.ext_ip :
                        pickle.dump( instance, pf )

            if len(instances) == 1 :
                os.remove( pickled_file % cloud_connector + "_" + resource_name )
        except Exception as err:
            logger.error( "Error deleting instance from pickled file %s\n%s" % (pickled_file % cloud_connector + "_" + resource_name, str( err )) )

def pickle_dump(instance, resource_name, cloud_connector):
    '''
    Adds a VM to the pickled file 
    @param: inst : VMs to be eliminated
    @param resource_name: name of the resource
    '''
    lock.acquire()
    try:
        with open(pickled_file % cloud_connector + "_" + resource_name, "a") as pf:
            pickle.dump(instance, pf)
    except Exception as err:
        logger.error( "Error adding instance into pickled file %s\n%s" % (pickled_file % cloud_connector + "_" + resource_name, str( err )) )
    finally:
        lock.release()
        
def create_num_instances(num_instances, resource_name, config):
    '''
    Creates a specified number of VMs for a selected resource configuration
    '''
    threads = []
    for number_of_th in range( num_instances ):
        th = threading.Thread( target = start_instance, args = ( config, resource_name, ) )
        th.start()
        threads.append( th )
    [ th.join() for th in threads ]

def start_instance( config, resource_name ) :
    """
    Creates a VM using the configuration indicated by a selected resource
    """
    try :
        if config['cloud_connector'] == 'rocci':
            hdpackage = import_module( CLOUD_CONNECTORS[config['cloud_connector']] + ".%s" % 'rocci' )
        else:
            hdpackage = import_module( CLOUD_CONNECTORS[config['cloud_connector']] )
    except Exception as err :
        raise Exception( "The infrastructure selected does not exist. "  + str( err ) )
    
    try:
        instance = eval( "hdpackage.Instance( config )" )
    except KeyError as err:
        logger.error( "Either you have defined an incorrect value in your configuration file 'resources.conf'" \
            " or there's a value that doesn't correspond with any of the keys in your cloud setup file 'cloudsetup.json':" )
        raise
    except Exception as err:
        logger.error( "An error occurred while trying to create a VM instance\n%s" % str( err ) )
        raise
    try:   
        instance.create()
        instance.get_ip()
        pickle_dump(instance, resource_name, config['cloud_connector'])
        
        #if exists( resource_conf_db ): #the configure will always have had it's load() method executed, so this is not needed
        try:
            #with lock:
            conn = sqlite3.connect(resource_conf_db)
            with lock:
                with conn:
                    cur = conn.cursor()
                    '''
                    cur.execute("SELECT vms, id FROM Resources WHERE name='%s'" % (resource_name))
                    vms, resource_id = cur.fetchone()
                    vms += 1
                    cur.execute("UPDATE Resources SET vms = %d WHERE name = '%s'" % (vms, resource_name))
                    '''
                    cur.execute("SELECT id FROM Resources WHERE name = '%s'" % resource_name)
                    resource_id = cur.fetchone()[0]
                    cur.execute("SELECT count(*) FROM VM_Pricing WHERE name = '%s'" % (resource_name+"_"+instance.ext_ip))
                    data=cur.fetchone()[0]
                    #if resource_id:
                    #with lock:
                    if data==0:
                        cur.execute("INSERT INTO VM_Pricing (name, resource_id, state, pricing, start_time) VALUES ('%s', %d, '%s', %f, %f)" % ((resource_name+"_"+instance.ext_ip), resource_id, 'active', instance.instance_pricing, instance.start_time))
                    else:
                        cur.execute("UPDATE VM_Pricing SET resource_id = %d, state = '%s', pricing = %f, start_time = %f WHERE name = '%s'" % (resource_id, 'active', instance.instance_pricing, instance.start_time, (resource_name+"_"+instance.ext_ip)))
        except Exception as err :
            raise Exception( "Error updating instance information in the database %s: %s" % (resource_conf_db, str( err )) )
    except Exception as err :
        logger.error( "Error creating instance: %s" % str( err ) )
        try :
            logger.debug( "Trying to destroy the instance" )
            stop_instance(config, instance, resource_name)
        except Exception as err :
            logger.error( "Error destroying instance\n%s" % str( err ) ) 
            
def delete_vm_from_db(instance, resource_name):
    try:
        #with lock:
        conn = sqlite3.connect(resource_conf_db)
        with conn:
            with lock:
                cur = conn.cursor()
                cur.execute("SELECT vms FROM Resources WHERE name = '%s'" % resource_name)
                vms = cur.fetchone()[0]
                cur.execute("UPDATE Resources SET vms= %d WHERE name = '%s'" % ((vms-1), resource_name))
                cur.execute("DELETE FROM VM_Pricing where name = '%s'" % (resource_name+"_"+instance.ext_ip))
    except Exception as err :
        raise Exception( "Error deleting instance information in the database %s: %s" % (resource_conf_db, str( err )) )

def stop_instance( config, instance, resource_name ):
    """
    Destroys one VM and eliminates it from the pickled file
    """
    try :
        pickle_remove(instance, resource_name, config['cloud_connector'])
        delete_vm_from_db(instance, resource_name)
        instance.destroy()
    except Exception as err :
        logger.error( "Error destroying instance\n%s" % str( err ) )
        
def manage_instances(args, resource_name, config):
    """
    Either creates as many VMs as indicated by the resource configuration
    or destroys all VMs for a selected resource
    """
    if args == "start" :
        conn = sqlite3.connect(resource_conf_db)
        with lock:
            with conn:
                #this updates the number of VMs in the database for "resource_name", but only when creating VMs through the command "drm4g resource create"
                cur = conn.cursor()
                cur.execute("SELECT vms FROM Resources WHERE name='%s'" % (resource_name))
                vms = cur.fetchone()[0]
                vms += int(config['min_nodes'])
                cur.execute("UPDATE Resources SET vms = %d WHERE name = '%s'" % (vms, resource_name))
                #config.resources[ resource_name ][ 'vm_instances' ] = vms #I can't update this value                         
        threads = []
        for number_of_th in range( int(config['min_nodes']) ):
            th = threading.Thread( target = start_instance, args = ( config, resource_name, ) )
            th.start()
            threads.append( th )
        [ th.join() for th in threads ]
    elif args == "stop" :
        cloud_connector = config['cloud_connector']
        if not exists( pickled_file % cloud_connector + "_" + resource_name ):
            logger.error( "There are no available VMs to be deleted for the resource %s" % resource_name )
        else:
            '''
            instances = []
            with open( pickled_file+"_"+resource_name, "r" ) as pf :
                while True :
                    try:
                        instances.append( pickle.load( pf ) )
                    except EOFError :
                        break
            if not instances :
                logger.error( "There are no VMs defined in '%s' or the file is not well formed." % (pickled_file+"_"+resource_name) )
                exit( 1 )
            '''
            instances = pickle_read(resource_name, cloud_connector)
            threads = []
            for instance in instances :
                th = threading.Thread( target = stop_instance, args = ( config, instance, resource_name ) )
                th.start()
                threads.append( th )
            [ th.join() for th in threads ]
        #if exists(resource_conf_db):
        #    os.remove( resource_conf_db ) 
    else :
        logger.error( "Invalid option" )
        exit( 1 )
        

class Instance(object):

    DEFAULT_USER = "drm4g_adm"
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
        with open( self.cloud_contextualisation_file, "r" ) as contex_file :
            cloud_config = contex_file.read()
        cloud_config = cloud_config % (user, user, public_key)
        if user_cloud_config:
            cloud_config += "\n%s\n\n" % user_cloud_config.replace("\\n", "\n")
        return cloud_config
    
    
class Resource (drm4g.managers.Resource):
    def hosts(self):
        """
        It will return a string with the host available in the resource.
        """
        if 'cloud_connector' in self.features :
            self.host_list = [ "" ]
            return ""
        else :
            self.host_list = [ self.name ]
            return self.name


class Job (drm4g.managers.fork.Job):
    pass
