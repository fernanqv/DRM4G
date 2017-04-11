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
import re
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
try:
    from configparser       import SafeConfigParser
except ImportError:
    from ConfigParser       import SafeConfigParser  # ver. < 3.0

lock = threading.RLock()
#logger = logging.getLogger(__name__)
pickled_file = join(DRM4G_DIR, "var", "%s_pickled.pkl")
resource_conf_db = join(DRM4G_DIR, "var", "resource_conf.db")

def pickle_read(resource_name, cloud_connector):
    '''
    Reads the pickled file and returns all of the VMs created for the specified resource
    @param resource_name: name of the resource 
    @return: instances : list of VMs
    '''
    instances = []
    if exists(pickled_file % ( cloud_connector + "_" + resource_name )):
        with lock:
            with open(pickled_file % ( cloud_connector + "_" + resource_name ), "r") as pf:
                while True:
                    try:
                        instances.append(pickle.load(pf))
                    except EOFError:
                        break
        if not instances:
            logger.error("There are no VMs defined in '%s' or the file is not well formed." % (pickled_file % ( cloud_connector + "_" + resource_name )))
    else:
        #logger.warn( "The file '%s' doesn't exist" % (pickled_file % ( cloud_connector + "_" + resource_name )) )
        logger.error( "The file '%s' doesn't exist, so there are no available VMs for the resource %s" % ((pickled_file % ( cloud_connector + "_" + resource_name )), resource_name) )
        logger.debug( "Deleting all entries of the resource %s from the database" % resource_name )
        with lock:
            conn = sqlite3.connect(resource_conf_db)
            with conn:
                cur = conn.cursor()
                cur.execute("SELECT count(*) FROM Resources WHERE name = '%s'" % resource_name)
                data=cur.fetchone()[0]
                if data:
                    logger.debug( "    Since it's not possible to recover any instance information for the resource %s, their entries will be deleted from the %s database" % (resource_name, resource_conf_db) )
                    cur.execute("UPDATE Resources SET vms = %d WHERE name = '%s'" % (0, resource_name))
                    cur.execute("SELECT id FROM Resources WHERE name = '%s'" % resource_name)
                    resource_id = cur.fetchone()[0]
                    cur.execute("DELETE FROM VM_Pricing where resource_id = %d" % resource_id)
                    cur.execute("SELECT count(*) FROM Non_Active_VMs WHERE resource_name = '%s'" % resource_name)
                    data=cur.fetchone()[0]
                    if data:
                        cur.execute("DELETE FROM Non_Active_VMs WHERE resource_name = '%s'" % resource_name)
    return instances

def pickle_remove(inst, resource_name, cloud_connector):
    '''
    Deletes a VM from the pickled file 
    @param: inst : VMs to be eliminated
    @param resource_name: name of the resource
    '''
    if exists(pickled_file % ( cloud_connector + "_" + resource_name )):
        instances = pickle_read(resource_name, cloud_connector)
        if instances:
            try:
                with lock:
                    with open( pickled_file % ( cloud_connector + "_" + resource_name ), "w" ) as pf :
                        for instance in instances:
                            if instance.node_id != inst.node_id :
                                pickle.dump( instance, pf )
                
                    if len(instances) == 1 :
                        os.remove( pickled_file % ( cloud_connector + "_" + resource_name ) )
            except Exception as err:
                logger.error( "Error deleting instance from pickled file %s\n%s" % (pickled_file % ( cloud_connector + "_" + resource_name ), str( err )) )
                logger.debug( "Saving the instances back into the pickled file %s" % (pickled_file % ( cloud_connector + "_" + resource_name )) )
                for instance in instances:
                    pickle_dump(instance, resource_name, cloud_connector)
            
def pickle_dump(instance, resource_name, cloud_connector):
    '''
    Adds a VM to the pickled file 
    @param: inst : VMs to be eliminated
    @param resource_name: name of the resource
    '''
    instances = []
    with lock:
        if exists(pickled_file % ( cloud_connector + "_" + resource_name )):
            with open(pickled_file % ( cloud_connector + "_" + resource_name ), "r") as pf:
                while True:
                    try:
                        instances.append(pickle.load(pf))
                    except EOFError:
                        break
        instances.append(instance)
        try:
            with open(pickled_file % ( cloud_connector + "_" + resource_name ), "w") as pf:
                for inst in instances:
                    pickle.dump(inst, pf)
        except Exception as err:
            logger.error( "Error adding instance into pickled file %s\n%s" % (pickled_file % ( cloud_connector + "_" + resource_name ), str( err )) )
    '''
    lock.acquire()
    try:
        with open(pickled_file % ( cloud_connector + "_" + resource_name ), "a") as pf:
            pickle.dump(instance, pf)
    except Exception as err:
        logger.error( "Error adding instance into pickled file %s\n%s" % (pickled_file % ( cloud_connector + "_" + resource_name ), str( err )) )
    finally:
        lock.release()
    '''
      
def create_num_instances(num_instances, resource_name, config):
    '''
    Creates a specified number of VMs for a selected resource configuration
    '''
    threads = []
    for number_of_th in range( num_instances ):
        th = threading.Thread( target = start_instance_no_wait, args = ( config, resource_name, ) )
        th.start()
        threads.append( th )
    [ th.join() for th in threads ]

def start_instance( config, resource_name ) :
    """
    Creates a VM using the configuration indicated by a selected resource
    """
    try :
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

        try:
            with lock:
                conn = sqlite3.connect(resource_conf_db)
                with conn:
                    cur = conn.cursor()
                    cur.execute("SELECT vms, id FROM Resources WHERE name='%s'" % (resource_name))
                    vms, resource_id = cur.fetchone()
                    vms += 1
                    cur.execute("UPDATE Resources SET vms = %d WHERE name = '%s'" % (vms, resource_name))
                    cur.execute("SELECT count(*) FROM VM_Pricing WHERE name = '%s'" % (resource_name+"_"+instance.ext_ip))
                    data=cur.fetchone()[0]
                    if data==0:
                        cur.execute("INSERT INTO VM_Pricing (id, name, resource_id, state, pricing, start_time) VALUES ('%s', '%s', %d, '%s', %f, %f)" % (instance.node_id, (resource_name+"_"+instance.ext_ip), resource_id, 'active', instance.instance_pricing, instance.start_time))
                    else:
                        cur.execute("UPDATE VM_Pricing SET resource_id = %d, state = '%s', pricing = %f, start_time = %f WHERE name = '%s'" % (resource_id, 'active', instance.instance_pricing, instance.start_time, (resource_name+"_"+instance.ext_ip)))
        except Exception as err :
            raise Exception( "Error updating instance information in the database %s: %s" % (resource_conf_db, str( err )) )
        
        pickle_dump(instance, resource_name, config['cloud_connector'])
        
    except Exception as err :
        logger.error( "Error creating instance: %s" % str( err ) )
        try :
            logger.debug( "Destroying the instance" )
            if instance.node_id:
                stop_instance(config, instance, resource_name)
        except Exception as err :
            logger.error( "Error destroying instance\n%s" % str( err ) )

def start_instance_no_wait( config, resource_name ) :
    """
    Creates a VM using the configuration indicated by a selected resource
    """
    try :
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
        if instance.volume_capacity:
            instance._create_volume()
        instance._create_resource()

        active = instance.is_resource_active()
        
        if active:
            instance.get_ip()

        try:
            with lock:
                conn = sqlite3.connect(resource_conf_db)
                with conn:
                    cur = conn.cursor()
                    cur.execute("SELECT vms, id FROM Resources WHERE name='%s'" % (resource_name))
                    vms, resource_id = cur.fetchone()
                    vms += 1
                    cur.execute("UPDATE Resources SET vms = %d WHERE name = '%s'" % (vms, resource_name))
                    if active:
                        cur.execute("INSERT INTO VM_Pricing (id, name, resource_id, state, pricing, start_time) VALUES ('%s', '%s', %d, '%s', %f, %f)" % (instance.node_id, (resource_name+"_"+instance.ext_ip), resource_id, 'active', instance.instance_pricing, instance.start_time))
                    else:
                        cur.execute("INSERT INTO VM_Pricing (id, resource_id, state, pricing, start_time) VALUES ('%s', %d, '%s', %f, %f)" % (instance.node_id, resource_id, 'inactive', instance.instance_pricing, instance.start_time))
                        cur.execute("INSERT INTO Non_Active_VMs (vm_id, resource_name, cloud_connector) VALUES ('%s', '%s', '%s')" % (instance.node_id, resource_name, config['cloud_connector']))
        except Exception as err :
            raise Exception( "Error updating instance information in the database %s: %s" % (resource_conf_db, str( err )) )
            
        pickle_dump(instance, resource_name, config['cloud_connector'])
    except Exception as err :
        logger.error( "Error creating instance: %s" % str( err ) )
        try :
            logger.debug( "Destroying the instance" )
            if instance.node_id:
                stop_instance(config, instance, resource_name)
        except Exception as err :
            logger.error( "Error destroying instance\n%s" % str( err ) )

def check_if_vms_active(inactive_vm_list):
    try:
        for resource_name, cloud_connector in inactive_vm_list:
            updated_list=False
            instances = pickle_read(resource_name, cloud_connector)
            if instances:
                cont = 0
                for instance in instances:
                    if not instance.ext_ip:
                        active = instance.is_resource_active()
                        if active:
                            instance.get_ip()
                            instances[cont] = instance
                            updated_list = True
                            
                            try:
                                with lock:
                                    conn = sqlite3.connect(resource_conf_db)
                                    with conn:
                                        cur = conn.cursor()
                                        cur.execute("UPDATE VM_Pricing SET name = '%s', state = '%s', active_time = %f WHERE id = '%s'" % ((resource_name+"_"+instance.ext_ip), 'active', time.time(), instance.node_id))
                                        cur.execute("DELETE FROM Non_Active_VMs WHERE vm_id = '%s'" % instance.node_id)
                            except Exception as err :
                                raise Exception( "Error updating instance information in the database %s: %s" % (resource_conf_db, str( err )) )
                    cont += 1
                if updated_list:
                    try:
                        with lock:
                            with open( pickled_file % ( cloud_connector + "_" + resource_name ), "w" ) as pf :
                                for instance in instances:
                                    pickle.dump( instance, pf )
                    except Exception as err:
                        #logger.error( "Error updating instances from pickled file %s\n%s" % (pickled_file % ( cloud_connector + "_" + resource_name ), str( err )) )
                        raise Exception( "Error updating instances from pickled file %s: %s" % (pickled_file % ( cloud_connector + "_" + resource_name ), str( err )) )
                        '''
                            NOT SURE ABOUT THIS
                        logger.debug( "Saving the instances back into the pickled file %s" % (pickled_file % ( cloud_connector + "_" + resource_name )) )
                        for instance in instances:
                            pickle_dump(instance, resource_name, cloud_connector)
                        '''
        return True
    except Exception as err:
        logger.error("It was not possible to check if any inactive VMs had been activated: %s" % str(err))
        return False

def destroy_vm_by_name(resource_name, vm_name, cloud_connector):
    '''
    Destroys a specific VM and removes it from the pickled file
    '''
    logger.info("Destroying VM '%s' by name from resource %s" % (vm_name, resource_name))
    instances = pickle_read(resource_name, cloud_connector)
    if instances:
        deleted_instance = None
        vm_deleted = False
        try:
            with lock:
                with open( pickled_file % ( cloud_connector + "_" + resource_name ), "w" ) as pf :
                    for instance in instances:
                        if instance.ext_ip:
                            if (resource_name+'_'+instance.ext_ip) != vm_name :
                                pickle.dump( instance, pf )
                            else:
                                deleted_instance = instance
                                instance.destroy()
                                vm_deleted = True
                if len(instances) == 1 :
                    logger.info("    Deleting %s since there was only one VM left and it has been destroyed" % (pickled_file % ( cloud_connector + "_" + resource_name )))
                    os.remove( pickled_file % ( cloud_connector + "_" + resource_name ) )
        except Exception as err:
            logger.error( "Error destroying instance by name and deleting instance from pickled file %s\n%s" % (pickled_file % ( cloud_connector + "_" + resource_name ), str( err )) )
            logger.debug( "Saving the instances back into the pickled file %s" % (pickled_file % ( cloud_connector + "_" + resource_name )) )
            for instance in instances:
                pickle_dump(instance, resource_name, cloud_connector)
        try:
            if vm_deleted:
                delete_vm_from_db(deleted_instance, resource_name)
        except Exception as err:
            logger.error( "Destroyed instance by name and deleted instance from pickled file but not from the database: %s" % str(err) )
    
def delete_vm_from_db(instance, resource_name):
    if instance.ext_ip:
        logger.info("    Deleting VM '%s' with the name %s from the database" % (instance.node_id, (resource_name+"_"+instance.ext_ip)))
    else:
        logger.info("    Deleting VM '%s' from the database" % instance.node_id)
    try:
        with lock:
            conn = sqlite3.connect(resource_conf_db)
            with conn:
                cur = conn.cursor()
                '''
                cur.execute("SELECT vms FROM Resources WHERE name = '%s'" % resource_name)
                vms = cur.fetchone()[0]
                '''
                cur.execute("SELECT vms, past_expenditure FROM Resources WHERE name = '%s'" % resource_name)
                vms, past_expenditure = cur.fetchone()
                vms -= 1
                past_expenditure += instance.current_balance()
                cur.execute("UPDATE Resources SET vms = %d, past_expenditure = %f WHERE name = '%s'" % (vms, past_expenditure, resource_name))
                #cur.execute("DELETE FROM VM_Pricing where name = '%s'" % (resource_name+"_"+instance.ext_ip))
                cur.execute("DELETE FROM VM_Pricing where id = '%s'" % (instance.node_id))
                #cur.execute("SELECT count(*) FROM Non_Active_VMs WHERE resource_name = '%s'" % resource_name)
                cur.execute("SELECT count(*) FROM Non_Active_VMs WHERE vm_id = '%s'" % instance.node_id)
                data=cur.fetchone()[0]
                if data:
                    #cur.execute("DELETE FROM Non_Active_VMs WHERE resource_name = '%s'" % resource_name)
                    cur.execute("DELETE FROM Non_Active_VMs WHERE vm_id = '%s'" % instance.node_id)
    except Exception as err :
        raise Exception( "Error deleting instance information from the database %s: %s" % (resource_conf_db, str( err )) )

def reset_vm_expenditure(resource_name):
    logger.info("Reseting to zero the 'past_expenditure' for the resource '%s' in the database" % (resource_name))
    try:
        with lock:
            conn = sqlite3.connect(resource_conf_db)
            with conn:
                cur = conn.cursor()
                cur.execute("UPDATE Resources SET past_expenditure = %f WHERE name = '%s'" % (0, resource_name))
    except Exception as err :
        raise Exception( "Error reseting the 'past_expenditure' for the resource '%s' in the database %s: %s" % (resource_name, resource_conf_db, str( err )) )
    
def stop_instance( config, instance, resource_name ):
    """
    Destroys one VM and eliminates it from the pickled file
    """
    try :
        instance.destroy()
        pickle_remove(instance, resource_name, config['cloud_connector'])
        delete_vm_from_db(instance, resource_name)
    except Exception as err :
        logger.error( "Error destroying instance\n    %s" % str( err ) )
        
def manage_instances(args, resource_name, config):
    """
    Either creates as many VMs as indicated by the resource configuration
    or destroys all VMs for a selected resource
    """
    if args == "start" :              
        threads = []
        for number_of_th in range( int(config['node_min_pool_size']) ):
            th = threading.Thread( target = start_instance_no_wait, args = ( config, resource_name ) )
            th.start()
            threads.append( th )
        [ th.join() for th in threads ]
    elif args == "stop" :
        cloud_connector = config['cloud_connector']
        instances = pickle_read(resource_name, cloud_connector)
        if instances:
            threads = []
            for instance in instances :
                th = threading.Thread( target = stop_instance, args = ( config, instance, resource_name ) )
                th.start()
                threads.append( th )
            [ th.join() for th in threads ]
    else :
        raise Exception("An invalid argument has been passed")
        

class Instance(object):

    DEFAULT_VM_USER = 'drm4g_adm'
    DEFAULT_VM_COMMUNICATOR = 'ssh' 
    DEFAULT_LRMS = 'fork'
    DEFAULT_PRIVATE_KEY = '~/.ssh/id_rsa'
    DEFAULT_PUBLIC_KEY = '~/.ssh/id_rsa.pub'
    DEFAULT_MYPROXY_SERVER = 'myproxy1.egee.cesnet.cz'
    DEFAULT_VM_CONFIG = join(DRM4G_DIR, 'etc', 'cloud_config.conf')
    DEFAULT_NODE_SAFE_TIME = 5 # minutes
    DEFAULT_VOLUME = 0
    DEFAULT_MIN_NODE_POOL_SIZE = 0
    DEFAULT_MAX_NODE_POOL_SIZE = 10
    DEFAULT_PRICING = 0.0
    DEFAULT_SOFT_BILLING = 0.0
    DEFAULT_HARD_BILLING = 0.0
    SECURITY_GROUP_NAME = 'drm4g_group'
    TIMEOUT = 600.0 # seconds
    WAIT_PERIOD = 3.0 # seconds
    instance_pricing = 0.0
    start_time = 0.0

    def __init__(self, basic_data=None):
        self.node_id = None
        self.volume_id = None
        self.volume_capacity = None
        self.ext_ip = None

    def create(self):
        raise NotImplementedError( "This function must be implemented" )

    def _create_resource(self):
        raise NotImplementedError( "This function must be implemented" )

    def _create_volume(self):
        raise NotImplementedError( "This function must be implemented" )
    
    def _wait_resource(self):
        raise NotImplementedError( "This function must be implemented" )

    def _wait_storage(self):
        raise NotImplementedError( "This function must be implemented" )

    def _create_link(self):
        raise NotImplementedError( "This function must be implemented" )
    
    def _destroy_link(self):
        #raise NotImplementedError( "This function must be implemented" )
        pass
    
    def destroy(self):
        raise NotImplementedError( "This function must be implemented" )

    def get_ip(self):
        raise NotImplementedError( "This function must be implemented" )

    def get_public_ip(self):
        pass

    def get_private_ip(self):
        pass

    def create_security_group(self):
        pass

    def wait_until_running(self):
        pass
    
    def is_resource_active(self):
        raise NotImplementedError( "This function must be implemented" )

    def _start_time(self):
        self.start_time = time.time()

    def running_time(self):
        if not self.start_time:
            return 0
        else:
            #return (time.time() - self.start_time)/3600
            return (time.time() - self.start_time)/360.0

    def current_balance(self):
        running_hours = ceil(self.running_time())
        return running_hours * self.instance_pricing

    def generate_cloud_config(self, public_key, user=None, user_cloud_config=None):
        """
        Generate the cloud-config file to be used in the user_data
        """
        if not user:
            user = self.DEFAULT_VM_USER
        with open( self.cloud_contextualisation_file, "r" ) as contex_file :
            cloud_config = contex_file.read()
        if self.cloud_contextualisation_file == self.DEFAULT_VM_CONFIG:
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

class CloudSetup(object):

    def __init__(self, name, features = {}):
        self.name             = name
        self.vo               = features.get( "vo" )
        #self.url              = features.get( "url" )
        self.cloud_providers  = features.get( "cloud_providers" )
