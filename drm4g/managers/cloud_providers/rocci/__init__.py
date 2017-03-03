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
import pickle
import sqlite3
import threading
import logging
import drm4g.managers
import drm4g.managers.fork
from utils                  import load_json
from os.path                import exists, join
from drm4g                  import DRM4G_DIR, DRM4G_LOGGER, RESOURCE_MANAGERS
from drm4g.utils.importlib  import import_module
try:
    from configparser       import SafeConfigParser
except ImportError:
    from ConfigParser       import SafeConfigParser  # ver. < 3.0
from drm4g.managers.cloud_providers  import logger

#logger = logging.getLogger(__name__)

pickled_file = join(DRM4G_DIR, "var", "rocci_pickled")
resource_conf_db = os.path.join(DRM4G_DIR, "var", "resource_conf.db")

lock = threading.RLock()


def pickle_read(resource_name):
    '''
    Reads the pickled file and returns all of the VMs created for the specified resource
    @param resource_name: name of the resource 
    @return: instances : list of VMs
    '''
    instances = []
    with open(pickled_file + "_" + resource_name, "r") as pf:
        while True:
            try:
                instances.append(pickle.load(pf))
            except EOFError:
                break    
    if not instances:
        logger.error("There are no VMs defined in '%s' or the file is not well formed." % (pickled_file + "_" + resource_name))
        exit(1)
    return instances

def pickle_remove(inst, resource_name):
    '''
    Deletes a VM from the pickled file 
    @param: inst : VMs to be eliminated
    @param resource_name: name of the resource
    '''
    with lock:
        try:
            instances = pickle_read(resource_name)

            with open( pickled_file+"_"+resource_name, "w" ) as pf :
                for instance in instances:
                    if instance.ext_ip != inst.ext_ip :
                        pickle.dump( instance, pf )

            if len(instances) == 1 :
                os.remove( pickled_file+"_"+resource_name )
        except Exception as err:
            logger.error( "Error deleting instance from pickled file %s\n%s" % (pickled_file+"_"+resource_name, str( err )) )

def pickle_dump(instance, resource_name):
    '''
    Adds a VM to the pickled file 
    @param: inst : VMs to be eliminated
    @param resource_name: name of the resource
    '''
    lock.acquire()
    try:
        with open(pickled_file + "_" + resource_name, "a") as pf:
            pickle.dump(instance, pf)
    except Exception as err:
        logger.error( "Error adding instance into pickled file %s\n%s" % (pickled_file+"_"+resource_name, str( err )) )
    finally:
        lock.release()

def start_instance( config, resource_name ) :
    """
    Creates a VM using the configuration indicated by a selected resource
    """
    try :
        hdpackage = import_module( RESOURCE_MANAGERS[config['lrms']] + ".%s" % config['lrms'] )
    except Exception as err :
        raise Exception( "The infrastructure selected does not exist. "  + str( err ) )
    
    try:
        instance = eval( "hdpackage.ROCCI( config )" )
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
        pickle_dump(instance, resource_name)
        
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
            stop_instance(instance, resource_name)
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

def stop_instance( instance, resource_name ):
    """
    Destroys one VM and eliminates it from the pickled file
    """
    try :
        pickle_remove(instance, resource_name)
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
        threads = []
        for number_of_th in range( int(config['instances']) ):
            th = threading.Thread( target = start_instance, args = ( config, resource_name, ) )
            th.start()
            threads.append( th )
        [ th.join() for th in threads ]
    elif args == "stop" :
        if not exists( pickled_file+"_"+resource_name ):
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
            instances = pickle_read(resource_name)
            threads = []
            for instance in instances :
                th = threading.Thread( target = stop_instance, args = ( instance, resource_name ) )
                th.start()
                threads.append( th )
            [ th.join() for th in threads ]
        #if exists(resource_conf_db):
        #    os.remove( resource_conf_db ) 
    else :
        logger.error( "Invalid option" )
        exit( 1 )

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
    
#not being used anymore
def destroy_num_instances(num_instances, resource_name, config):
    '''
    Destroys a specified number of VMs for a selected resource configuration
    '''
    threads = []
    for number_of_th in range( num_instances ):
        th = threading.Thread( target = stop_instance, args = ( config, resource_name, ) ) #stop_instance doens't use "config"
        th.start()
        threads.append( th )
    [ th.join() for th in threads ]
    
def destroy_vm_by_name(resource_name, vm_name):
    '''
    Destroys a specific VM and removes it from the pickled file
    '''
    #with lock: #it's only called by the im from inside a lock, so this one is redundant
    try:
        '''
        instances=[]
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
        instances = pickle_read(resource_name)
        with open( pickled_file+"_"+resource_name, "w" ) as pf :
            for instance in instances:
                if (resource_name+'_'+instance.ext_ip) != vm_name :
                    pickle.dump( instance, pf )
                else:
                    delete_vm_from_db(instance, resource_name)
                    instance.destroy()
        if len(instances) == 1 :
            os.remove( pickled_file+"_"+resource_name )
    except Exception as err:
        logger.error( "Error deleting instance from pickled file %s\n%s" % (pickled_file+"_"+resource_name, str( err )) )


class Resource (drm4g.managers.Resource):
    def hosts(self):
        """
        It will return a string with the host available in the resource.
        """
        if 'cloud_provider' in self.features :
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
