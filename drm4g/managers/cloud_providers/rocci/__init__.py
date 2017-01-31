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
import threading
import logging
import drm4g.managers
import drm4g.managers.fork
from drm4g.core.configure   import Configuration
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

lock = threading.RLock()

def pickle_remove(inst, resource_name):
    with lock:
        try:
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

            with open( pickled_file+"_"+resource_name, "w" ) as pf :
                for instance in instances:
                    if instance.ext_ip != inst.ext_ip :
                        pickle.dump( instance, pf )

            if len(instances) == 1 :
                os.remove( pickled_file+"_"+resource_name )
        except Exception as err:
            logger.error( "Error deleting instance from pickled file %s\n%s" % (pickled_file+"_"+resource_name, str( err )) )

def pickle_dump(instance, resource_name):
    #with lock:
    lock.acquire()
    try:
        with open(pickled_file + "_" + resource_name, "a") as pf:
            pickle.dump(instance, pf)
    except Exception as err:
        logger.error( "Error adding instance into pickled file %s\n%s" % (pickled_file+"_"+resource_name, str( err )) )
    finally:
        lock.release()

def start_instance( config, resource_name ) :
    #with lock:
    try:            
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
            logger.error( "An error ocurred while trying to create a VM instance." )
            raise
        
        instance.create()
        instance.get_ip()
        pickle_dump(instance, resource_name)
        #Configuration.vm_instances[ resource_name ] += 1 #this could cause problems while using threads
    except Exception as err :
        logger.error( "Error creating instance: %s" % str( err ) )
        try :
            logger.debug( "Trying to destroy the instance" )
            instance.destroy( )
        except Exception as err :
            logger.error( "Error destroying instance\n%s" % str( err ) )

def stop_instance( instance, resource_name ):
    try :
        instance.destroy()
        pickle_remove(instance, resource_name)
        Configuration.vm_instances[ resource_name ] = 0
    except Exception as err :
        logger.error( "Error destroying instance\n%s" % str( err ) )

def manage_instances(args, resource_name, config):
    if args == "start" :
        threads = []
        #handlers = []
        for number_of_th in range( int(config['instances']) ):
            th = threading.Thread( target = start_instance, args = ( config, resource_name, ) )
            th.start()
            threads.append( th )
        [ th.join() for th in threads ]
    elif args == "stop" :
        instances = []
        if not exists( pickled_file+"_"+resource_name ):
            logger.error( "There are no available VMs to be deleted for the resource %s" % resource_name )
        else:
            with open( pickled_file+"_"+resource_name, "r" ) as pf :
                while True :
                    try:
                        instances.append( pickle.load( pf ) )
                    except EOFError :
                        break
            if not instances :
                logger.error( "There are no VMs defined in '%s' or the file is not well formed." % (pickled_file+"_"+resource_name) )
                exit( 1 )
            threads = []
            for instance in instances :
                #Configuration.vm_instances[ resource_name ] += 1
                th = threading.Thread( target = stop_instance, args = ( instance, resource_name ) )
                th.start()
                threads.append( th )
            [ th.join() for th in threads ]
    else :
        logger.error( "Invalid option" )
        exit( 1 )

def create_num_instances(num_instances, resource_name, config):
    try :
        hdpackage = import_module( RESOURCE_MANAGERS[config['lrms']] + ".%s" % config['lrms'] )
    except Exception as err :
        raise Exception( "The infrastructure selected does not exist. "  + str( err ) )
    threads = []
    try:
        instance = eval( "hdpackage.ROCCI( config )" )
    except KeyError as err:
        logger.error( "Either you have defined an incorrect value in your configuration file 'resources.conf'" \
            " or there's a value that doesn't correspond with any of the keys in your cloud setup file 'cloudsetup.json':" )
        raise
    except Exception as err:
        logger.error( "An error occurred while trying to create a VM instance." )
        raise
    for number_of_th in range( num_instances ):
        th = threading.Thread( target = start_instance, args = ( instance, resource_name, ) )
        th.start()
        threads.append( th )
    [ th.join() for th in threads ]

class Resource (drm4g.managers.Resource):
    pass

class Job (drm4g.managers.fork.Job):
    pass

class CloudSetup(object):

    def __init__(self, name, features = {}):
        self.name             = name
        self.vo               = features.get( "vo" )
        #self.url              = features.get( "url" )
        self.cloud_providers  = features.get( "cloud_providers" )
