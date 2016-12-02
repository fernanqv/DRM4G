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
from utils                  import load_json
from os.path                import exists, join
from drm4g                  import DRM4G_DIR, DRM4G_LOGGER, RESOURCE_MANAGERS
from drm4g.utils.importlib  import import_module
try:
    from configparser       import SafeConfigParser
except ImportError:
    from ConfigParser       import SafeConfigParser  # ver. < 3.0

logger = logging.getLogger(__name__)

__version__  = '2.6.0'
__author__   = 'Carlos Blanco and Antonio Minondo'
__revision__ = "$Id$"

pickled_file = join(DRM4G_DIR, "var", "fedcloud_pickled")

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

def start_instance( instance, resource_name ) :
    with lock:
        try:
            instance.create()
            instance.get_ip()
            with open( pickled_file+"_"+resource_name, "a" ) as pf :
                pickle.dump( instance, pf )
        except Exception as err :
            logger.error( "Error creating instance: %s" % str( err ) )
            try :
                logger.debug( "Trying to destroy the instance" )
                instance.delete( )
            except Exception as err :
                logger.error( "Error destroying instance\n%s" % str( err ) )

def stop_instance( instance, resource_name ):
    try :
        instance.delete()
        pickle_remove(instance, resource_name)
    except Exception as err :
        logger.error( "Error destroying instance\n%s" % str( err ) )

def manage_instances(args, resource_name, config):
    if args == "start" :
        try :
            hdpackage = import_module( RESOURCE_MANAGERS[config['lrms']] + ".%s" % config['lrms'] )
        except Exception as err :
            raise Exception( "The infrastructure selected does not exist. "  + str( err ) )
        threads = []
        handlers = []
        try:
            instance = eval( "hdpackage.Instance( config )" )
        except KeyError as err:
            logger.error( "You have defined an incorrect value in your configuration file 'resources.conf':" )
            raise
        except Exception as err:
            logger.error( "An error ocurred while trying to create a VM instance." )
            raise
        for number_of_th in range( int(config['nodes']) ):
            th = threading.Thread( target = start_instance, args = ( instance, resource_name, ) )
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
                th = threading.Thread( target = stop_instance, args = ( instance, resource_name ) )
                th.start()
                threads.append( th )
            [ th.join() for th in threads ]
    else :
        logger.error( "Invalid option" )
        exit( 1 )

class Resource (drm4g.managers.Resource):
    pass

class Job (drm4g.managers.fork.Job):
    pass

#NOT USING THIS CLASS
class ClusterSetup(object):

    def __init__(self, infrastructure, cloud, app, flavour, nodes = 1, volume = None, credentials = {} ):
        self.infrastructure = infrastructure
        self.cloud          = cloud
        self.app            = app
        self.flavour        = flavour
        self.nodes          = ( 1 if int( nodes ) < 1 else int( nodes ) ) + 1
        self.volume         = None if ( volume and int( volume ) <= 0 ) else volume
        self.credentials    = credentials

class CloudSetup(object):

    def __init__(self, name, features = {}):
        self.name     = name
        self.vo       = features.get( "vo" )
        self.url      = features.get( "url" )
        self.clouds   = features.get( "clouds" )

#NOT USING THIS CLASS
class ClusterBasicData(object):

    def __init__(self, pickle_file, cluster_file, setup_file ):
        self.pickle_file = pickle_file
        try :
            cluster_cfg = SafeConfigParser()
            cluster_cfg.read( cluster_file )
            cluster_dict = dict( cluster_cfg.items( 'cluster' ) )
            self.cluster_setup = ClusterSetup( cluster_dict[ 'infrastructure' ],
                                               cluster_dict[ 'cloud' ],
                                               cluster_dict[ 'app' ],
                                               cluster_dict[ 'flavour' ],
                                               cluster_dict[ 'nodes' ],
                                               cluster_dict.get( 'volume' ),
                                               dict( cluster_cfg.items( 'credentials' ) ) )
        except KeyError as err :
            logging.error( "Please review your hadoop cluster configuration: " + str( err ) )
        except Exception as err :
            logging.error( "Error reading hadoop cluster configuration: " + str( err ) )

        try :
            self.cloud_setup = {}
            for name, features in load_json( setup_file ).items() :
                self.cloud_setup[ name ] =  CloudSetup(name, features)
        except Exception as err :
            logging.error( "Error reading hadoop cloud setup: " + str( err ) )
