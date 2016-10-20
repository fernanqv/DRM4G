import logging
import cloud_cli
from fedcloud.utils import load_json
try:
    from configparser import SafeConfigParser
except ImportError:
    from ConfigParser import SafeConfigParser  # ver. < 3.0

logging.basicConfig( level = logging.DEBUG )

__version__  = '0.1.0'
__author__   = 'Carlos Blanco'
__revision__ = "$Id$"

def main(args):
    cloud_cli.main(args)    

#the rest of manager have this
#I think it's needed because of this line "resource_object = getattr( manager , 'Resource' ) ()" in make_resources of configure.py
class Resource (drm4g.managers.Resource):
    pass

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
