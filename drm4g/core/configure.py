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
import pickle
import sqlite3
import logging
import threading
import drm4g.managers.cloud_providers
from drm4g.utils.importlib import import_module
from drm4g                 import ( DRM4G_CONFIG_FILE,
                                    COMMUNICATORS,
                                    RESOURCE_MANAGERS,
                                    CLOUD_CONNECTORS,
                                    REMOTE_JOBS_DIR,
                                    DRM4G_DIR )
try :
    import configparser
except ImportError :
    import ConfigParser as configparser


logger = logging.getLogger(__name__)
pickled_file = os.path.join(DRM4G_DIR, "var", "%s_pickled")
resource_conf_db = os.path.join(DRM4G_DIR, "var", "resource_conf.db")

class ConfigureException(Exception):
    pass

class Configuration(object):
    """
    Configuration class provides facilities to:

    * parse DRM4G_CONFIG_FILE resources
    * check key resources
    * instantiate objects such as communicators or managers

    """
    lock = threading.Lock()
    
    def __init__(self):
        self.resources  = dict()
        self.configuration_keys = ['enable', 'communicator', 'username', 'frontend', 'private_key',
                                   'public_key', 'scratch', 'lrms', 'queue', 'max_jobs_in_queue',
                                   'max_jobs_running', 'parallel_env', 'project', 'vo', 'host_filter',
                                   'bdii', 'myproxy_server', 'vm_user', 'vm_communicator', 'vm_config',
                                   'cloud_provider', 'flavour', 'virtual_image', 'instances', 'volume',
                                   'max_nodes', 'min_nodes', 'access_id', 'secret_key', 'region', 'size',
                                   'image', 'pricing', 'cloud_user', 'cloud_connector', 'cloud_config_script',
                                   'soft_billing', 'hard_billing', 'node_safe_time', 'vm_instances']
        if not os.path.exists( DRM4G_CONFIG_FILE ):
            assert DRM4G_CONFIG_FILE, "resources.conf does not exist, please provide one"
        self.init_time = os.stat( DRM4G_CONFIG_FILE ).st_mtime

    def check_update(self):
        """
        It checks if DRM4G file configuration has been updated.
        """
        if os.stat(DRM4G_CONFIG_FILE).st_mtime != self.init_time:
            self.init_time = os.stat(DRM4G_CONFIG_FILE).st_mtime
            return True
        else:
            return False

    def load(self):
        """
        Read the configuration file.
        """
        logger.debug("Reading file '%s' ..." % DRM4G_CONFIG_FILE)
        try:
            try:
                conf_file   = open(DRM4G_CONFIG_FILE, 'r')
                parser = configparser.RawConfigParser()
                try:
                    parser.readfp( conf_file , DRM4G_CONFIG_FILE )
                except Exception as err:
                    output = "Configuration file '%s' is unreadable or malformed: %s" % ( DRM4G_CONFIG_FILE , str( err ) )
                    logger.error( output )

                for sectname in parser.sections():
                    name                   = sectname
                    logger.debug(" Reading configuration for resource '%s'." % name )
                    self.resources[ name ] = dict( parser.items( sectname ) )

                    if 'cloud_connector' in self.resources[ name ].keys():
                        cloud_connector = self.resources[ name ]['cloud_connector']
                        if os.path.exists( pickled_file % cloud_connector + "_" + name ):
                            try:
                                instances = []
                                with self.lock:
                                    with open( pickled_file % cloud_connector + "_" + name, "r" ) as pf :
                                        while True :
                                            try:
                                                instances.append( pickle.load( pf ) )
                                            except EOFError :
                                                break
                                if instances:
                                    for instance in instances :
                                        insdict = dict()
                                        insdict['username'] = instance.vm_user
                                        insdict['frontend'] = instance.ext_ip
                                        insdict['communicator'] = instance.vm_comm
                                        insdict['private_key'] = instance.private_key
                                        insdict['enable'] = 'true'
                                        insdict['lrms'] = instance.lrms 
                                        insdict['max_jobs_running'] = instance.max_jobs_running
                                        self.resources[ name+"_"+instance.ext_ip ] = insdict
                                        logger.debug("Resource '%s' defined by: %s.",
                                                name+"_"+instance.ext_ip, ', '.join([("%s=%s" % (k,v)) for k,v in sorted(self.resources[name+"_"+instance.ext_ip].items())]))
                                #self.resources[ name ][ 'vm_instances' ] = len(instances)
                            except Exception as err :
                                raise Exception( "Could not add %s VM's information to the resource list:\n%s" % (name,str(err)) )
                        #else:
                            #self.resources[ name ][ 'vm_instances' ] = 0
                        
                        #if no database exists 
                        with self.lock:
                            if not os.path.exists( resource_conf_db ):
                                if os.path.exists( os.path.join(DRM4G_DIR, "var") ):
                                    self.resources[ name ][ 'vm_instances' ] = 0
                                    #with self.lock:
                                    conn = sqlite3.connect(resource_conf_db)
                                    with conn:
                                        cur = conn.cursor()
                                        cur.execute("CREATE TABLE Resources (name text not null, vms integer, id integer primary key autoincrement)")
                                        cur.execute("INSERT INTO Resources (name, vms) VALUES ('%s', %d)" % (name, 0))
                                        
                                        cur.execute("CREATE TABLE VM_Pricing (name text primary key, resource_id int, state text, pricing real, start_time real, foreign key(resource_id) references Resources(id))")
                            else:
                                conn = sqlite3.connect(resource_conf_db)
                                with conn:
                                    cur = conn.cursor()
                                    cur.execute("SELECT count(*) FROM Resources WHERE name = '%s'" % name)
                                    data=cur.fetchone()[0]
                                    #if database exists but it's the first time a resource is found
                                    if data==0:
                                        #with self.lock:
                                        self.resources[ name ][ 'vm_instances' ] = 0
                                        cur.execute("INSERT INTO Resources (name, vms) VALUES ('%s', %d)" % (name, 0))
                                    else:
                                        cur.execute("SELECT vms FROM Resources WHERE name='%s'" % (name))
                                        vms = cur.fetchone()[0]
                                        self.resources[ name ][ 'vm_instances' ] = vms

                    logger.debug("Resource '%s' defined by: %s.",
                             sectname, ', '.join([("%s=%s" % (k,v)) for k,v in sorted(self.resources[name].items())]))
            except Exception as err:
                output = "Error while reading '%s' file: %s" % (DRM4G_CONFIG_FILE, str(err))
                logger.error( output )
                
        finally:
            conf_file.close()

    def check(self):
        """
        Check if the drm4g.conf file has been configured well.

        Return a list with the errors.
        """
        errors = []
        for resname, resdict in list(self.resources.items()) :
            logger.debug("Checking resource '%s' ..." % resname)

            reslist = list(resdict.keys( ))
            for key in reslist:
                if key not in self.configuration_keys :
                    output = "'%s' resource has an invalid key : '%s'" % (resname, key)
                    logger.error( output )
                    errors.append( output )
            for key in [ 'enable' , 'frontend' , 'lrms' , 'communicator' ] :
                if not key in reslist :
                    output = "'%s' resource does not have '%s' key" % (resname, key)
                    logger.error( output )
                    errors.append( output )
            if ( resdict.get( 'cloud_connector' ) in CLOUD_CONNECTORS and not resdict.get( 'private_key' ) ) :
                output = "'private_key' key has not been defined for '%s' resource" % resname
                logger.error( output )
                errors.append( output )
            if 'pricing' in reslist :
                #if ( not 'soft_billing' in reslist ) or ( not 'hard_billing' in reslist ):
                #    output = "'soft_billing' and 'hard_billing' keys are mandatory for '%s' resource" % resname
                #    logger.error( output )
                #    errors.append( output )
                if resdict[ 'soft_billing' ] > resdict[ 'hard_billing' ] :
                    output = "'soft_billing' can't be larger than 'hard_billing', problem found in '%s' resource" % resname
                    logger.error( output )
                    errors.append( output )
                if resdict[ 'soft_billing' ] < 0 or resdict[ 'hard_billing' ] < 0 :
                    output = "'soft_billing' and 'hard_billing' can't be smaller than 0, problem found in '%s' resource" % resname
                    logger.error( output )
                    errors.append( output )
            if ( not 'max_jobs_running' in reslist ) and ( resdict[ 'lrms' ] != 'cream' ) :
                output = "'max_jobs_running' key is mandatory for '%s' resource" % resname
                logger.error( output )
                errors.append( output )
            if ( resdict[ 'lrms' ] != 'cream' and not resdict.get( 'max_jobs_running' ) ) :
                output = "'max_jobs_running' key has a wrong value for '%s' resource" % resname
                logger.error( output )
                errors.append( output )
            if ( not ( 'max_jobs_in_queue' in reslist ) and ( 'max_jobs_running' in reslist ) and ( resdict[ 'lrms' ] != 'cream' ) ) :
                self.resources[resname]['max_jobs_in_queue'] = resdict['max_jobs_running']
                logger.debug( "'max_jobs_in_queue' will be the same as the 'max_jobs_running'" )
            if ( not 'queue' in reslist ) and ( resdict[ 'lrms' ] != 'cream' ) :
                self.resources[resname]['queue'] = "default"
                output = "'queue' key will be called 'default' for '%s' resource" % resname
                logger.debug( output )
            if 'max_jobs_running' in reslist and resdict[ 'lrms' ] != 'cream' and resdict.get( 'max_jobs_in_queue' ).count( ',' ) !=  resdict.get( 'queue' ).count( ',' ) :
                output = "The number of elements in 'max_jobs_in_queue' are different to the elements of 'queue'"
                logger.error( output )
                errors.append( output )
            if 'max_jobs_running' in reslist and resdict[ 'lrms' ] != 'cream' and resdict.get( 'max_jobs_running' ).count( ',' ) !=  resdict.get( 'queue' ).count( ',' ) :
                output = "The number of elements in 'max_jobs_running' are different to the elements of 'queue'"
                logger.error( output )
                errors.append( output )
            if resdict[ 'lrms' ] != 'cream' and ( 'host_filter' in reslist ) :
                output = "'host_filter' key is only available for 'cream' lrms"
                logger.error( output )
                errors.append( output )
            if resdict[ 'communicator' ] not in COMMUNICATORS :
                output = "'%s' has a wrong communicator: '%s'" % (resname , resdict[ 'communicator' ] )
                logger.error( output )
                errors.append( output )
            if resdict[ 'communicator' ] != 'local' and 'username' not in resdict :
                output = "'username' key is mandatory for '%s' communicator, '%s' resource" % (resdict[ 'communicator' ], resname)
                logger.error( output )
                errors.append( output )
            if resdict[ 'lrms' ] not in RESOURCE_MANAGERS :
                output = "'%s' has a wrong lrms: '%s'" % ( resname , resdict[ 'lrms' ] )
                logger.error( output )
                errors.append( output )
            if resdict[ 'communicator' ] != 'local' :
                private_key = resdict.get( 'private_key' )
                if not private_key :
                    output = "'private_key' key is mandatory for '%s' resource" % resname
                    logger.error( output )
                    errors.append( output )
                else :
                    abs_private_key = os.path.expandvars( os.path.expanduser( private_key ) )
                    if not os.path.isfile( abs_private_key ) :
                        output = "'%s' does not exist for '%s' resource" % ( private_key , resname )
                        logger.error( output )
                        errors.append( output )
                    else :
                        self.resources[resname]['private_key'] = abs_private_key
                    public_key = resdict.get( 'public_key' )
                    if not public_key :
                        abs_public_key = abs_private_key + '.pub'
                    else :
                        abs_public_key = os.path.expandvars( os.path.expanduser( public_key ) )
                    if not os.path.isfile( abs_private_key ) :
                        output = "'%s' does not exist for '%s' resource" % ( abs_public_key , resname )
                        logger.error( output )
                        errors.append( output )
                    else :
                        self.resources[resname]['public_key'] = abs_public_key
            grid_cert = resdict.get( 'grid_cert' )
            if grid_cert :
                abs_grid_cert = os.path.expandvars( os.path.expanduser( grid_cert ) )
                if not os.path.isfile( abs_grid_cert ) :
                    output = "'%s' does not exist for '%s' resource" % ( abs_grid_cert , resname )
                    logger.error( output )
                    errors.append( output )
                else :
                    self.resources[resname]['grid_cert'] = abs_grid_cert
        '''
        #This is quite redundant
        #When running "drm4g resource edit", an exception appears if you have an error saying
        #"Please, review your configuration file", that is thrown by drm4g/commands
        if errors:
            output="Modify your configuration file before trying again."
            logger.error( output )
        '''
        return errors

    def make_communicators(self):
        """
        Make communicator objects corresponding to the configured resources.

        Return a dictionary, mapping the resource name into the corresponding objects.
        """
        communicators = dict()
        for name, resdict in list(self.resources.items()):
            try:
                communicator              = import_module(COMMUNICATORS[ resdict[ 'communicator' ] ] )
                com_object                = getattr( communicator , 'Communicator' ) ()
                com_object.username       = resdict.get( 'username' )
                com_object.frontend       = resdict.get( 'frontend' )
                com_object.private_key    = resdict.get( 'private_key' )
                com_object.public_key     = resdict.get( 'public_key' )
                com_object.work_directory = resdict.get( 'scratch', REMOTE_JOBS_DIR )
                communicators[name]       = com_object
                logger.debug("Communicator of resource '%s' is defined by: %s.",
                    name, ', '.join([("%s=%s" % (k,v)) for k,v in sorted(communicators[name].__dict__.items())]))
            except Exception as err:
                output = "Failed creating communicator for resource '%s' : %s" % ( name, str( err ) )
                logger.warning( output , exc_info=1 )
        return communicators

    def make_resources(self):
        """
        Make manager objects corresponding to the configured resources.

        Return a dictionary, mapping the resource name into the corresponding objects.
        """
        resources = dict()
        for name, resdict in list(self.resources.items()):
            try:
                resources[name]             = dict()
                if 'cloud_connector' in self.resources[ name ].keys():
                    #this is probably not needed, since the im_mad has an "if" that skips all resources with a "cloud_connector" key
                    manager                     = drm4g.managers.cloud_providers
                else:
                    manager                     = import_module(RESOURCE_MANAGERS[ resdict[ 'lrms' ] ] )
                resource_object             = getattr( manager , 'Resource' ) ()
                resource_object.name        = name
                resource_object.features    = resdict
                job_object                  = getattr( manager , 'Job' ) ()
                job_object.resfeatures      = resdict
                resources[name]['Resource'] = resource_object
                resources[name]['Job']      = job_object
            except Exception as err:
                output = "Failed creating objects for resource '%s' of type : %s" % ( name, str( err ) )
                logger.warning( output , exc_info=1 )
        return resources


