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
from drm4g.managers.cloud_providers         import Instance
from drm4g.utils.importlib                  import import_module
from drm4g                                  import ( DRM4G_CONFIG_FILE,
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
pickled_file = os.path.join(DRM4G_DIR, "var", "%s_pickled.pkl")
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
                                   'instances', 'volume', 'region', 'size', 'image', 'volume_type',
                                   'node_max_pool_size', 'node_min_pool_size', 'access_id', 'secret_key',
                                   'pricing', 'cloud_user', 'cloud_connector', 'cloud_config_script', 'is_vm',
                                   'soft_billing', 'hard_billing', 'node_safe_time', 'vm_instances', 'grid_cert']
        assert_message = "The resource configuration file 'resources.conf' does not exist, please provide one\n" \
                         "    If you wish to restore your entire configuration folder you can run the command \033[93m'drm4g start --clear-conf'\033[0m, " \
                         "but bear in mind that this will overwrite or delete every configuration file in '%s'" % os.path.join(DRM4G_DIR, 'etc')
        assert os.path.exists( DRM4G_CONFIG_FILE ), assert_message
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
                        if os.path.exists( pickled_file % (cloud_connector + "_" + name) ):
                            try:
                                instances = []
                                with self.lock:
                                    with open( pickled_file % (cloud_connector + "_" + name), "r" ) as pf :
                                        while True :
                                            try:
                                                instances.append( pickle.load( pf ) )
                                            except EOFError :
                                                break
                                if instances:
                                    for instance in instances :
                                        if instance.ext_ip:
                                            insdict = dict()
                                            insdict['username'] = instance.vm_user
                                            insdict['frontend'] = instance.ext_ip
                                            insdict['communicator'] = instance.vm_comm
                                            insdict['private_key'] = instance.private_key
                                            insdict['enable'] = 'true'
                                            insdict['lrms'] = instance.lrms 
                                            insdict['max_jobs_running'] = instance.max_jobs_running
                                            insdict['is_vm'] = 'true'
                                            self.resources[ name+"_"+instance.ext_ip ] = insdict
                                            logger.debug("Resource '%s' defined by: %s.",
                                                    name+"_"+instance.ext_ip, ', '.join([("%s=%s" % (k,v)) for k,v in sorted(self.resources[name+"_"+instance.ext_ip].items())]))
                            except Exception as err :
                                raise Exception( "Could not add %s VM's information to the resource list:\n%s" % (name, str(err)) )
                        #if no database exists 
                        with self.lock:
                            if not os.path.exists( resource_conf_db ):
                                if os.path.exists( os.path.join(DRM4G_DIR, "var") ):
                                    self.resources[ name ][ 'vm_instances' ] = 0
                                    conn = sqlite3.connect(resource_conf_db)
                                    with conn:
                                        cur = conn.cursor()
                                        cur.execute("CREATE TABLE Resources (id integer primary key autoincrement, name text not null, vms integer, past_expenditure real)")
                                        cur.execute("INSERT INTO Resources (name, vms, past_expenditure) VALUES ('%s', %d, %f)" % (name, 0, 0))
                                        
                                        cur.execute("CREATE TABLE VM_Pricing (resource_id int, id text primary key, name text, state text, pricing real, start_time real, active_time real, foreign key(resource_id) references Resources(id))")
                                        cur.execute("CREATE TABLE Non_Active_VMs (id integer primary key autoincrement, vm_id text, resource_name text, cloud_connector text, foreign key(vm_id) references VM_Pricing(id), foreign key(resource_name) references Resources(name))")
                            else:
                                conn = sqlite3.connect(resource_conf_db)
                                with conn:
                                    cur = conn.cursor()
                                    cur.execute("SELECT count(*) FROM Resources WHERE name = '%s'" % name)
                                    data=cur.fetchone()[0]
                                    #if database exists but it's the first time a resource is found
                                    if data==0:
                                        self.resources[ name ][ 'vm_instances' ] = 0
                                        cur.execute("INSERT INTO Resources (name, vms, past_expenditure) VALUES ('%s', %d, %f)" % (name, 0, 0))
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
                    output = "    '%s' resource has an invalid key : '%s'" % (resname, key)
                    logger.error( output )
                    errors.append( output )
            for key in [ 'enable' , 'frontend' , 'lrms' , 'communicator' ] :
                if not key in reslist :
                    output = "    '%s' resource does not have '%s' key" % (resname, key)
                    logger.error( output )
                    errors.append( output )
            if 'cloud_connector' in reslist:
                if not resdict.get( 'cloud_connector' ) in CLOUD_CONNECTORS:
                    output = "    'cloud_connector' has an incorrect value for resource '%s'" % resname
                    logger.error( output )
                    errors.append( output )
                if not resdict.get( 'private_key' ) :
                    self.resources[resname]['private_key'] = '~/.ssh/id_rsa'
                    resdict[ 'private_key' ] = '~/.ssh/id_rsa'
                    output = "    'private_key' key will have a value of '~/.ssh/id_rsa' for the resource '%s'" % resname
                    logger.debug( output )
                if not resdict.get( 'public_key' ) :
                    self.resources[resname]['public_key'] = '~/.ssh/id_rsa.pub'
                    resdict[ 'public_key' ] = '~/.ssh/id_rsa.pub'
                    output = "    'public_key' key will have a value of '~/.ssh/id_rsa.pub' for the resource '%s'" % resname
                    logger.debug( output )
                if resdict[ 'cloud_connector' ] == 'ec2':
                    for key in [ 'access_id' , 'secret_key' ] :
                        if not key in reslist :
                            output = "    '%s' resource needs the '%s' key" % (resname, key)
                            logger.error( output )
                            errors.append( output )
                if resdict[ 'cloud_connector' ] == 'rocci':
                    if not 'myproxy_server' in reslist:
                        self.resources[resname]['myproxy_server'] = Instance.DEFAULT_MYPROXY_SERVER
                        resdict[ 'myproxy_server' ] = Instance.DEFAULT_MYPROXY_SERVER
                        output = "    'myproxy_server' key will have a value of %s for the resource '%s'" % (Instance.DEFAULT_MYPROXY_SERVER, resname)
                        logger.debug( output )
                for key in [ 'pricing' , 'soft_billing', 'hard_billing', 'node_min_pool_size', 'node_max_pool_size' ] :
                    if key in reslist:
                        if float( resdict[ key ] ) < 0:
                            output = "    '%s' can't be smaller than 0, error found in '%s' resource" % ( key, resname )
                            logger.error( output )
                            errors.append( output )
                if not resdict.get( 'pricing' ):
                    self.resources[resname]['pricing'] = str(Instance.instance_pricing)
                    resdict[ 'pricing' ] = str(Instance.instance_pricing)
                    output = "    'pricing' key will have a value of %s for the resource '%s'" % (Instance.instance_pricing, resname)
                    logger.debug( output )
                if 'soft_billing' in reslist and 'hard_billing' in reslist:
                    if float(resdict[ 'soft_billing' ]) > float(resdict[ 'hard_billing' ]) :
                        output = "    'soft_billing' can't be larger than 'hard_billing', error found in '%s' resource" % resname
                        logger.error( output )
                        errors.append( output )
                else:
                    if not 'soft_billing' in reslist:
                        self.resources[resname]['soft_billing'] = str(Instance.DEFAULT_SOFT_BILLING)
                        resdict['soft_billing'] = str(Instance.DEFAULT_SOFT_BILLING)
                        output = "    'soft_billing' key will have a value of '%s' for the resource '%s'" % (Instance.DEFAULT_SOFT_BILLING, resname)
                        logger.debug( output )
                        if not 'hard_billing' in reslist:
                            self.resources[resname]['hard_billing'] = str(Instance.DEFAULT_HARD_BILLING)
                            resdict['hard_billing'] = str(Instance.DEFAULT_HARD_BILLING)
                            output = "    'hard_billing' key will have a value of '%s' for the resource '%s'" % (Instance.DEFAULT_HARD_BILLING, resname)
                            logger.debug( output )
                    else:
                        if not 'hard_billing' in reslist:
                            self.resources[resname]['hard_billing'] = self.resources[resname]['soft_billing']
                            resdict['hard_billing'] = self.resources[resname]['soft_billing']
                            output = "    'hard_billing' key will have the same value as 'soft_billing' for the resource '%s'" % resname
                            logger.debug( output )
                if resdict.get( 'vm_communicator' ) == 'local':
                    output = "    'vm_communicator' key cannot have the value of 'local', error found in '%s' resource" % resname
                    logger.error( output )
                    errors.append( output )
                if not 'node_min_pool_size' in reslist:
                    if 'node_max_pool_size' in reslist:
                        self.resources[resname]['node_min_pool_size'] = '0'
                        resdict['node_min_pool_size'] = '0'
                        output = "    'node_min_pool_size' key will have a value of '0' for the resource '%s'" % resname
                    else:
                        self.resources[resname]['node_min_pool_size'] = '0'
                        self.resources[resname]['node_max_pool_size'] = '10'
                        resdict['node_min_pool_size'] = '0'
                        resdict['node_max_pool_size'] = '10'
                        output = "    'node_min_pool_size' key will have a value of '0' and 'node_max_pool_size'" \
                                 " a value of '10' for the resource '%s'" % resname
                    logger.debug( output )
                else:
                    if not 'node_max_pool_size' in reslist:
                        self.resources[resname]['node_max_pool_size'] = self.resources[resname]['node_min_pool_size']
                        resdict['node_max_pool_size'] = self.resources[resname]['node_min_pool_size']
                        output = "    'node_max_pool_size' key will have the same value as the 'node_min_pool_size' key for the resource '%s'" % resname
                        logger.debug( output )
                if 'node_min_pool_size' in reslist and 'node_max_pool_size' in reslist:
                    if int(resdict[ 'node_min_pool_size' ]) > int(resdict[ 'node_max_pool_size' ]):
                        output = "    'node_min_pool_size' key cannot have a value larger than 'node_max_pool_size', error found in '%s' resource" % resname
                        logger.error( output )
                        errors.append( output )
                if float(resdict[ 'pricing' ]) >= float(resdict[ 'hard_billing' ]) and not float(resdict[ 'pricing' ]) == 0 :
                    output = "    'pricing' key cannot have a value larger than 'hard_billing' since no VMs will be created for the resource '%s'" % resname
                    logger.error( output )
                    errors.append( output )
                if (float(resdict[ 'pricing' ]) * int(resdict[ 'node_min_pool_size' ]) >= float(resdict[ 'hard_billing' ])) and not float(resdict[ 'pricing' ]) == 0 :
                    output = "    With the current values of the 'pricing' and 'node_min_pool_size' keys, the number of VMs created will not reach" \
                             " 'node_min_pool_size' since 'pricing' * 'node_min_pool_size' >= 'hard_billing' for the resource '%s'" % resname
                    logger.info( output )
                if not 'node_safe_time' in reslist:
                    self.resources[resname]['node_safe_time'] = str(Instance.DEFAULT_NODE_SAFE_TIME)
                    output = "    'node_safe_time' key will have a value of '%s' for the resource '%s'" % (Instance.DEFAULT_NODE_SAFE_TIME, resname)
                    logger.debug( output )
                else:
                    if int( resdict[ 'node_safe_time' ] ) < 0:
                        output = "    'node_safe_time' can't be smaller than 0, error found in '%s' resource" % ( key, resname )
                        logger.error( output )
                        errors.append( output )
                if 'volume' in reslist:
                    if int(resdict[ 'volume' ]) < 0:
                        output = "    'volume' can't be smaller than 0, error found in '%s' resource" % ( key, resname )
                        logger.error( output )
                        errors.append( output )
            if ( resdict[ 'lrms' ] != 'cream' and not resdict.get( 'max_jobs_running' ) ) :
                '''
                output = "    'max_jobs_running' key is mandatory for the resource '%s'" % resname
                logger.error( output )
                errors.append( output )
                '''
                self.resources[resname]['max_jobs_running'] = '1'
                resdict['max_jobs_running'] = '1'
                output = "    'max_jobs_running' key will have a value of 1 for the resource '%s'" % (resname)
                logger.debug( output )
            if ( not ( 'max_jobs_in_queue' in reslist ) and ( 'max_jobs_running' in reslist ) and ( resdict[ 'lrms' ] != 'cream' ) ) :
                self.resources[resname]['max_jobs_in_queue'] = resdict['max_jobs_running']
                resdict['max_jobs_in_queue'] = resdict['max_jobs_running']
                logger.debug( "    'max_jobs_in_queue' will be the same as the 'max_jobs_running'" )
            if ( not 'queue' in reslist ) and ( resdict[ 'lrms' ] != 'cream' ) :
                self.resources[resname]['queue'] = "default"
                resdict['queue'] = "default"
                output = "    'queue' key will be called 'default' for the resource '%s'" % resname
                logger.debug( output )
            if 'max_jobs_running' in reslist and resdict[ 'lrms' ] != 'cream' and resdict.get( 'max_jobs_in_queue' ).count( ',' ) !=  resdict.get( 'queue' ).count( ',' ) :
                output = "    The number of elements in 'max_jobs_in_queue' are different to the elements of 'queue'"
                logger.error( output )
                errors.append( output )
            if 'max_jobs_running' in reslist and resdict[ 'lrms' ] != 'cream' and resdict.get( 'max_jobs_running' ).count( ',' ) !=  resdict.get( 'queue' ).count( ',' ) :
                output = "    The number of elements in 'max_jobs_running' are different to the elements of 'queue'"
                logger.error( output )
                errors.append( output )
            if resdict[ 'lrms' ] != 'cream' and ( 'host_filter' in reslist ) :
                output = "    'host_filter' key is only available for 'cream' lrms"
                logger.error( output )
                errors.append( output )
            if resdict[ 'communicator' ] not in COMMUNICATORS :
                output = "    '%s' has a wrong communicator: '%s'" % (resname , resdict[ 'communicator' ] )
                logger.error( output )
                errors.append( output )
            if 'vm_communicator' in reslist:
                if resdict[ 'vm_communicator' ] not in COMMUNICATORS :
                    output = "    '%s' has a wrong vm_communicator: '%s'" % (resname , resdict[ 'vm_communicator' ] )
                    logger.error( output )
                    errors.append( output )
            if resdict[ 'communicator' ] != 'local' and 'username' not in resdict :
                output = "    'username' key is mandatory for '%s' communicator, '%s' resource" % (resdict[ 'communicator' ], resname)
                logger.error( output )
                errors.append( output )
            if resdict[ 'lrms' ] not in RESOURCE_MANAGERS :
                output = "    '%s' has a wrong lrms: '%s'" % ( resname , resdict[ 'lrms' ] )
                logger.error( output )
                errors.append( output )
            if resdict[ 'communicator' ] != 'local' :
                private_key = resdict.get( 'private_key' )
                if not private_key :
                    output = "    'private_key' key is mandatory for the resource '%s'" % resname
                    logger.error( output )
                    errors.append( output )
                else :
                    abs_private_key = os.path.expandvars( os.path.expanduser( private_key ) )
                    if not os.path.isfile( abs_private_key ) :
                        output = "    '%s' does not exist for the resource '%s'" % ( private_key , resname )
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
                        output = "    '%s' does not exist for the resource '%s'" % ( abs_public_key , resname )
                        logger.error( output )
                        errors.append( output )
                    else :
                        self.resources[resname]['public_key'] = abs_public_key
            grid_cert = resdict.get( 'grid_cert' )
            if grid_cert :
                abs_grid_cert = os.path.expandvars( os.path.expanduser( grid_cert ) )
                if not os.path.isfile( abs_grid_cert ) :
                    output = "    '%s' does not exist for the resource '%s'" % ( abs_grid_cert , resname )
                    logger.error( output )
                    errors.append( output )
                else :
                    self.resources[resname]['grid_cert'] = abs_grid_cert
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


