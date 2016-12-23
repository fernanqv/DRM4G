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

import cmd
import os
import sys
import re
import time
import getpass
import logging
import subprocess
import datetime

from drm4g             import REMOTE_VOS_DIR, DRM4G_CONFIG_FILE, DRM4G_DIR
from drm4g.managers    import rocci
from drm4g.core.im_mad import GwImMad
from os.path           import expanduser, join, dirname, exists, basename, expandvars


PY2 = sys.version_info[0] == 2

logging.basicConfig( format='%(message)s', level = logging.INFO , stream = sys.stdout )
logger = logging.getLogger(__name__)

def process_is_runnig( pid ):
    """
    Check is a process is running given a file
    """
    try:
        with open( pid , 'r' ) as f:
            lines = f.readlines()
        os.kill( int( lines[0].strip() ) , 0 )
    except :
        return False
    else:
        return True

def exec_cmd( cmd , stdin=subprocess.PIPE, stdout=subprocess.PIPE,
              stderr=subprocess.STDOUT, env=os.environ ):
    """
    Execute shell commands
    """
    logger.debug( "Executing command ... " + cmd )
    cmd_to_exec = subprocess.Popen(  cmd ,
                                  shell=True ,
                                  stdin=subprocess.PIPE,
                                  stdout=subprocess.PIPE,
                                  stderr=subprocess.PIPE,
                                  env=env
                                  )
    out , err =  cmd_to_exec.communicate()
    return out , err

def yes_no_choice( message ,  default = 'y' ) :
    """
    Ask for Yes/No questions
    """
    choices = 'Y/n' if default.lower() in ( 'y', 'yes' ) else 'y/N'
    if PY2 :
        choice = raw_input( "%s (%s) " % ( message, choices ) )
    else :
        choice = input( "%s (%s) " % ( message, choices ) )
    values = ( 'y', 'yes', '' ) if default == 'y' else ( 'y', 'yes' )
    return choice.strip().lower() in values

class Agent( object ):
    """
    Class to manage ssh-agent command.
    """

    def __init__( self, resource = None ):
        if resource :
            self.private_key  = resource[ 'private_key' ]
            self.public_key   = resource[ 'public_key' ]
            self.user         = resource[ 'username' ]
            self.frontend     = resource[ 'frontend' ]
        self.agent_env    = dict()
        self.agent_file   = join( DRM4G_DIR  , 'var' , 'agent.conf' )

    def start( self ):
        def _start():
            # 's' option generates Bourne shell commands on stdout
            out , err = exec_cmd( 'ssh-agent -s ' )
            logger.debug( out )
            match = re.search( 'SSH_AUTH_SOCK=(?P<SSH_AUTH_SOCK>[^;]+);.*' \
                           + 'SSH_AGENT_PID=(?P<SSH_AGENT_PID>\d+);', out, re.DOTALL)
            if match :
                logger.debug( "  OK" )
                self.agent_env = match.groupdict()
                logger.debug( '  Agent pid: %s'  % self.agent_env['SSH_AGENT_PID'])
            else:
                logger.error( err )
                raise Exception( '  Cannot determine agent data from output: %s' % out )
            with open( self.agent_file , 'w') as f:
                f.write( self.agent_env['SSH_AGENT_PID'] + '\n' + self.agent_env['SSH_AUTH_SOCK'] )
        logger.debug('Starting ssh-agent ...')
        if not self.is_alive() :
            _start()
        elif self.is_alive() :
            logger.debug( "  ssh-agent is already running" )
        elif not self.agent_env:
            self.get_agent_env()

    def status( self ) :
        if self.is_alive() :
            logger.info( "ssh-agent is running" )
        else :
            logger.info( "ssh-agent is stopped" )

    def is_alive( self ):
        if not exists( self.agent_file ) :
            logger.debug("'%s' does not exist" % ( self.agent_file ) )
            return False
        else :
            if process_is_runnig( self.agent_file ):
                return True
            else :
                return False

    def get_agent_env( self ):
        logger.debug("Reading '%s' file" % ( self.agent_file ) )
        with open( self.agent_file , 'r' ) as f:
            lines = f.readlines()
        self.agent_env['SSH_AGENT_PID'] = lines[0].strip()
        self.agent_env['SSH_AUTH_SOCK'] = lines[1].strip()

    def update_agent_env( self ):
        env = os.environ
        if not self.agent_env :
            self.get_agent_env()
        env.update( self.agent_env )
        return env

    def add_key( self, lifetime ):
        logger.info("--> Add '%s' into ssh-agent for %s hours" % ( self.private_key , lifetime ) )
        out , err = exec_cmd( 'ssh-add -t %sh %s' % ( lifetime , self.private_key ),
                  stdin=sys.stdin, env=self.update_agent_env() )
        mo = re.compile(r'.* (\d*) .*').search( err )
        if mo :
            logger.info( " Lifetime set to " + str( datetime.timedelta( seconds=int( mo.group(1) ) ) ) )
        else :
            logger.info( err )

    def delete_key( self ):
        logger.info('--> Remove key %s' % self.private_key )
        out , err = exec_cmd( 'ssh-add -d %s' % self.private_key,
                              stdin=sys.stdin, stdout=sys.stdout, env=self.update_agent_env() )
        if err :
            logger.info( err )

    def copy_key( self ):
        logger.info("--> Copying '%s' to ~/.ssh/authorized_keys file on '%s'" % ( self.private_key, self.frontend ) )

        private_key_path = expanduser(self.private_key)
        public_key_path = expanduser(self.public_key)

        error_message = "Could not find the %s in %s.\n" \
                "Be sure to first generate the authentication keys with 'ssh_keygen'" \
                " and then specify it's path when defining your resource.\n" \
                "By default '~/.ssh/id_rsa' and '~/.ssh/id_rsa.pub' will be " \
                "considered to be your private and public keys respectively"

        if not exists(private_key_path):
            raise Exception(error_message % ('private key', private_key_path))

        if not public_key_path:
            public_key_path = private_key_path+'.pub'

        if not exists(public_key_path):
            raise Exception(error_message % ('public key', public_key_path))

        cmd='cat %s | ssh %s@%s "mkdir -p ~/.ssh;' \
        ' cat > ~/temp_pub_key && grep -q -f temp_pub_key ~/.ssh/authorized_keys || cat temp_pub_key >> ~/.ssh/authorized_keys &&' \
        ' chmod 600 ~/.ssh/authorized_keys && chmod 700 ~/.ssh && rm temp_pub_key"' % (public_key_path, self.user, self.frontend)

        #answer = subprocess.Popen(cmd.split(), stdin=subprocess.PIPE, stdout=subprocess.PIPE, stderr=subprocess.PIPE) #does not work
        answer = subprocess.Popen(cmd, stdin=subprocess.PIPE, stdout=subprocess.PIPE, stderr=subprocess.PIPE, shell=True)
        out,err = answer.communicate()

        if err:
            if "grep" in err:
                pass
            else:
                logger.info( err )
                raise Exception(err)
        logger.info( "The copy of the public key %s has been successful" % public_key_path )

    def list_key( self ):
        logger.info("--> Display '%s' key" % self.private_key )
        out , err = exec_cmd( 'ssh-add -L' , env=self.update_agent_env() )
        match = re.search( '.*%s' % basename( self.private_key ) , out )
        if match :
            logger.info( match.group() )
        else :
            logger.info( " The private key '%s' is not available anymore" % self.private_key )

    def stop( self ):
        logger.info( 'Stopping ssh-agent ... ' )
        if self.is_alive():
            out , err = exec_cmd( 'ssh-agent -k' , env=self.update_agent_env() )
            if out :
                logger.debug( out )
            if err :
                logger.info( err )
            else :
                logger.info( "  OK" )
        else:
            logger.warn( ' WARNING: ssh-agent is already stopped' )
        try:
            os.remove( self.agent_file )
        except :
            pass


class Daemon( object ):

    def __init__( self ):
        self.gwd_pid  = join( DRM4G_DIR, 'var', 'gwd.pid' )

    def status( self ):
        if self.is_alive() :
            logger.info( "DRM4G is running" )
        else :
            logger.info( "DRM4G is stopped" )

    def is_alive( self ):
        if not exists( self.gwd_pid ) :
            return False
        else :
            if process_is_runnig( self.gwd_pid ) :
                return True
            else :
                return False

    def start( self ):
        logger.info( "Starting DRM4G .... " )
        if not exists( self.gwd_pid ) or ( exists( self.gwd_pid ) and not process_is_runnig( self.gwd_pid ) ) :
            lock = join( DRM4G_DIR , 'var', '/.lock' )
            if exists( lock ) :
                os.remove( lock )
            logger.debug( "Starting gwd .... " )
            out , err = exec_cmd( 'gwd' )
            if err :
                logger.info( err )
            if out :
                logger.info( out )
            if not err and not out :
                logger.info( "  OK" )
        else :
            logger.info( "  WARNING: DRM4G is already running." )

    def stop( self ):
        logger.info( "Stopping DRM4G .... " )
        logger.debug( "Stopping gwd .... " )
        out , err = exec_cmd( "%s -k" % ( "gwd" ) )
        if err :
            logger.info( err )
        if out :
            logger.info( out )
        if not err and not out :
            while self.is_alive() :
                time.sleep(1)
            logger.info( "  OK" )

    def clear( self ):
        yes_choise = yes_no_choice( "Do you want to continue clearing DRM4G? " )
        if yes_choise :
            logger.info( "Clearing DRM4G .... " )
            cmd = "%s -c" % ( "gwd" )
            out , err = exec_cmd( cmd )
            logger.debug( out )
            if err :
                logger.info( err )
            if out :
                logger.info( out )
            if not err and not out :
                logger.info( "  OK" )
        else :
            self.start()

class Resource( object ):

    def __init__( self , config ):
        self.config = config

    def create_vms(self):
        """
        Creates a virtual machine with the information given through the resources.conf and cloudsetup.json files
        """
        self.check( )
        for resname, resdict in self.config.resources.items():
            if resdict[ 'lrms' ] in ['rocci']:
                rocci.manage_instances('start', resname, resdict)

    def destroy_vms(self):
        """
        Destroy running virtual machines
        """
        self.check( )
        for resname, resdict in self.config.resources.items():
            if resdict[ 'lrms' ] in ['rocci']:
                rocci.manage_instances('stop', resname, resdict)

    def update_hosts(self):
        """
        Forces the host list to be updated
        """
        try:
            GwImMad().do_DISCOVER("discover - - -", False)
            #GwImMad().do_MONITOR("monitor 2 %s -" % resname)#, False)
        except Exception as err:
            logger.error( "Could not update hosts:\n%s" % str(err))

    def list_resources(self):
        """
        List all resources, the ones configured by the user and the ones configured internally
        For example, when creating cloud virtual machines
        """
        self.check( )
        logger.info("Resources:")
        for resname, resdict in self.config.resources.items():
            logger.info("    "+str(resname))
            logger.info("        communicator:  "+str(resdict['communicator']))
            if 'username' in resdict.keys():
                logger.info("        username:      "+str(resdict['username']))
            logger.info("        frontend:      "+str(resdict['frontend']))
            if 'private_key' in resdict.keys():
                logger.info("        private key:   "+str(resdict['private_key']))
            logger.info("        lrms:          "+str(resdict['lrms']))

    def check_frontends( self ) :
        """
        Check if the frontend of a given resource is reachable.
        """
        self.check( )
        communicators = self.config.make_communicators()
        for resname, resdict in sorted( self.config.resources.items() ) :
            if resdict[ 'enable' ] == 'true' :
                communicator = communicators.get( resname )
                try :
                    if resdict[ 'communicator' ] == 'op_ssh' :
                        #it will use im's socket instead of creating a new one
                        communicator.parent_module = 'im'
                        communicator.configfile = join(DRM4G_DIR, 'etc', 'openssh_im.conf')
                    communicator.connect()
                    logger.info( "Resource '%s' :" % ( resname ) )
                    logger.info( "--> The front-end '%s' is accessible\n" % communicator.frontend )
                except Exception as err :
                    logger.error( "Resource '%s' :" % ( resname ) )
                    logger.error( "--> The front-end '%s' is not accessible\n" % communicator.frontend )

    def edit( self ) :
        """
        Edit resources file.
        """
        logger.debug( "Editing '%s' file" % DRM4G_CONFIG_FILE )
        os.system( "%s %s" % ( os.environ.get('EDITOR', 'nano') , DRM4G_CONFIG_FILE ) )
        self.check( )

    def list( self ) :
        """
        Check if the resource.conf file has been configured well and list the resources available.
        """
        self.check( )
        logger.info( "\033[1;4m%-20.20s%-20.20s\033[0m" % ('RESOURCE', 'STATE' ) )
        for resname, resdict in sorted( self.config.resources.items() ) :
            # To ignore VMs created by the DRM4G, since they should appear as hosts, not resources
            if '_' in resname:
                first_half, _ = resname.rsplit('_',1)
                if first_half in self.config.resources.keys() :
                    continue
            if resdict[ 'enable' ] == 'true' :
                state = 'enabled'
            else :
                state = 'disabled'
            logger.info( "%-20.20s%s" % ( resname, state ) )

    def features( self ) :
        """
        List the features of a given resource.
        """
        self.check( )
        for resname, resdict in sorted( self.config.resources.items() ) :
            logger.info( "Resource '%s' :" % ( resname ) )
            for key , val in sorted( resdict.items() ) :
                logger.info( " --> '%s' : '%s'" % ( key, val ) )

    def check( self ) :
        """
        Check if the resource.conf file has been configured well.
        """
        self.config.load()
        errors = self.config.check()
        if errors :
            raise Exception( "Please, review your configuration file" )

class Proxy( object ):

    def __init__( self, resource, communicator ):
        self.resource     = resource
        self.communicator = communicator
        if 'myproxy_server' in self.resource :
            self.prefix = "X509_USER_PROXY=%s MYPROXY_SERVER=%s %s" % (
                                                                 join( REMOTE_VOS_DIR , self.resource[ 'myproxy_server' ] ),
                                                                 self.resource[ 'myproxy_server' ],
                                                                 "GT_PROXY_MODE=rfc " if self.resource[ "lrms" ] == "rocci" else ""
                                                                 )
        else :
            self.prefix = "X509_USER_PROXY=%s/${MYPROXY_SERVER} %s" % ( REMOTE_VOS_DIR,
                                                                        "GT_PROXY_MODE=rfc " if self.resource[ "lrms" ] == "rocci" else "" )

    def create( self , proxy_lifetime ):
        logger.info("--> Creating '%s' directory to store the proxy ... " % REMOTE_VOS_DIR )
        cmd = "mkdir -p %s" % REMOTE_VOS_DIR
        logger.debug( "Executing command ... " + cmd )
        out, err = self.communicator.execCommand( cmd )
        if not err :
            logger.info("--> Create a local proxy credential ... ")
            message      = 'Insert your Grid password: '
            grid_passwd  = getpass.getpass(message)
            cmd = self.prefix + "myproxy-init -S --cred_lifetime %s --proxy_lifetime %s --local_proxy -n -d" % (
                                                                                                         proxy_lifetime ,
                                                                                                         proxy_lifetime
                                                                                                         )
            logger.debug( "Executing command ... " + cmd )
            out , err = self.communicator.execCommand( cmd , input = grid_passwd )
            logger.info( out )
            if err :
                logger.info( err )
        else :
            raise Exception( err )

    def configure( self ) :
        certificate = self.resource.get( 'grid_cert' )
        if not certificate :
            logger.warning( " WARNING: It is assumed that the grid certificate has been already configured" )
        else :
            dir_certificate   = dirname( certificate )
            base_certificate  = basename( certificate )
            logger.info( "--> Converting '%s' key to pem format ... " % base_certificate )
            cmd = "openssl pkcs12 -nocerts -in %s -out %s" % ( certificate, join( dir_certificate, 'userkey.pem' ) )
            out , err = exec_cmd( cmd, stdin=sys.stdin, stdout=sys.stdout )
            if "invalid password" in err :
                raise Exception( err )
            logger.info( "--> Converting '%s' certificate to pem format ... " % base_certificate )
            cmd = "openssl pkcs12 -clcerts -nokeys -in %s -out %s" % ( certificate, join( dir_certificate, 'usercert.pem' ) )
            out , err = exec_cmd( cmd , stdin=sys.stdin, stdout=sys.stdout )
            if "invalid password" in err :
                raise Exception( err )
            logger.debug( "--> Creating '~/.globus' directory ... " )
            cmd = "mkdir -p ~/.globus"
            logger.debug( "Executing command ... " + cmd )
            out, err = self.communicator.execCommand( cmd )
            if err :
                raise Exception( err )
            for file in [ 'userkey.pem' , 'usercert.pem' ] :
                cmd = "rm -rf $HOME/.globus/%s" % file
                logger.debug( "Executing command ... " + cmd )
                out, err = self.communicator.execCommand( cmd )
                if err :
                    raise Exception( err )
                logger.info( "--> Copying '%s' to '%s' ..." % ( file , self.resource.get( 'frontend' ) ) )
                self.communicator.copy( 'file://%s'  % join( dir_certificate, file ) ,
                                        'ssh://_/%s' % join( '.globus' , file ) )
            logger.info( "--> Modifying userkey.pem permissions ... " )
            cmd = "chmod 400 $HOME/.globus/userkey.pem"
            logger.debug( "Executing command ... " + cmd )
            out, err = self.communicator.execCommand( cmd )
            if err :
                raise Exception( err )
            logger.info( "--> Modifying usercert.pem permissions ... " )
            cmd = "chmod 644 $HOME/.globus/usercert.pem"
            logger.debug( "Executing command ... " + cmd )
            out, err = self.communicator.execCommand( cmd )
            if err :
                raise Exception( err )

    def check( self ):
        logger.info( "--> Display information about the proxy certificate" )
        cmd = self.prefix + "grid-proxy-info"
        logger.debug( "Executing command ... " + cmd )
        out, err = self.communicator.execCommand( cmd )
        logger.info( out )
        if err :
            logger.info( err )

    def destroy( self ):
        logger.info( "--> Remove grid credentials" )
        cmd = self.prefix + "myproxy-destroy"
        logger.debug( "Executing command ... " + cmd )
        out , err = self.communicator.execCommand( cmd )
        logger.info( out )
        if err :
            logger.info( err )
        cmd = self.prefix + "grid-proxy-destroy"
        logger.debug( "Executing command ... " + cmd )
        out , err = self.communicator.execCommand( cmd )
        logger.info( out )
        if err :
            logger.info( err )

