#
# Copyright 2021 Santander Meteorology Group (UC-CSIC)
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
import sys
import re
import time
import signal
import getpass
import subprocess
import datetime
import pprint

from drm4g             import REMOTE_VOS_DIR, DRM4G_RESOURCES_CONF, DRM4G_DIR,console_logger, DRM4G_DIR_VAR
#from drm4g.managers    import rocci
from drm4g.core.im_mad import GwImMad
from os.path           import expanduser, join, dirname, exists, basename, expandvars


PY2 = sys.version_info[0] == 2


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
    console_logger.debug( "Executing command ... " + cmd )
    cmd_to_exec = subprocess.Popen(  cmd ,
                                  shell=True ,
                                  stdin=subprocess.PIPE,
                                  stdout=subprocess.PIPE,
                                  stderr=subprocess.PIPE,
                                  env=env
                                  )
    out , err =  cmd_to_exec.communicate()
    return out.decode() , err.decode()

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
        self.agent_file   = join(DRM4G_DIR_VAR, 'agent.conf' )

    def start( self ):
        def _start():
            # 's' option generates Bourne shell commands on stdout
            out , err = exec_cmd( 'ssh-agent -s ' )
            console_logger.debug( out )
            match = re.search( 'SSH_AUTH_SOCK=(?P<SSH_AUTH_SOCK>[^;]+);.*' \
                           + 'SSH_AGENT_PID=(?P<SSH_AGENT_PID>\d+);', out, re.DOTALL)
            if match :
                console_logger.debug( "  OK" )
                self.agent_env = match.groupdict()
                console_logger.debug( '  Agent pid: %s'  % self.agent_env['SSH_AGENT_PID'])
            else:
                console_logger.error( err )
                raise Exception( '  Cannot determine agent data from output: %s' % out )
            with open( self.agent_file , 'w') as f:
                f.write( self.agent_env['SSH_AGENT_PID'] + '\n' + self.agent_env['SSH_AUTH_SOCK'] )
        console_logger.debug('Starting ssh-agent ...')
        if not self.is_alive() :
            _start()
        elif self.is_alive() :
            console_logger.debug( "  ssh-agent is already running" )
        elif not self.agent_env:
            self.get_agent_env()

    def status( self ) :
        if self.is_alive() :
            console_logger.info( "ssh-agent is running" )
        else :
            console_logger.info( "ssh-agent is stopped" )

    def is_alive( self ):
        if not exists( self.agent_file ) :
            console_logger.debug("  '%s' does not exist" % ( self.agent_file ) )
            return False
        else :
            if process_is_runnig( self.agent_file ):
                return True
            else :
                return False

    def get_agent_env( self ):
        console_logger.debug("Reading '%s' file" % ( self.agent_file ) )
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
        console_logger.info("--> Add '%s' into ssh-agent for %s hours" % ( self.private_key , lifetime ) )
        out , err = exec_cmd( 'ssh-add -t %sh %s' % ( lifetime , self.private_key ),
                  stdin=sys.stdin, env=self.update_agent_env() )
        mo = re.compile(r'.* (\d*) .*').search( err )
        if mo :
            console_logger.info( " Lifetime set to " + str( datetime.timedelta( seconds=int( mo.group(1) ) ) ) )
        else :
            console_logger.info( err )

    def delete_key( self ):
        console_logger.info('--> Remove key %s' % self.private_key )
        out , err = exec_cmd( 'ssh-add -d %s' % self.private_key,
                              stdin=sys.stdin, stdout=sys.stdout, env=self.update_agent_env() )
        if err :
            console_logger.info( err )

    def copy_key( self ):
        console_logger.info("--> Copying '%s' to ~/.ssh/authorized_keys file on '%s'" % ( self.private_key, self.frontend ) )

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
                console_logger.info( err )
                raise Exception(err)
        console_logger.info( "The copy of the public key %s has been successful" % public_key_path )

    def list_key( self ):
        console_logger.info("--> Display '%s' key" % self.private_key )
        out , err = exec_cmd( 'ssh-add -L' , env=self.update_agent_env() )
        match = re.search( '.*%s' % basename( self.private_key ) , out )
        if match :
            console_logger.info( match.group() )
        else :
            console_logger.info( " The private key '%s' is not available anymore" % self.private_key )

    def stop( self ):
        console_logger.info( 'Stopping ssh-agent ... ' )
        if self.is_alive():
            out , err = exec_cmd( 'ssh-agent -k' , env=self.update_agent_env() )
            if out :
                console_logger.debug( out )
            if err :
                console_logger.info( err )
            else :
                console_logger.info( "  OK" )
        else:
            console_logger.warn( '  WARNING: ssh-agent is already stopped' )
        try:
            os.remove( self.agent_file )
        except :
            pass


class Daemon( object ):

    def __init__( self ):
        self.gwd_pid  = join(DRM4G_DIR_VAR , 'gwd.pid' )

    def status( self ):
        if self.is_alive() :
            console_logger.info( "DRM4G is running" )
        else :
            console_logger.info( "DRM4G is stopped" )

    def is_alive( self ):
        if not exists( self.gwd_pid ) :
            return False
        else :
            if process_is_runnig( self.gwd_pid ) :
                return True
            else :
                return False

    def start( self ):
        console_logger.info( "Starting DRM4G .... " )
        if not exists( self.gwd_pid ) or ( exists( self.gwd_pid ) and not process_is_runnig( self.gwd_pid ) ) :
            lock = join(DRM4G_DIR_VAR , '/.lock' )
            if exists( lock ) :
                os.remove( lock )
            console_logger.debug( "Starting gwd .... " )
            out , err = exec_cmd( 'gwd' )
            if err :
                console_logger.info( err )
            if out :
                console_logger.info( out )
            if not err and not out :
                console_logger.info( "  OK" )
        else :
            console_logger.info( "  WARNING: DRM4G is already running" )

    def stop( self ):
        console_logger.info( "Stopping DRM4G .... " )
        console_logger.debug( "Stopping gwd .... " )
        if self.is_alive():
            with open(self.gwd_pid) as pid_file:
                pid = next(pid_file)
                processes_to_kill = [pid]
                while processes_to_kill:
                    for process in processes_to_kill:
                        try:
                            cmd = "ps ho pid --ppid %s" % (process)
                            out , err = exec_cmd( cmd )
                            processes_to_kill = [line.lstrip() for line in out.splitlines()] + processes_to_kill
                            processes_to_kill.remove(process)
                            os.kill( int(process), signal.SIGTERM )
                        except OSError as err:
                            if "No such process" in err:
                                continue
                            raise
            while self.is_alive() :
                time.sleep(1)
            try:
                os.remove(self.gwd_pid)
            except OSError:
                console_logger.error("  '%s' does not exist" % self.gwd_pid)
            console_logger.info( "  OK" )
        else:
            console_logger.info("  WARNING: daemon is already stopped")

    def clear( self ):
        yes_choise = yes_no_choice( "Do you want to continue clearing DRM4G? " )
        if yes_choise :
            console_logger.info( "Clearing DRM4G .... " )
            cmd = "%s -c" % ( "gwd" )
            out , err = exec_cmd( cmd )
            console_logger.debug( out )
            if err :
                console_logger.info( err )
            if out :
                console_logger.info( out )
            if not err and not out :
                console_logger.info( "  OK" )
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
            console_logger.error( "Could not update hosts:\n%s" % str(err))

    def list_resources(self):
        """
        List all resources, the ones configured by the user and the ones configured internally
        For example, when creating cloud virtual machines
        """
        self.check( )
        console_logger.info("Resources:")
        for resname, resdict in self.config.resources.items():
            console_logger.info("    "+str(resname))
            console_logger.info("        communicator:  "+str(resdict['communicator']))
            if 'username' in resdict.keys():
                console_logger.info("        username:      "+str(resdict['username']))
            console_logger.info("        frontend:      "+str(resdict['frontend']))
            if 'private_key' in resdict.keys():
                console_logger.info("        private key:   "+str(resdict['private_key']))
            console_logger.info("        lrms:          "+str(resdict['lrms']))

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
                    # if resdict[ 'communicator' ] == 'op_ssh' :
                    #     #it will use im's socket instead of creating a new one
                    #     communicator.parent_module = 'im'
                    #     communicator.configfile = join(DRM4G_DIR, 'etc', 'openssh_im.conf')
                    communicator.connect()
                    console_logger.info( "Resource '%s' :" % ( resname ) )
                    console_logger.info( "--> The front-end '%s' is accessible\n" % communicator.frontend )
                except Exception as err :
                    console_logger.error( "Resource '%s' :" % ( resname ) )
                    console_logger.error( "--> The front-end '%s' is not accessible\n" % communicator.frontend )

    def edit( self ) :
        """
        Edit resources file.
        """
        console_logger.debug( "Editing '%s' file" % DRM4G_RESOURCES_CONF )
        os.system( "%s %s" % ( os.environ.get('EDITOR', 'nano') , DRM4G_RESOURCES_CONF ) )
        self.check( )

    def list( self ) :
        """
        Check if the resource.conf file has been configured well and list the resources available.
        """
        self.check( )
        console_logger.info( "\033[1;4m%-20.20s%-20.20s\033[0m" % ('RESOURCE', 'STATE' ) )
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
            console_logger.info( "%-20.20s%s" % ( resname, state ) )

    def features( self ) :
        """
        List the features of a given resource.
        """
        self.check( )
        for resname, resdict in sorted( self.config.resources.items() ) :
            console_logger.info( "Resource '%s' :" % ( resname ) )
            for key , val in sorted( resdict.items() ) :
                console_logger.info( " --> '%s' : '%s'" % ( key, val ) )

    def check( self ) :
        """
        Check if the resource.conf file has been configured well.
        """
        self.config.load()
        errors = self.config.check()
        if errors :
            raise Exception( "Please, review your configuration file:\n%s" % pprint.pformat(errors) )

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
        console_logger.info("--> Creating '%s' directory to store the proxy ... " % REMOTE_VOS_DIR )
        cmd = "mkdir -p %s" % REMOTE_VOS_DIR
        console_logger.debug( "Executing command ... " + cmd )
        out, err = self.communicator.execCommand( cmd )
        if not err :
            console_logger.info("--> Create a local proxy credential ... ")
            message      = 'Insert your Grid password: '
            grid_passwd  = getpass.getpass(message)
            cmd = self.prefix + "myproxy-init -S --cred_lifetime %s --proxy_lifetime %s --local_proxy -n -d" % (
                                                                                                         proxy_lifetime ,
                                                                                                         proxy_lifetime
                                                                                                         )
            console_logger.debug( "Executing command ... " + cmd )
            out , err = self.communicator.execCommand( cmd , input = grid_passwd )
            console_logger.info( out )
            if err :
                console_logger.info( err )
        else :
            raise Exception( err )

    def configure( self ) :
        certificate = self.resource.get( 'grid_cert' )
        if not certificate :
            console_logger.warning( " WARNING: It is assumed that the grid certificate has been already configured" )
        else :
            dir_certificate   = dirname( certificate )
            base_certificate  = basename( certificate )
            console_logger.info( "--> Converting '%s' key to pem format ... " % base_certificate )
            cmd = "openssl pkcs12 -nocerts -in %s -out %s" % ( certificate, join( dir_certificate, 'userkey.pem' ) )
            out , err = exec_cmd( cmd, stdin=sys.stdin, stdout=sys.stdout )
            if "invalid password" in err :
                raise Exception( err )
            console_logger.info( "--> Converting '%s' certificate to pem format ... " % base_certificate )
            cmd = "openssl pkcs12 -clcerts -nokeys -in %s -out %s" % ( certificate, join( dir_certificate, 'usercert.pem' ) )
            out , err = exec_cmd( cmd , stdin=sys.stdin, stdout=sys.stdout )
            if "invalid password" in err :
                raise Exception( err )
            console_logger.debug( "--> Creating '~/.globus' directory ... " )
            cmd = "mkdir -p ~/.globus"
            console_logger.debug( "Executing command ... " + cmd )
            out, err = self.communicator.execCommand( cmd )
            if err :
                raise Exception( err )
            for file in [ 'userkey.pem' , 'usercert.pem' ] :
                cmd = "rm -rf $HOME/.globus/%s" % file
                console_logger.debug( "Executing command ... " + cmd )
                out, err = self.communicator.execCommand( cmd )
                if err :
                    raise Exception( err )
                console_logger.info( "--> Copying '%s' to '%s' ..." % ( file , self.resource.get( 'frontend' ) ) )
                self.communicator.copy( 'file://%s'  % join( dir_certificate, file ) ,
                                        'ssh://_/%s' % join( '.globus' , file ) )
            console_logger.info( "--> Modifying userkey.pem permissions ... " )
            cmd = "chmod 400 $HOME/.globus/userkey.pem"
            console_logger.debug( "Executing command ... " + cmd )
            out, err = self.communicator.execCommand( cmd )
            if err :
                raise Exception( err )
            console_logger.info( "--> Modifying usercert.pem permissions ... " )
            cmd = "chmod 644 $HOME/.globus/usercert.pem"
            console_logger.debug( "Executing command ... " + cmd )
            out, err = self.communicator.execCommand( cmd )
            if err :
                raise Exception( err )

    def check( self ):
        console_logger.info( "--> Display information about the proxy certificate" )
        cmd = self.prefix + "grid-proxy-info"
        console_logger.debug( "Executing command ... " + cmd )
        out, err = self.communicator.execCommand( cmd )
        console_logger.info( out )
        if err :
            console_logger.info( err )

    def destroy( self ):
        console_logger.info( "--> Remove grid credentials" )
        cmd = self.prefix + "myproxy-destroy"
        console_logger.debug( "Executing command ... " + cmd )
        out , err = self.communicator.execCommand( cmd )
        console_logger.info( out )
        if err :
            console_logger.info( err )
        cmd = self.prefix + "grid-proxy-destroy"
        console_logger.debug( "Executing command ... " + cmd )
        out , err = self.communicator.execCommand( cmd )
        console_logger.info( out )
        if err :
            console_logger.info( err )

