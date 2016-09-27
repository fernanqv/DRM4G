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

import sys
import platform
import os
from os.path     import dirname, abspath, join, expanduser, exists

import socket
import re
import pipes
import signal
import subprocess
import drm4g.communicators
import drm4g.commands
from drm4g.commands         import Agent
from drm4g.communicators    import ComException
from drm4g                  import SFTP_CONNECTIONS, SSH_CONNECT_TIMEOUT, DRM4G_DIR
from drm4g.utils.url        import urlparse
from openssh_wrapper import SSHConnection

__version__  = '2.5.0-0b3'
__author__   = 'Carlos Blanco'
__revision__ = "$Id$"

class Communicator(drm4g.communicators.Communicator):
    """
    Create a SSH session to remote resources.
    """
    _lock       = __import__('threading').Lock()
    _sem        = __import__('threading').Semaphore(SFTP_CONNECTIONS)
    _trans      = None

    def __init__(self):
        super(Communicator,self).__init__()
        self.configfile=join(DRM4G_DIR, 'etc', 'openssh_tm.conf')
        self.conn=None
        self.agent=Agent()
        self.agent.start()
        self.agent_socket=self.agent.update_agent_env()['SSH_AUTH_SOCK']
        if not os.path.exists('~/.ssh/drm4g'):
            subprocess.call('mkdir -p ~/.ssh/drm4g', shell=True)

    def connect(self):
        """
        To establish the connection to resource.
        """
        #if exists(join(os.environ['HOME'],'.ssh/drm4g',str(self.username)+'@'+str(self.frontend)+':'+str(self.port))):
        #    print 'conect    '+str(self.username)+'@'+str(self.frontend)+':'+str(self.port)
        if self.conn==None:
            self.conn = SSHConnection(self.frontend, login=self.username, port=str(self.port), 
                configfile=self.configfile, identity_file=self.private_key, 
                ssh_agent_socket=self.agent_socket, timeout=SSH_CONNECT_TIMEOUT)

    def execCommand(self , command , input = None ):
        self.connect()
        ret = self.conn.run(command)
        '''
        self.connect()
        with self._lock :
            channel = self._trans.open_session()
        channel.settimeout( SSH_CONNECT_TIMEOUT )
        channel.exec_command( command )
        if input :
            for line in input.split( ):
                channel.makefile( 'wb' , -1 ).write( '%s\n' % line )
                channel.makefile( 'wb' , -1 ).flush( )
        stdout = ''.join( channel.makefile( 'rb' , -1 ).readlines( ) )
        stderr = ''.join( channel.makefile_stderr( 'rb' , -1).readlines( ) )
        if channel :
            channel.close( )
        '''
        return ret.stdout , ret.stderr

    def mkDirectory(self, url):
        try:
            self.connect()
            to_dir         = self._set_dir(urlparse(url).path)
            stdout, stderr = self.execCommand( "mkdir -p %s" % to_dir )
            if stderr :
                raise ComException( "Could not create %s directory: %s" % ( to_dir , stderr ) )
        except Exception as excep:
            if "disabling multiplexing" in str(excep):
                subprocess.call("rm -r ~/.ssh/drm4g/%s@%s:%s" % (self.username,self.frontend,str(self.port)),shell=True)
                self.mkDirectory(url)
            else:
                #raise ComException("Error connecting to remote machine %s@%s while trying to create a folder : " % (self.username,self.frontend) + str(excep))
                raise

    def rmDirectory(self, url):
        try:
            self.connect()
            to_dir         = self._set_dir(urlparse(url).path)
            stdout, stderr = self.execCommand( "rm -rf %s" % to_dir )
            if stderr:
                raise ComException( "Could not remove %s directory: %s" % ( to_dir , stderr ) )
        except Exception as excep:
            if "disabling multiplexing" in str(excep):
                subprocess.call("rm -r ~/.ssh/drm4g/%s@%s:%s" % (self.username,self.frontend,str(self.port)),shell=True)
                self.rmDirectory(url)
            else:
                #raise ComException("Error connecting to remote machine %s@%s while trying to remove a folder : " % (self.username,self.frontend) + str(excep))
                raise


    def copy( self , source_url , destination_url , execution_mode = '' ) :
        try:
            self.connect()
            with self._sem :
                if 'file://' in source_url :
                    from_dir = urlparse( source_url ).path
                    to_dir   = self._set_dir( urlparse( destination_url ).path )
                    self.conn.scp( [from_dir] , target=to_dir )
                    if execution_mode == 'X':
                        stdout, stderr = self.execCommand( "chmod +x %s" % to_dir )
                        if stderr :
                            raise ComException( "Could not change access permissions of %s file: %s" % ( to_dir , stderr ) )
                else:
                    from_dir = self._set_dir( urlparse( source_url ).path )
                    to_dir   = urlparse(destination_url).path
                    self.remote_scp( [from_dir] , target=to_dir )
        except Exception as excep:
            if "disabling multiplexing" in str(excep):
                subprocess.call("rm -r ~/.ssh/drm4g/%s@%s:%s" % (self.username,self.frontend,str(self.port)),shell=True)
                self.copy(source_url , destination_url)
            else:
                #raise ComException("Error connecting to remote machine %s@%s while trying to copy a file : " % (self.username,self.frontend) + str(excep))
                raise

    #internal
    def _set_dir(self, path):
        work_directory =  re.compile( r'^~' ).sub( self.work_directory , path )
        return  work_directory


    def remote_scp(self, files, target):
        scp_command = self.scp_command(files, target)
        pipe = subprocess.Popen(scp_command,
                stdin=subprocess.PIPE, stdout=subprocess.PIPE,
                stderr=subprocess.PIPE, env=self.get_env())

        signal.alarm(SSH_CONNECT_TIMEOUT)
        err = ''
        try:
            _, err = pipe.communicate()
        except IOError as exc:
            #pipe.terminate() # only in python 2.6 allowed
            os.kill(pipe.pid, signal.SIGTERM)
            signal.alarm(0)  # disable alarm
            raise ComException("%s (under %s): %s" % (' '.join(scp_command), self.username, str(exc)))
        signal.alarm(0)  # disable alarm
        returncode = pipe.returncode
        if returncode != 0:  # ssh client error
            raise ComException("%s (under %s): %s" % (' '.join(scp_command), self.username, err.strip()))

    def scp_command(self, files, target, debug=False):
        """
        Build the command string to transfer the files identified by filenames.
        Include target(s) if specified. Internal function
        """
        cmd = ['scp', debug and '-vvvv' or '-q', '-r']

        if self.username:
            remotename = '%s@%s' % (self.username, self.frontend)
        else:
            remotename = self.frontend
        if self.configfile:
            cmd += ['-F', self.configfile]
        if self.private_key:
            cmd += ['-i', self.private_key]
        if self.port:
            cmd += ['-P', str(self.port)]

        if not isinstance(files, list):
            raise ValueError('"files" argument have to be iterable (list or tuple)')
        if len(files) < 1:
            raise ValueError('You should name at least one file to copy')

        for f in files:
            cmd.append('%s:%s' % (remotename, f))
        cmd.append(target)
        return cmd

    def get_env(self):
        """
        Retrieve environment variables and replace SSH_AUTH_SOCK
        if ssh_agent_socket was specified on object creation.
        """
        env = os.environ.copy()
        if self.agent_socket: #should i check that it's empty? "if not env['SSH_AUTH_SOCK'] and self.agent_socket:"
            env['SSH_AUTH_SOCK'] = self.agent_socket
        return env
