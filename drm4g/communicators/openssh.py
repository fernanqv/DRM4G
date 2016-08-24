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
from os.path     import dirname, abspath, join, expanduser, exists
from openssh_wrapper import SSHConnection

import socket
import re
import logging
import drm4g.communicators
import drm4g.commands
from drm4g.commands         import Agent
from drm4g.communicators    import ComException, logger
from drm4g                  import SFTP_CONNECTIONS, SSH_CONNECT_TIMEOUT, DRM4G_DIR
from drm4g.utils.url        import urlparse

__version__  = '2.5.0-0b2'
__author__   = 'Carlos Blanco'
__revision__ = "$Id$"

class Communicator(drm4g.communicators.Communicator):
    """
    Create a SSH session to remote resources.
    """
    _lock       = __import__('threading').Lock()
    _sem        = __import__('threading').Semaphore(SFTP_CONNECTIONS)
    _trans      = None

    conn=None
    #agent=None

    def connect(self):
        """
        To establish the connection to resource.
        """
        #agent=Agent()

    def execCommand(self , command , input = None ):
        '''
        TODO
        para habilitar la multiplexacion, despues del port especificar el configfile=join(DRM4G_DIR, 'etc', 'openssh.conf')
        '''
        #raise Exception("\n\n\n\nNO APARECE POR NINGUN SITIO\n\n\n\n")
        #agent_socket=agent.update_agent_env()['SSH_AUTH_SOCK']
        #logger.debug('\n'+str(agent_socket)+'\n')
        logger.info("\nHI, HOW YA DOIN' ON THIS MORROW\n")
        #conn = SSHConnection(self.frontend, login=self.username, port=self.port, identity_file=self.private_key, 
        #                    ssh_agent_socket=agent_socket, timeout=SSH_CONNECT_TIMEOUT)
        self.conn = SSHConnection(self.frontend, login=self.username, port=str(self.port), identity_file=self.private_key,timeout=SSH_CONNECT_TIMEOUT)
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
        to_dir         = self._set_dir(urlparse(url).path)
        stdout, stderr = self.execCommand( "mkdir -p %s" % to_dir )
        if stderr :
            raise ComException( "Could not create %s directory: %s" % ( to_dir , stderr ) )

    def rmDirectory(self, url):
        logger.warning('HOla')
        to_dir         = self._set_dir(urlparse(url).path)
        stdout, stderr = self.execCommand( "rm -rf %s" % to_dir )
        if stderr:
            raise ComException( "Could not remove %s directory: %s" % ( to_dir , stderr ) )

    def copy( self , source_url , destination_url , execution_mode = '' ) :
        with self._sem :
            if 'file://' in source_url :
                from_dir = urlparse( source_url ).path
                to_dir   = self._set_dir( urlparse( destination_url ).path )
                if execution_mode == 'X':
                    self.conn.scp( from_dir , target=to_dir , mode='+x' )
                else:
                    self.conn.scp( from_dir , target=to_dir )
            else:
                from_dir = self._set_dir( urlparse( source_url ).path )
                to_dir   = urlparse(destination_url).path
                logger.warning( "%s , %s" %  (from_dir, to_dir  ))
                self.conn.scp( from_dir , target=to_dir )

    #internal
    def _set_dir(self, path):
        work_directory =  re.compile( r'^~' ).sub( self.work_directory , path )
        if work_directory[0] == r'~' :
            return ".%s" %  work_directory[ 1: ]
        else :
            return  work_directory