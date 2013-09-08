import sys
import platform
from os.path import dirname, abspath, join, expanduser, exists
try:
    import paramiko
    from paramiko.dsskey import DSSKey
    from paramiko.rsakey import RSAKey
except ImportError:
    try:
        GW_LOCATION  = dirname( dirname ( abspath( __file__ ) ) )
        cryptos_path = join( 'utils' , 'Cryptos' )
        if platform.architecture()[0] == '32bit':
            crypto_package = 'Crypto_i686'
        else:
            crypto_package = 'Crypto_x86_64'
        sys.path.append( join( cryptos_path , crypto_package ) )
        import paramiko
        from paramiko.dsskey import DSSKey
        from paramiko.rsakey import RSAKey
    except Exception, e:
        print 'Caught exception: %s' % str(e)
        sys.exit(-1)

import socket
import re
import logging
import drm4g.communicators
from drm4g.communicators import ComException, logger
from drm4g               import SFTP_CONNECTIONS, SSH_CONNECT_TIMEOUT  
from drm4g.utils.url     import urlparse

__version__  = '1.0'
__author__   = 'Carlos Blanco'
__revision__ = "$Id$"

class Communicator (drm4g.communicators.Communicator):
    """
    Create a SSH session to remote resources.  
    """
    _lock  = __import__('threading').Lock()
    _sem   = __import__('threading').Semaphore(SFTP_CONNECTIONS)
    _trans = None    
 
    def connect(self):
        with self._lock :
            if not self._trans or not self._trans.is_authenticated( ) :
                logger.debug("Opening ssh connection ... ")
                keys = None
                if not self.public_key :
                    logger.debug("Trying ssh-agent ... " )
                    agent = paramiko.Agent()
                    keys  = agent.get_keys()
                    if not keys :
                        try:
                            status_ssh_agent = agent._conn
                        except Exception, err :
                            logger.warning("Probably you are using paramiko version <= 1.7.7.2 : %s " % str( err ) )
                            status_ssh_agent = agent.conn
                        if not status_ssh_agent:
                            output = "'ssh-agent' is not running"
                            logger.error( output )
                            raise ComException( output )
                        else:
                            if agent.get_keys():
                                output = "ssh-agent is running but none of the keys have been accepted" 
                                "by remote frontend %s." % self.frontend
                                logger.error( output )
                                raise ComException( output )
                            else:
                                output = "'ssh-agent' is running but without any keys"
                                logger.error( output )
                                raise ComException( output )
                else:
                    logger.debug("Trying '%s' key ... " % self.public_key )
                    public_key_path = expanduser( self.public_key )
                    if not exists(public_key_path):
                        output = "'ssh-agent' is running but without any keys"
                        logger.error( output )
                        raise ComException( output )
                    for pkey_class in (RSAKey, DSSKey):
                        try :
                            key  = pkey_class.from_private_key_file( public_key_path )
                            keys = (key,)
                        except Exception : 
                            pass
                    if not keys :
                        output = "Impossible to load '%s' key "  % self.public_key
                        logger.error( output )
                        raise ComException( output )
                for key in keys:
                    try:
                        sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
                        sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
                        try:
                            sock.settimeout( SSH_CONNECT_TIMEOUT )
                        except:
                            logger.error("")
                        logger.debug( "Connecting to '%s' as user '%s' port  '%s' ..." 
                                           % ( self.frontend , self.username, self.port ) )
                        if ':' in self.frontend :
                            self.frontend , self.port = self.frontend.split( ':' )
                        sock.connect( ( self.frontend , self.port ) )
                        self._trans = paramiko.Transport( sock )
                        self._trans.connect( username = self.username , pkey = key )
                        if self._trans.is_authenticated( ) :
                            break
                    except socket.gaierror:
                        output = "Could not resolve hostname '%s' " % self.frontend
                        logger.error( output )
                        raise ComException( output )                        
                    except Exception:
                        logger.warning( "Bad '%s' key." % key ) 
            if not self._trans :
                output = "Authentication failed to '%s'. Try to execute `ssh -vvv -p %d %s@%s` and see the response." % ( self.frontend , self.port, self.username, self.frontend )
                logger.error( output )
                raise ComException( output )
        
    def execCommand(self, command , input=None ):
        self.connect()
        with self._lock :
            channel = self._trans.open_session()
        channel.exec_command( command )
        if input :
            for line in input.split():
                channel.makefile('wb', -1).write( '%s\n' % line )
                channel.makefile('wb', -1).flush()
        stdout = ''.join( channel.makefile('rb', -1).readlines( ) )
        stderr = ''.join( channel.makefile_stderr('rb', -1).readlines( ) )
        return stdout , stderr
            
    def mkDirectory(self, url):
        to_dir         = self._set_dir(urlparse(url).path)    
        stdout, stderr = self.execCommand( "mkdir -p %s" % to_dir )
        if stderr :
            raise ComException( "Could not create %s directory on '%s': %s" % ( to_dir , self.frontend, stderr ) )
        
    def rmDirectory(self, url):
        to_dir         = self._set_dir(urlparse(url).path)    
        stdout, stderr = self.execCommand( "rm -rf %s" % to_dir )
        if stderr:
            raise ComException( "Could not remove %s directory on '%s': %s" % ( to_dir , self.frontend, stderr ) )
            
    def copy(self, source_url, destination_url, execution_mode):
        self._sem.acquire()
        try:
            self.connect()
            with self._lock :
                sftp = paramiko.SFTPClient.from_transport( self._trans )
            if 'file://' in source_url :
                from_dir = urlparse( source_url ).path
                to_dir   = self._set_dir( urlparse( destination_url ).path )
                sftp.put( from_dir , to_dir )
                if execution_mode == 'X': 
                    sftp.chmod( to_dir, 0755 )#execution permissions
            else:
                from_dir = self._set_dir( urlparse( source_url ).path )
                to_dir   = urlparse( destination_url ).path
                sftp.get( from_dir, to_dir )
            try: 
                sftp.close()
            except Exception, err:
                logger.warning( "Could not close the sftp connection to '%s': %s" % ( self.frontend , str(err) ) )
        finally:
            self._sem.release()
            
    def checkOutLock(self, url):
        self.connect()
        with self._lock :
            sftp = paramiko.SFTPClient.from_transport( self._trans )
        to_dir = self._set_dir( urlparse( url ).path )
        try:
            file = sftp.open( '%s/.lock' % to_dir )
        except Exception:
            output = False
        else:
            file.close()
            output = True        
        try: 
            sftp.close()
        except Exception, err:
            logger.warning( "Could not close the sftp connection to '%s': %s" % ( self.frontend , str(err) ) )
        return output

    def close(self, force = True):
        with self._lock :
            try: 
                self._trans.close()
            except Exception, err: 
                logger.warning( "Could not close the SSH connection to '%s': %s" % ( self.frontend , str(err) ) )
            
    #internal
    def _set_dir(self, path):
        work_directory =  re.compile( r'^~' ).sub( self.work_directory , path )
        if work_directory[0] == r'~' :
            return ".%s" %  work_directory[ 1: ]
        else :
            return  work_directory




