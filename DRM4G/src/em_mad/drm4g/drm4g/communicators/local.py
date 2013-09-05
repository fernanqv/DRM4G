import subprocess
import os
import re
import drm4g.communicators
from drm4g.communicators import ComException
from drm4g.utils.url     import urlparse

__version__  = '1.0'
__author__   = 'Carlos Blanco'
__revision__ = "$Id$"

class Communicator(drm4g.communicators.Communicator):
    """
    Interact with local resources using shell commands 
    """
        
    def execCommand(self, command):
        command_proc = subprocess.Popen(command,
            shell = True,
            stdout = subprocess.PIPE,
            stderr = subprocess.PIPE,
            env = os.environ)
        stdout, stderr = command_proc.communicate()
        return stdout , stderr 
        
    def mkDirectory(self, url):
        to_dir = self._set_dir(urlparse(url).path)
        out, err = self.execCommand("mkdir -p %s" % to_dir )
        if err:
            raise ComException( "Could not create %s directory" % to_dir )  
        
    def copy(self, source_url, destination_url, execution_mode):
        if 'file://' in source_url:
            from_dir = urlparse(source_url).path
            to_dir   = self._set_dir(urlparse(destination_url).path)
        else:
            from_dir = self._set_dir(urlparse(source_url).path)
            to_dir   = urlparse(destination_url).path
        out, err = self.execCommand("cp -r %s %s" % (from_dir,to_dir))
        if err:
            raise ComException("Could not copy from %s to %s" % (from_dir, to_dir))
        if execution_mode == 'X':
            os.chmod(to_dir, 0755)#execution permissions
            
    def rmDirectory(self, url):
        to_dir   = self._set_dir(urlparse(url).path)    
        out, err = self.execCommand("rm -rf %s" % to_dir )
        if err:
            raise ComException("Could not remove %s directory" % to_dir )
    
    def checkOutLock(self, url):   
        to_dir = self._set_dir(urlparse(url).path)
        return os.path.isfile( '%s/.lock' % to_dir )

    #internal
    def _set_dir(self, path):
        work_directory = os.path.expanduser( self.work_directory )
        return re.compile( r'^~' ).sub( work_directory , path )
            
        
