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

import sys
import subprocess
import os
import re
import drm4g.communicators
from drm4g.communicators import ComException
from drm4g.utils.url     import urlparse

import logging
logger  = logging.getLogger(__name__)

import stat
execution_permissions = stat.S_IRUSR | stat.S_IWUSR | stat.S_IXUSR | stat.S_IRGRP | stat.S_IXGRP | stat.S_IROTH | stat.S_IXOTH

class Communicator(drm4g.communicators.Communicator):
    """
    Interact with local resources using shell commands
    """
    _lock = __import__('threading').Lock()

    def connect(self):
        logger.debug( "Your are using the local communicator" )

    def execCommand(self, command, input=None ):
        command_proc = subprocess.Popen(command,
            shell = True,
            stdin  = subprocess.PIPE,
            stdout = subprocess.PIPE,
            stderr = subprocess.PIPE,
            env = os.environ)
        if input :
            for line in input.split():
                command_proc.stdin.write("%s\n" % line)
                command_proc.stdin.flush()
                stdout, stderr = command_proc.communicate("%s\n" % line)
        else :
            stdout, stderr = command_proc.communicate()
        return stdout.decode() , stderr.decode()

    def mkDirectory(self, url):
        to_dir = self._set_dir(urlparse(url).path)
        out, err = self.execCommand("mkdir -p %s" % to_dir )
        if err:
            output = "Could not create %s directory: %s " % ( to_dir , ' '.join( err.split( '\n' ) ) )
            logger.error( output )
            raise ComException( output )

    def copy(self, source_url, destination_url, execution_mode):
        with self._lock:
            if 'file://' in source_url:
                from_dir = urlparse(source_url).path
                to_dir   = self._set_dir(urlparse(destination_url).path)
            else:
                from_dir = self._set_dir(urlparse(source_url).path)
                to_dir   = urlparse(destination_url).path
            out, err = self.execCommand("cp -r %s %s" % (from_dir,to_dir))
            if err:
                output = "Could not copy from %s to %s : %s" % ( from_dir, to_dir , ' '.join( err.split( '\n' ) ) )
                logger.error( output )
                raise ComException( output )
            if execution_mode == 'X':
                os.chmod(to_dir, execution_permissions )

    def rmDirectory(self, url):
        to_dir   = self._set_dir(urlparse(url).path)
        out, err = self.execCommand("rm -rf %s" % to_dir )
        if err:
            output = "Could not remove %s directory: %s " % ( to_dir , ' '.join( err.split( '\n' ) ) )
            logger.error( output )
            raise ComException( output )

    def checkoutLock(self, url):
        to_dir = self._set_dir(urlparse(url).path)
        return os.path.isfile( '%s/.lock' % to_dir )

    def close(self):
        pass


    #internal
    def _set_dir(self, path):
        work_directory = os.path.expanduser( self.work_directory )
        return re.compile( r'^~' ).sub( work_directory , path )


