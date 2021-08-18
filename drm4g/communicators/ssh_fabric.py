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
from os.path     import dirname, abspath, join, expanduser, exists
import fabric

import re
import socket
import drm4g.commands
import drm4g.communicators
from drm4g.communicators    import ComException, SFTP_CONNECTIONS
from drm4g.utils.url        import urlparse

import logging
logger  = logging.getLogger(__name__)

class Communicator(drm4g.communicators.Communicator):
    """
    Create a SSH session to remote resources.
    """
    _lock = __import__('threading').Lock()
    _sem = __import__('threading').Semaphore(SFTP_CONNECTIONS)
    _conn = None

    def connect(self):
        with self._lock:
            if not self.is_connected:
                self._connect()

    def _connect(self):
        logger.debug("Opening ssh connection ... ")
        logger.debug("Trying '%s' key ... " % self.private_key )
        private_key_path = expanduser(self.private_key)
        if (
                (not exists( private_key_path)) and
                ('PRIVATE KEY' not in self.private_key)
        ):
            output = "'%s'key does not exist" % private_key_path
            raise ComException(output)
        logger.debug(
            "Connecting to '%s' as user '%s' port  '%s' ..."
            % ( self.frontend, self.username, self.port )
        )
        if ':' in self.frontend:
            self.frontend, self.port = self.frontend.split( ':' )
        try:
            self._conn = fabric.Connection(
                host=self.frontend,
                user=self.username,
                connect_kwargs={
                    "key_filename": private_key_path,
                },
                connect_timeout=300
            )
        except Exception as err:
            logger.warning(
                "Error connecting '%s': %s" %
                (self.frontend, str(err))
            )

    @property
    def is_connected(self):
        if self._conn is None:
            return False
        else:
            return self._conn.is_connected

    def execCommand(self, command, input=None):
        self.connect()
        with self._lock:
            result = self._conn.run(command)
            stdout = result.stdout
            stderr = result.stderr
        return stdout, stderr

    def mkDirectory(self, url):
        to_dir = self._set_dir(urlparse(url).path)
        stdout, stderr = self.execCommand("mkdir -p %s" % to_dir)
        if stderr:
            raise ComException("Could not create %s directory on '%s': %s" % (to_dir, self.frontend , stderr ))

    def rmDirectory(self, url):
        to_dir = self._set_dir(urlparse(url).path)
        stdout, stderr = self.execCommand("rm -rf %s" % to_dir)
        if stderr:
            raise ComException("Could not remove %s directory on '%s': %s" % (to_dir, self.frontend , stderr ))

    def copy(self, source_url, destination_url, execution_mode=''):
        with self._sem:
            self.connect()
            if 'file://' in source_url:
                from_dir = urlparse(source_url).path
                to_dir = self._set_dir(urlparse(destination_url).path)
                with self._lock:
                    self._conn.put(from_dir, to_dir)
                if execution_mode == 'X':
                    stdout, stderr = self.execCommand("chmod +x %s" % to_dir)
            else:
                from_dir = self._set_dir(urlparse(source_url).path)
                to_dir = urlparse(destination_url).path
                logger.warning("%s , %s" % (from_dir, to_dir))
                with self._lock:
                    self._conn.get(from_dir, to_dir)

    def close(self):
        try:
            if self.is_connected:
                self._conn.close()
        except Exception as err:
            logger.warning("Could not close the SSH connection to '%s': %s" % (self.frontend, str(err)))

    def __del__(self):
        """
        Attempt to clean up if not explicitly closed.
        """
        self.close()

    #internal
    def _set_dir(self, path):
        work_directory = re.compile(r'^~').sub(self.work_directory, path)
        if work_directory[0] == r'~':
            return ".%s" % work_directory[1:]
        else:
            return work_directory
