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

import logging
from drm4g import REMOTE_JOBS_DIR, SSH_PORT


logger  = logging.getLogger(__name__)

class ComException(Exception):
    """
    Exception raised by failures in Communicator class
    """
    pass

class Communicator(object):
    """
    Communicator is a abstract class that you must overload for your
    particular communicator. Communicator defines several methods to
    interact with computing resources.
    """
    def __init__(self):
        self.work_directory = REMOTE_JOBS_DIR
        self.port           = SSH_PORT
        self.username       = None
        self.frontend       = None
        self.private_key    = None
        self.public_key     = None

    def connect(self):
        """
        To establish the connection to resource.
        """
        pass

    def execCommand(self, command , input=None ):
        """
        Execute command and return stdout and stderr.

        @param command: a shell command to execute.
        @type command: string
        @param input: optional input argument
        @type input: string
        @return: stdout and stderr associated with the command executed
        @rtype: tuple of string (stdout, stderr)
        """
        pass

    def mkDirectory(self, destination_url):
        """
        Create a directory.

        @param destination_url: url of the folder to create
        @type destination_url: string
        """
        pass

    def copy(self, source_url, destination_url, execution_mode = 'X'):
        """
        Copy a file from source_url to destination_url. If execution_mode = 'X' you
        set execute permission to the destination file.

        @param source_url : file source (url) to copy
        @type source_url: string
        @param destination_url : file destination (url)
        @type destination_url: string
        @param execution_mode : give execute permissions to the file
        @type execution_mode : string
        """
        pass

    def rmDirectory(self, destination_url):
        """
        Remove a directory.

        @param destination_url: url of the folder to remove
        @type destination_url: string
        """
        pass

    def close(self):
        """
        Close the connection.
        """
        pass