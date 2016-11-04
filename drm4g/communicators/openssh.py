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
from os.path     import dirname, abspath, join, expanduser, exists, basename

import io
import socket
import re
import time
import pipes
import signal
import logging
import threading
import subprocess
import drm4g.communicators
import drm4g.commands
from drm4g.commands         import Agent
from drm4g.communicators    import ComException, logger
from drm4g                  import SFTP_CONNECTIONS, SSH_CONNECT_TIMEOUT, DRM4G_DIR
from drm4g.utils.url        import urlparse
from openssh_wrapper import SSHConnection

__version__  = '2.5.1'
__author__   = 'Carlos Blanco'
__revision__ = "$Id$"

class Communicator(drm4g.communicators.Communicator):
    """
    Create a SSH session to remote resources.
    """
    _lock       = __import__('threading').Lock()
    _sem        = __import__('threading').Semaphore(SFTP_CONNECTIONS)
    _trans      = None

    socket_dir=None

    def __init__(self):
        super(Communicator,self).__init__()
        self.conn=None
        self.parent_module=None
        with self._lock:
            self.agent=Agent()
            self.agent.start()
        self.agent_socket=self.agent.update_agent_env()['SSH_AUTH_SOCK']

        if not Communicator.socket_dir:
            Communicator.socket_dir=join(DRM4G_DIR, 'var', 'sockets')


    def createConfFiles(self):
        logger.debug("\nRunning createConfFiles function from "+self.parent_module+"\n")
        #the maximum length of the path of a unix domain socket is 108 on Linux, 104 on Mac OS X
        conf_text = ("Host *\n"
            "    ControlMaster auto\n"
            "    ControlPath %s/%s-%s\n"
            "    ControlPersist 10m")

        for manager in ['im', 'tm', 'em']:
            with io.FileIO(join(DRM4G_DIR, 'etc', 'openssh_%s.conf' % manager), 'w') as file:
                file.write(conf_text % (Communicator.socket_dir, manager, '%r@%h:%p'))
        try:
            if not exists(Communicator.socket_dir):
                logger.debug("\nCreating socket directory in "+Communicator.socket_dir+".\n")
                os.makedirs(Communicator.socket_dir)
        except OSError as excep:
            if "File exists" in str(excep):
                logger.warning("The directory %s already exists" % Communicator.socket_dir)
            else:
                logger.error("An unexpected exception ocurred:\n"+str(excep))
        logger.debug("\nEnding createConfFiles function from "+self.parent_module+"\n")

    def connect(self):
        """
        To establish the connection to resource.
        """
        try:
            p_module=sys._getframe().f_back.f_code.co_filename
            p_function=sys._getframe().f_back.f_code.co_name
            logger.debug("\nRunning connect function\n - called from "+p_module+"\n - by function "+p_function+"\n")

            if not self.configfile:
                logger.debug("\nself.configfile is not defined\n")
                logger.error("\n\nOpenSHH configuration file's path is not defined\n\n")

            if not exists(self.configfile):
                self.createConfFiles()
            logger.debug("\nThe socket "+join(Communicator.socket_dir, '%s-%s@%s:%s' % (self.parent_module ,self.username, self.frontend, self.port))+" existance is "+str(exists(join(Communicator.socket_dir, '%s-%s@%s:%s' % (self.parent_module ,self.username, self.frontend, self.port))))+".\n")

            if not exists(join(Communicator.socket_dir, '%s-%s@%s:%s' % (self.parent_module ,self.username, self.frontend, self.port))):
                logger.debug("\nNo master conncection exists for "+self.parent_module+" so a new one will be created.\n")
                def first_ssh():
                    logger.debug("\nRunning first_ssh function\n - Creating first connection for "+self.parent_module+"\n")
                    #this is here because the threads are created at the same time, so the moment one creates the conection, the rest are going to cause an exception (which probably shouldn't be ocurring since ControlMaster is set to auto)
                    if not exists(join(Communicator.socket_dir, '%s-%s@%s:%s' % (self.parent_module ,self.username, self.frontend, self.port))):
                        command = 'ssh -F %s -i %s -p %s -T %s@%s' % (self.configfile, self.private_key, str(self.port), self.username, self.frontend)
                    pipe = subprocess.Popen(command.split(), stdin=subprocess.PIPE, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
                    out,err = pipe.communicate()

                    if err:
                        if "too long for Unix domain socket" in str(err) or "ControlPath too long" in str(err):
                            logger.debug("\nSocket path was too long for Unix domain socket.\nCreating sockets in ~/.ssh/dmr4g.\nException captured in first_ssh.\n")
                            Communicator.socket_dir = join(expanduser('~'), '.ssh/drm4g')
                            self.createConfFiles()
                            logger.debug("\nCalling first_ssh once again, but with a new socket_dir\n")
                            first_ssh()
                        elif "disabling multiplexing" in str(err):
                            logger.debug("\nMux isn't working from the connect function. Eliminating "+self.parent_module+"'s socket file.\n This shouldn't be appearing since it's supposedly the first time that it's being created.\n")
                            self._delete_socket()
                            first_ssh()
                        elif "bind: No such file or directory" in str(err) or "cannot bind to path" in str(err):
                            logger.debug("\nThe connection through the socket %s-%s@%s:%s wasn't established since the socket directory %s hasn't been created yet.\n" % (self.parent_module ,self.username, self.frontend, self.port, Communicator.socket_dir))
                            self.createConfFiles()
                            first_ssh()
                        else:
                            logger.debug("\nUnexpected error occured while running first_ssh:\n"+str(err))
                            logger.warning(str(err))

                t = threading.Thread(target=first_ssh, args = ())
                t.daemon = True
                logger.debug("\nStarting thread with first_ssh\n")
                t.start()
                time.sleep(5) #so that there's time to make the first connection in case there was an error
                #cont=0
                # while not exists(join(Communicator.socket_dir, '%s-%s@%s:%s' % (self.parent_module ,self.username, self.frontend, self.port))) and cont < 130:
                #     cont+=1
                #     time.sleep(1)
                # if not cont < 130:
                #     logger.debug("\n**Tried to wait for first connection to be established, but after 2 minutes still nothing\n")

            if self.conn==None:
                logger.debug("\nNo conn exists (conn == "+str(self.conn)+") for "+self.parent_module+" so a new one will be created.\n")
                self.conn = SSHConnection(self.frontend, login=self.username, port=str(self.port),
                    configfile=self.configfile, identity_file=self.private_key,
                    ssh_agent_socket=self.agent_socket, timeout=SSH_CONNECT_TIMEOUT)

            logger.debug("\nEnding connect function from "+self.parent_module+"\n")

        except Exception as excep :
            if "too long for Unix domain socket" in str(excep):
                logger.debug("\nSocket path was too long for Unix domain socket.\nCreating sockets in ~/.ssh/dmr4g.\nException captured in connect's except.\n")
                Communicator.socket_dir = join(expanduser('~'), '.ssh/drm4g')
                self.createConfFiles()
                self.connect()
            else:
                logger.warning(str(excep))

    def execCommand(self , command , input = None ):
        try:
            logger.debug("\nRunning execCommand function from "+self.parent_module+"\n    - Trying to execute command "+str(command)+"\n")

            if not self.conn:
                logger.debug("\nGoing to run connect function.\n - That should already have been done, so it shouldn't do anything.\n")
                self.connect()

            ret = self.conn.run(command)
            logger.debug("\nEnding execCommand function.\n")
            return ret.stdout , ret.stderr
        except Exception as excep:
            if "disabling multiplexing" in str(excep):
                logger.debug("\nMux isn't working from the execCommand function. Eliminating "+self.parent_module+"'s socket file.\n")
                self._delete_socket()
                self.execCommand(command, input)
            else:
                logger.warning(str(excep))

    def mkDirectory(self, url):
        try:
            logger.debug("\nRunning mkDirectory function from "+self.parent_module+"\n")
            to_dir         = self._set_dir(urlparse(url).path)
            stdout, stderr = self.execCommand( "mkdir -p %s" % to_dir )
            if stderr :
                logger.warning( "Could not create %s directory: %s" % ( to_dir , stderr ) )
            logger.debug("\nEnding mkDirectory function from "+self.parent_module+"\n")
        except Exception as excep:
            if "disabling multiplexing" in str(excep):
                logger.debug("\nMux isn't working from the mkDirectory function. Eliminating "+self.parent_module+"'s socket file.\n")
                self._delete_socket()
                self.mkDirectory(url)
            else:
                #raise ComException("Error connecting to remote machine %s@%s while trying to create a folder : " % (self.username,self.frontend) + str(excep))
                logger.warning(str(excep))

    def rmDirectory(self, url):
        try:
            logger.debug("\nRunning rmDirectory function from "+self.parent_module+"\n")
            to_dir         = self._set_dir(urlparse(url).path)
            stdout, stderr = self.execCommand( "rm -rf %s" % to_dir )
            if stderr:
                logger.warning( "Could not remove %s directory: %s" % ( to_dir , stderr ) )
            logger.debug("\nEnding rmDirectory function from "+self.parent_module+"\n")
        except Exception as excep:
            if "disabling multiplexing" in str(excep):
                logger.debug("\nMux isn't working from the rmDirectory function. Eliminating "+self.parent_module+"'s socket file.\n")
                self._delete_socket()
                self.rmDirectory(url)
            else:
                #raise ComException("Error connecting to remote machine %s@%s while trying to remove a folder : " % (self.username,self.frontend) + str(excep))
                logger.warning(str(excep))

    def copy( self , source_url , destination_url , execution_mode = '' ) :
        try:
            logger.debug("\nRunning copy function from "+self.parent_module+"\n")
            if not self.conn:
                self.connect()
            with self._sem :
                if 'file://' in source_url :
                    from_dir = urlparse( source_url ).path
                    to_dir   = self._set_dir( urlparse( destination_url ).path )
                    self.conn.scp( [from_dir] , target=to_dir )
                    if execution_mode == 'X':
                        stdout, stderr = self.execCommand( "chmod +x %s" % to_dir )
                        if stderr :
                            logger.warning( "Could not change access permissions of %s file: %s" % ( to_dir , stderr ) )
                else:
                    from_dir = self._set_dir( urlparse( source_url ).path )
                    to_dir   = urlparse(destination_url).path
                    self.remote_scp( [from_dir] , target=to_dir )
            logger.debug("\nEnding copy function from "+self.parent_module+"\n")
        except Exception as excep:
            if "disabling multiplexing" in str(excep):
                logger.debug("\nMux isn't working from the copy function. Eliminating "+self.parent_module+"'s socket file.\n")
                self._delete_socket()
                self.copy(source_url , destination_url)
            else:
                #raise ComException("Error connecting to remote machine %s@%s while trying to copy a file : " % (self.username,self.frontend) + str(excep))
                logger.warning(str(excep))

    def _delete_socket(self):
        try:
            logger.debug("\nRunning _delete_socket function from "+self.parent_module+"\n")
            os.remove("%s/%s-%s@%s:%s" % (Communicator.socket_dir,self.parent_module,self.username,self.frontend,str(self.port)))
            logger.debug("\nEnding _delete_socket function from "+self.parent_module+"\n")
        except OSError as excep:
            if "No such file or directory" in str(excep):
                logger.debug("The socket %s does not exist" % (Communicator.socket_dir,self.parent_module,self.username,self.frontend,str(self.port)))
                logger.debug("\nEnding _delete_socket function from "+self.parent_module+"\n")
            else:
                logger.warning(str(excep))

    #internal
    def _set_dir(self, path):
        logger.debug("\nRunning _set_dir function from "+self.parent_module+"\n")
        work_directory =  re.compile( r'^~' ).sub( self.work_directory , path )
        logger.debug("\nEnding _set_dir function from "+self.parent_module+"\n")
        return  work_directory

    def remote_scp(self, files, target):
        logger.debug("\nRunning remote_scp function from "+self.parent_module+"\n")
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
            logger.warning("%s (under %s): %s" % (' '.join(scp_command), self.username, str(exc)))
        signal.alarm(0)  # disable alarm
        returncode = pipe.returncode
        if returncode != 0:  # ssh client error
            logger.warning("%s (under %s): %s" % (' '.join(scp_command), self.username, err.strip()))
        logger.debug("\nEnding remote_scp function from "+self.parent_module+"\n")

    def scp_command(self, files, target, debug=False):
        """
        Build the command string to transfer the files identified by filenames.
        Include target(s) if specified. Internal function
        """
        logger.debug("\nRunning scp_command function from "+self.parent_module+"\n")
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
            #raise ValueError('"files" argument have to be iterable (list or tuple)')
            logger.warning('"files" argument have to be iterable (list or tuple)')
        if len(files) < 1:
            #raise ValueError('You should name at least one file to copy')
            logger.warning('You should name at least one file to copy')

        for f in files:
            cmd.append('%s:%s' % (remotename, f))
        cmd.append(target)
        logger.debug("\nThe command is "+str(cmd)+"\n")
        logger.debug("\nEnding scp_command function from "+self.parent_module+"\n")
        return cmd

    def get_env(self):
        """
        Retrieve environment variables and replace SSH_AUTH_SOCK
        if ssh_agent_socket was specified on object creation.
        """
        logger.debug("\nRunning get_env function from "+self.parent_module+"\n")
        env = os.environ.copy()
        if self.agent_socket:
            env['SSH_AUTH_SOCK'] = self.agent_socket
        logger.debug("\nEnding get_env function from "+self.parent_module+"\n")
        return env
