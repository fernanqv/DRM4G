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
import threading
import os
import re
import logging
from drm4g.utils.url       import urlparse
from drm4g.utils.dynamic   import ThreadPool
from drm4g.core.configure  import Configuration
from drm4g.utils.message   import Send
from drm4g                 import DRM4G_DIR


class GwTmMad (object):
    """
    Transfer manager MAD

    The File Transfer Driver interfaces with Grid Data Management Services
    and is responsible for file staging, remote working directory set-up
    and remote host clean up.

    The format to send a request to the Transfer MAD, through its standard
    input, is:

    OPERATION JID TID EXE_MODE SRC_URL DST_URL

    Where:

    -OPERATION: Can be one of the following:
        -INIT: Initializes the MAD, JID should be max number of jobs.
        -START: Init transfer associated with job JID
        -END: Finish transfer associated with job JID
        -MKDIR: Creates directory SRC_URL
        -RMDIR: Removes directory SRC_URL
        -CP: start a copy of SRC_URL  to DST_URL, with identification TID,
            and associated with job JID.
        -FINALIZE: Finalizes the MAD.
    -JID: Is a job identifier, chosen by GridWay.
    -TID: Transfer identifier, only relevant for command CP.
    -EXE_MODE: If equal to 'X' file will be given execution permissions,
        only relevant for command CP.

    The format to receive a response from the MAD, through its standard
    output, is:

    OPERATION JID TID RESULT INFO

    Where:

    -OPERATION: Is the operation specified in the request that originated
        the response or CALLBACK, in the case of an asynchronous notification
        of a state change.
    -JID: It is the job identifier, as provided in the START request.
    -TID: It is the transfer identifier, as provided in the CP request.
    -RESULT: It is the result of the operation. Could be SUCCESS or FAILURE.
    -INFO: If RESULT is FAILURE, it contains the cause of failure.

    """

    logger  = logging.getLogger(__name__)
    message = Send()

    def __init__(self):
        self._max_thread   = 30
        self._min_thread   = 5
        self._lock         = threading.Lock()
        self._communicator = dict()
        self._configure    = None

    def do_INIT(self, args):
        """
        INIT: Initializes the MAD, JID should be max number of jobs.
        (i.e. INIT JID - - - -)
        @param args : arguments of operation
        @type args : string
        """
        out = 'INIT - - SUCCESS -'
        self.message.stdout( out )
        self.logger.debug( out )

    def do_START(self, args):
        """
        START: Init transfer associated with job JID.(i.e. START JID - - - -)
        @param args : arguments of operation
        @type args : string
        """
        out = 'START %s - SUCCESS -' % ( args.split( ) [ 1 ] )
        self.message.stdout( out )
        self.logger.debug( out )

    def do_END(self, args):
        """
        END: Finish transfer associated with job JID .(i.e. END JID - - - -)
        @param args : arguments of operation
        @type args : string
        """
        out = 'END %s - SUCCESS -' % ( args.split( ) [ 1 ] )
        self.message.stdout( out )
        self.logger.debug( out )

    def do_FINALIZE(self, args):
        """
        Finalizes the MAD (i.e. FINALIZE - - - - -)
        @param args : arguments of operation
        @type args : string
        """
        out = 'FINALIZE %s - SUCCESS -' % ( args.split( ) [ 1 ] )
        self.message.stdout( out )
        self.logger.debug( out )
        sys.exit( 0 )

    def do_MKDIR(self, args):
        """
        MKDIR: Creates directory SRC_URL (i.e. MKDIR JID - - SRC_URL -)
        @param args : arguments of operation
        @type args : string
        """
        OPERATION, JID, TID, EXE_MODE, SRC_URL, DST_URL = args.split()
        try:
            com = self._update_com( urlparse( SRC_URL ).host )
            com.rmDirectory( SRC_URL )
            com.mkDirectory( SRC_URL )
            out = 'MKDIR %s - SUCCESS -' % ( JID )
        except Exception as err :
            out = 'MKDIR %s - FAILURE %s' % ( JID , str( err ) )
        self.message.stdout( out )
        self.logger.debug( out , exc_info=1 )

    def do_RMDIR(self, args):
        """
        RMDIR: Removes directory SRC_URL (i.e. RMDIR JID - - SRC_URL -)
        @param args : arguments of operation
        @type args : string
        """
        OPERATION, JID, TID, EXE_MODE, SRC_URL, DST_URL = args.split()
        try:
            com = self._update_com( urlparse( SRC_URL ).host )
            com.rmDirectory( SRC_URL )
            out = 'RMDIR %s - SUCCESS -' % ( JID )
        except Exception as err :
            out = 'RMDIR %s - FAILURE %s' % ( JID , str( err ) )
        self.message.stdout( out )
        self.logger.debug( out, exc_info=1 )

    def do_CP(self, args):
        """
        CP: start a copy of SRC_URL  to DST_URL, with identification TID,
        and associated with job JID.(i.e. CP JID TID - SRC_URL DST_URL)
        @param args : arguments of operation
        @type args : string
        """
        OPERATION, JID, TID, EXE_MODE, SRC_URL, DST_URL = args.split()
        if 'file:' in SRC_URL:
            url = DST_URL
        else:
            url = SRC_URL
        try:
            com = self._update_com( urlparse( url ).host )
            com.copy( SRC_URL , DST_URL , EXE_MODE )
            out = 'CP %s %s SUCCESS -' % ( JID , TID )
        except Exception as err :
            out = 'CP %s %s FAILURE %s' % ( JID , TID , str( err ) )
        self.message.stdout( out )
        self.logger.debug(out , exc_info=1 )

    methods = {'INIT'    : do_INIT,
               'START'   : do_START,
               'END'     : do_END,
               'MKDIR'   : do_MKDIR,
               'RMDIR'   : do_RMDIR,
               'CP'      : do_CP,
               'FINALIZE': do_FINALIZE}

    def processLine(self):
        """
        Choose the OPERATION through the command line
        """
        try:
            pool = ThreadPool( self._min_thread , self._max_thread )
            self._configure = Configuration()
            while True:
                input = sys.stdin.readline().split()
                self.logger.debug(' '.join(input))
                OPERATION = input[0].upper()
                if len(input) == 6 and OPERATION in self.methods:
                    if OPERATION == 'FINALIZE' or OPERATION == 'INIT':
                        self.methods[OPERATION](self, ' '.join(input))
                    else: pool.add_task(self.methods[OPERATION], self,' '.join(input))
                else:
                    out = 'WRONG COMMAND'
                    self.message.stdout(out)
                    self.logger.debug(out)
        except Exception as err :
            self.logger.warning( str ( err ) , exc_info=1 )

    def _update_com(self, host):
        with self._lock :
            self.logger.debug( "_update_com function - It's looking for a communicator for the host: %s" % host )
            if not self._configure:
                self._configure = Configuration()
            '''
            if self._configure.check_update() or not self._configure.resources :
                self._configure.load()
                errors = self._configure.check()
                if errors :
                    self.logger.error ( ' '.join( errors ) )
                    raise Exception ( ' '.join( errors ) )
            '''
            #This needs to be optimized
            self._configure.load()
            errors = self._configure.check()
            if errors :
                self.logger.error ( ' '.join( errors ) )
                raise Exception ( ' '.join( errors ) )
            for resname, resdict in list(self._configure.resources.items()) :
                self.logger.debug( "    The current resource to which it's being compared is %s" % resname )
                if 'cloud_provider' in self._configure.resources[ resname ].keys():
                    continue
                elif '::' in host :
                    _resname , _ = host.split( '::' )
                    if resname != _resname :
                        continue
                elif resname != host :
                    continue
                elif resname not in self._communicator:
                    self.logger.debug( "    Since they are the same, its Communicator() will be returned")
                    self._communicator[ resname ] = self._configure.make_communicators()[resname]
                    if resdict[ 'communicator' ] == 'op_ssh' :
                        self._communicator[ resname ].configfile=os.path.join(DRM4G_DIR,'etc','openssh_tm.conf')
                        self._communicator[ resname ].parent_module='tm'
                self.logger.debug( "\nCommunicator for %s:\n    communicator: %s\n    username: %s\n    frontend: %s" % (resname, resdict[ 'communicator' ], self._communicator[ resname ].username, self._communicator[ resname ].frontend) )
                return self._communicator[ resname ]
