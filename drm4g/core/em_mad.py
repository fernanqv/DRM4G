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
import re
import time
import threading
import logging
from os.path                 import join, dirname
from string                  import Template
try :
    from Queue               import Queue
except :
    from queue               import Queue
from drm4g                   import REMOTE_JOBS_DIR, DRM4G_DIR
from drm4g.utils.rsl2        import Rsl2Parser
from drm4g.utils.list        import List
from drm4g.core.configure    import Configuration
from drm4g.utils.dynamic     import ThreadPool
from drm4g.utils.message     import Send


class GwEmMad (object):
    """
    Execution manager MAD

    GridWay uses a Middleware Access Driver (MAD) module to submit,
    control and monitor the execution of jobs.

    The format to send a request to the Execution MAD, through its
    standard input, is:
    OPERATION JID HOST/JM RSL

    Where:

    -OPERATION: Can be one of the following:
        -INIT: Initializes the MAD (i.e. INIT - - -).
        -SUBMIT: Submits a job(i.e. SUBMIT JID HOST/JM RSL).
        -POLL: Polls a job to obtain its state (i.e. POLL JID - -).
    -CANCEL: Cancels a job (i.e. CANCEL JID - -).
    -FINALIZE:Finalizes the MAD (i.e. FINALIZE - - -).
    -JID: Is a job identifier, chosen by GridWay.
    -HOST: If the operation is SUBMIT, it specifies the resource contact
        to submit the job. Otherwise it is ignored.
    -JM: If the operation is SUBMIT, it specifies the job manager to submit
        the job. Otherwise it is ignored.
    -RSL: If the operation is SUBMIT, it specifies the resource specification
        to submit the job. Otherwise it is ignored.

    The format to receive a response from the MAD, through its standard output, is:

    OPERATION JID RESULT INFO

         Where:

    -OPERATION: Is the operation specified in the request that originated
        the response or CALLBACK, in the case of an asynchronous notification
        of a state change.
    -JID: It is the job identifier, as provided in the submission request.
    -RESULT: It is the result of the operation. Could be SUCCESS or FAILURE
    -INFO: If RESULT is FAILURE, it contains the cause of failure. Otherwise,
        if OPERATION is POLL or CALLBACK,it contains the state of the job.
    """
    logger  = logging.getLogger(__name__)
    message = Send()

    def __init__(self):
        self._callback_interval = 30 #seconds
        self._max_thread        = 10
        self._min_thread        = 3
        self._job_list          = List()
        self._configure         = None
        self._communicators     = dict()
        self._lock              = threading.Lock()

    def do_INIT(self, args):
        """
        Initializes the MAD (i.e. INIT - - -)
        @param args : arguments of operation
        @type args : string
        """
        out = 'INIT - SUCCESS -'
        self.message.stdout( out )
        self.logger.debug( out )

    def do_SUBMIT(self, args):
        """
        Submits a job(i.e. SUBMIT JID HOST/JM RSL).
        @param args : arguments of operation
        @type args : string
        """
        OPERATION, JID, HOST_JM, RSL = args.split()
        try:
            HOST, JM = HOST_JM.rsplit( '/', 1 )
            # Init Job class
            job, communicator = self._update_resource( HOST )
            job.Communicator  = communicator
            # Parse rsl
            rsl                = Rsl2Parser(RSL).parser()
            if 'project' in job.resfeatures :
                rsl['project']      = job.resfeatures[ 'project' ]
            if 'parallel_env' in job.resfeatures :
                rsl['parallel_env'] = job.resfeatures[ 'parallel_env' ]
            if 'vo' in job.resfeatures and "::" in HOST :
                _ , host                    = HOST.split('::')
                job.resfeatures['host']     = host
                job.resfeatures['jm']       = JM
                job.resfeatures['env_file'] = join( dirname(RSL), "job.env" )
                job.resfeatures['queue']    = rsl[ 'queue' ]
            # Update remote directories
            ABS_REMOTE_JOBS_DIR = job.get_abs_directory( job.resfeatures.get( 'scratch', REMOTE_JOBS_DIR ) )
            for key in [ "stdout" , "stderr" , "executable" ] :
                rsl[key] = join( ABS_REMOTE_JOBS_DIR , rsl[key] )
            # Create and copy wrapper_drm4g file
            local_file    = join( dirname ( RSL ), "wrapper_drm4g.%s" % RSL.split( '.' )[ -1 ] )
            remote_file   = join( dirname ( rsl[ 'executable' ] ), 'wrapper_drm4g' )
            job.createWrapper( local_file, job.jobTemplate( rsl ) )
            job.copyWrapper( local_file, remote_file )
            # Execute wrapper_drm4g
            job.JobId = job.jobSubmit( remote_file )
            self._job_list.put( JID, job )
            out = 'SUBMIT %s SUCCESS %s:%s' % ( JID, HOST, job.JobId )
        except Exception as err:
            out = 'SUBMIT %s FAILURE %s' % ( JID, str( err ) )
        self.message.stdout(out)
        self.logger.debug(out, exc_info=1 )

    def do_FINALIZE(self, args):
        """
        Finalizes the MAD (i.e. FINALIZE - - -).
        @param args : arguments of operation
        @type args : string
        """
        out = 'FINALIZE - SUCCESS -'
        self.message.stdout( out )
        self.logger.debug( out )
        sys.exit( 0 )

    def do_POLL(self, args):
        """
        Polls a job to obtain its state (i.e. POLL JID - -).
        @param args : arguments of operation
        @type args : string
        """
        OPERATION, JID, HOST_JM, RSL = args.split()
        try:
            if self._job_list.has_key( JID ) :
                job    = self._job_list.get( JID )
                status = job.getStatus( )
                out = 'POLL %s SUCCESS %s' % ( JID, status )
            else:
                out = 'POLL %s FAILURE Job not submitted' % ( JID )
        except Exception as err:
            out = 'POLL %s FAILURE %s' % ( JID, str( err ) )
        self.message.stdout( out )
        self.logger.debug( out )

    def do_RECOVER(self, args):
        """
        Polls a job to obtain its state (i.e. RECOVER JID - -).
        @param args : arguments of operation
        @type args : string
        """
        OPERATION, JID, HOST_JM, RSL = args.split()
        try:
            HOST, remote_job_id = HOST_JM.split( ':', 1 )
            job , communicator  = self._update_resource( HOST )
            job.Communicator    = communicator
            job.JobId           = remote_job_id
            job.refreshJobStatus( )
            self._job_list.put( JID, job )
            out = 'RECOVER %s SUCCESS %s' % ( JID, job.getStatus( ) )
        except Exception as err:
            out = 'RECOVER %s FAILURE %s' % ( JID, str( err ) )
        self.message.stdout( out )
        self.logger.debug( out, exc_info=1 )

    def do_CALLBACK(self):
        """
        Show the state of the job
        """
        while True:
            time.sleep( self._callback_interval )
            self.logger.debug( "CALLBACK new iteration ..." )
            for JID , job in self._job_list.items( ):
                try:
                    self.logger.debug( "CALLBACK checking job '%s'" % JID  )
                    oldStatus = job.getStatus( )
                    job.refreshJobStatus( )
                    newStatus = job.getStatus( )
                    if oldStatus != newStatus:
                        if newStatus == 'DONE' or newStatus == 'FAILED':
                            self._job_list.delete(JID)
                            time.sleep ( 0.1 )
                        out = 'CALLBACK %s SUCCESS %s' % ( JID, newStatus )
                        self.message.stdout( out )
                        self.logger.debug( out )
                except Exception as err:
                    out = 'CALLBACK %s FAILURE %s' % ( JID, str( err ) )
                    self.message.stdout( out )
                    self.logger.debug( out, exc_info=1 )

    def do_CANCEL(self, args):
        """
        Cancels a job (i.e. CANCEL JID - -).
        @param args : arguments of operation
        @type args : string
        """
        OPERATION, JID, HOST_JM, RSL = args.split()
        try:
            if self._job_list.has_key( JID ) :
                self._job_list.get(JID).jobCancel()
                out = 'CANCEL %s SUCCESS -' % (JID)
            else:
                out = 'CANCEL %s FAILURE Job not submitted' % (JID)
        except Exception as e:
            out = 'CANCEL %s FAILURE %s' % ( JID, str(e) )
        self.message.stdout( out )
        self.logger.debug( out )

    methods = {'INIT'    : do_INIT,
               'SUBMIT'  : do_SUBMIT,
               'POLL'    : do_POLL,
               'RECOVER' : do_RECOVER,
               'CANCEL'  : do_CANCEL,
               'FINALIZE': do_FINALIZE}

    def processLine(self):
        """
        Choose the OPERATION through the command line
        """
        try:
            worker = threading.Thread( target = self.do_CALLBACK, )
            worker.setDaemon( True )
            worker.start()
            self._configure = Configuration()
            pool = ThreadPool( self._min_thread, self._max_thread )
            while True:
                input = sys.stdin.readline().split()
                self.logger.debug( ' '.join(input) )
                OPERATION = input[0].upper()
                if len(input) == 4 and OPERATION in self.methods:
                    if OPERATION in ( 'FINALIZE', 'INIT', 'SUBMIT', 'RECOVER' ):
                        self.methods[ OPERATION ]( self, ' '.join(input) )
                    else:
                        pool.add_task( self.methods[ OPERATION ], self, ' '.join(input) )
                else:
                    out = 'WRONG COMMAND'
                    self.message.stdout( out )
                    self.logger.debug( out )
        except Exception as err:
            self.logger.warning( str( err ) )

    def _update_resource(self, host):
        with self._lock :
            if not self._configure.check_update() or not self._configure.resources :
                self._configure.load()
                errors = self._configure.check()
                if errors :
                    self.logger.error ( ' '.join( errors ) )
                    raise Exception ( ' '.join( errors ) )
            for resname, resdict in list( self._configure.resources.items() ) :
                if  'cloud_provider' in self._configure.resources[ resname ].keys():
                    continue
                if '::' in host :
                    _resname , _ = host.split( '::' )
                    if resname != _resname :
                        continue
                elif resname != host :
                    continue
                if resname not in self._communicators :
                    self._communicators[ resname ] = self._configure.make_communicators()[resname]
                job          = self._configure.make_resources()[ resname ]['Job']
                communicator = self._communicators[ resname ]
                if resdict[ 'communicator' ] == 'op_ssh' :
                    communicator.configfile=join(DRM4G_DIR,'etc','openssh_em.conf')
                    communicator.parent_module='em'
                return job, communicator


