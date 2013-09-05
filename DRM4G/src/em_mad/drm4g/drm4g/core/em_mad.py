import sys
import re
import time
import threading
import logging
from os.path                 import join, dirname
from string                  import Template
from Queue                   import Queue
from drm4g                   import REMOTE_JOBS_DIR 
from drm4g.utils.rsl2        import Rsl2Parser
from drm4g.utils.list        import List 
from drm4g.core.configure    import Configuration
from drm4g.utils.dynamic     import ThreadPool
from drm4g.utils.message     import Send

__version__  = '1.0'
__author__   = 'Carlos Blanco'
__revision__ = "$Id$"

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
        self._max_thread        = 100
        self._min_thread        = 5
        self._job_list          = List()
        self._resources         = dict()
        self._job_obj           = dict()
        self._configure         = None 
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
            HOST, JM = HOST_JM.rsplit('/',1)
            # Parse rsl
            rsl                    = Rsl2Parser(RSL).parser()
            rsl['project']         = self._resources.get('project')
            rsl['parallel_env']    = self._resources.get('parallel_env')
            # Init Job class
            job = self._update_resource( HOST )
            if '_VO_' in HOST :
                host , job.resfeatures['vo'] = HOST.split('_VO_')
                job.resfeatures['jm']        = JM
                job.resfeatures['env_file']  = join( dirname(RSL) , "job.env" )
                job.resfeatures['queue']     = rsl[ 'queue' ]
            else :
                host = HOST
            # Update remote directories 
            ABS_REMOTE_JOBS_DIR   = job.get_abs_directory( REMOTE_JOBS_DIR )
            for key in [ "stdout" , "stderr" , "directory" , "executable" ] :
                rsl[key] = join( ABS_REMOTE_JOBS_DIR , rsl[key] )
            # Create and copy wrapper_drm4g file 
            local_file    = join ( RSL.rsplit( '/' , 1 )[ 0 ] , "wrapper_drm4g.%s" % RSL.split( '.' )[ -1 ] )
            remote_file   = join ( rsl[ 'directory' ] , 'wrapper_drm4g' )
            job.createWrapper( local_file , job.jobTemplate( rsl ) )
            job.copyWrapper( local_file , remote_file )
            # Execute wrapper_drm4g 
            job.JobId = job.jobSubmit( remote_file )
            self._job_list.put( JID , job )
            out = 'SUBMIT %s SUCCESS %s:%s' % ( JID , HOST , job.JobId )
        except Exception, err:
            import  traceback
            traceback.print_exc(file=sys.stdout)
            out = 'SUBMIT %s FAILURE %s' % ( JID , str( err ) )
        self.message.stdout(out)
        self.logger.debug(out)

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
                out = 'POLL %s SUCCESS %s' % ( JID , status )
            else:
                out = 'POLL %s FAILURE Job not submitted' % (JID) 
        except Exception, err:
            out = 'POLL %s FAILURE %s' % ( JID , str( err ) )
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
            HOST, remote_JobId = HOST_JM.split( ':' )
            job                = self._update_resource( HOST )            
            job.JobId          = remote_JobId
            job.refreshJobStatus( )
            self._job_list.put( JID , job )
            out = 'RECOVER %s SUCCESS %s' % ( JID, job.getStatus() )
        except Exception, err:
            out = 'RECOVER %s FAILURE %s' % (JID , str( err ) )    
        self.message.stdout(out)
        self.logger.debug(out)
            
    def do_CALLBACK(self):
        """
        Show the state of the job
        """
        while True:
            time.sleep( self._callback_interval )
            for JID, job  in self._job_list.items( ):
                try:
                    oldStatus = job.getStatus( )
                    job.refreshJobStatus( )
                    newStatus = job.getStatus( )
                    if oldStatus != newStatus:
                        if newStatus == 'DONE' or newStatus == 'FAILED': 
                            self._job_list.delete(JID)
                        time.sleep ( 0.2 )
                        out = 'CALLBACK %s SUCCESS %s' % ( JID , newStatus )
                        self.message.stdout( out )
                        self.logger.debug( out )
                except Exception, err:
                    out = 'CALLBACK %s FAILURE %s' % ( JID , str( err ) )
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
            if self._job_list.has_key(JID):
                self._job_list.get(JID).jobCancel()
                out = 'CANCEL %s SUCCESS -' % (JID)
            else:
                out = 'CANCEL %s FAILURE Job not submitted' % (JID)
        except Exception, e:
            out = 'CANCEL %s FAILURE %s' % (JID, str(e))    
        self.message.stdout(out)
        self.logger.debug(out)
        
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
            worker = threading.Thread(target = self.do_CALLBACK, )
            worker.setDaemon(True)
            worker.start()
            self._configure = Configuration()
            pool = ThreadPool(self._min_thread, self._max_thread)
            while True:
                input = sys.stdin.readline().split()
                self.logger.debug(' '.join(input))
                OPERATION = input[0].upper()
                if len(input) == 4 and self.methods.has_key(OPERATION):
                    if OPERATION == 'FINALIZE' or OPERATION == 'INIT' \
                        or OPERATION == 'RECOVER':
                        self.methods[OPERATION](self, ' '.join(input))
                    else:
                        pool.add_task(self.methods[OPERATION], self, ' '.join(input))    
                else:
                    out = 'WRONG COMMAND'
                    self.message.stdout(out)
                    self.logger.debug(out)
        except Exception, err:
            self.logger.warning( str( err ) )
    
    def _update_resource(self, host):
        with self._lock :
            if not self._job_obj.has_key( host ) or self._configure.check_update() : 
                self._configure.load()
                errors = self._configure.check()
                if errors :
                    self.logger.error ( ' '.join( errors ) )
                    raise Exception ( ' '.join( errors ) )
                for resname, resdict in self._configure.resources.iteritems() :
                    if '_VO_' in host :
                        _ , vo = host.split( '_VO_' )
                        if self._configure.resources[resname][ 'vo' ] != vo :
                            continue
                    if resname != host : 
                            continue
                    if not self._job_obj.has_key( host ) :
                        self._resources[resname] = self._configure.resources[resname]
                        self._job_obj[host]      = self._configure.make_resources()[resname]['Job']
                        return self._job_obj[host]
                    for key in ['communicator' , 'username', 'frontend' , 'public_key', 
                                'lrms' , 'parallel_env' , 'project' , 'vo' , 'ldap' , 'myproxy_server' ] :
                        try :
                            if self._resources[resname][key] != self._configure.resources[resname][key] :
                                self._job_obj[ host ].Communicator.close()
                                self._com_obj[ host ] = self._configure.make_resources()[resname]['Job']
                                return self._job_obj[ host ]
                        except Exception:
                            pass
                    return self._job_obj[ host ]
            else :
                return self._job_obj[ host ]

