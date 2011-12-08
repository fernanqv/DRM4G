import threading
from drm4g.utils.list import List 
from drm4g.utils.logger import *
from drm4g.utils.message import Send
from drm4g.global_settings import COMMUNICATOR, RESOURCE_MANAGER
from drm4g.utils.importlib import import_module
import drm4g.core.em_mad.GwEmMad

__version__ = '0.1'
__author__  = 'Carlos Blanco'
__revision__ = "$Id$"

class GwEmMad (drm4g.core.em_mad.GwEmMad):
    
    logger = get_logger('drm4g.core.em_cream')
    message = Send()

    def __init__(self):
        self._callback_interval = 200 #seconds
        self._max_thread        = 50
        self._min_thread        = 5
        self._JID_list          = List()
        self._resource_module 	= None
        self._com               = None
        self._pool_sema         = threading.Semaphore()
	        
    def do_INIT(self, args):
        """
        Initializes the MAD (i.e. INIT - - -)
        @param args : arguments of operation
        @type args : string
        """
        try:
            self._com = getattr(import_module(COMMUNICATOR['local']), 'Communicator')()
            self._resource_module = import_module(RESOURCE_MANAGER['cream'])
            out = 'INIT - SUCCESS -'
        except Exception, e:
            out = 'INIT - FAILURE %s' % (str(e))
        self.message.stdout(out)
        self.logger.log(DEBUG, '--> ' + out)
    
    def do_SUBMIT(self, args):
        """
        Submits a job(i.e. SUBMIT JID HOST/JM RSL).
        @param args : arguments of operation
        @type args : string
        """
        OPERATION, JID, HOST_JM, RSL = args.split()
        self._pool_sema.acquire()
        try:
            job = getattr(self._resource_module, 'Job')()
            job.Communicator = self._com
            job.jobSubmit(HOST_JM, RSL)
            self._JID_list.put(JID, job)
            out = 'SUBMIT %s SUCCESS %s' % (JID, job.JobId)
        except Exception, e:
            out = 'SUBMIT %s FAILURE %s' % (JID, str(e))
        finally:
            self._pool_sema.release()
        self.message.stdout(out)
        self.logger.log(DEBUG, '--> ' + out)
        
    def do_RECOVER(self, args):
        """
        Polls a job to obtain its state (i.e. RECOVER JID - -).
        @param args : arguments of operation
        @type args : string 
        """
        OPERATION, JID, HOST_JM, RSL = args.split()
        self._pool_sema.acquire()
        try:
            job = getattr(self._resource_module, 'Job')()
            job.Communicator = self._com
            job.JobId = HOST_JM
            job.refreshJobStatus()
            self._JID_list.put(JID, job)
            out = 'RECOVER %s SUCCESS %s' % (JID, job.Status)
        except Exception, e:
            out = 'RECOVER %s FAILURE %s' % (JID, str(e)) 
        finally:
            self._pool_sema.release()   
        self.message.stdout(out)
        self.logger.log(DEBUG, '--> ' + out)
       
    def do_CANCEL(self, args):
        """
        Cancels a job (i.e. CANCEL JID - -).
        @param args : arguments of operation
        @type args : string
        """
        OPERATION, JID, HOST_JM, RSL = args.split()
        self._pool_sema.acquire()
        try:
            if self._JID_list.has_key(JID):
                self._JID_list.get(JID).jobCancel()
                out = 'CANCEL %s SUCCESS -' % (JID)
            else:
                out = 'CANCEL %s FAILURE Job not submited' % (JID)
        except Exception, e:
            out = 'CANCEL %s FAILURE %s' % (JID, str(e))
        finally:
            self._pool_sema.release()
        self.message.stdout(out)
        self.logger.log(DEBUG, '--> ' + out)
 
