import sys
import os
import threading
import logging
from drm4g.core.configure  import Configuration
from drm4g.managers        import HostInformation
from drm4g.utils.dynamic   import ThreadPool
from drm4g.utils.message   import Send

__version__  = '1.0'
__author__   = 'Carlos Blanco'
__revision__ = "$Id$"

class GwImMad (object):
    """
    Information manager MAD 
	
	The format to send a request to the Information MAD, through its standard input, is: 
	
		OPERATION HID HOST ARGS
		
	Where:
	-OPERATION: Can be one of the following:
		-INIT: Initializes the MAD (i.e. INIT - - -).
		-DISCOVER: Discovers hosts (i.e. DISCOVER - - - ).
		-MONITOR: Monitors a host (i.e. MONITOR HID HOST -).
		-FINALIZE: Finalizes the MAD (i.e. FINALIZE - - -).
	-HID: if the operation is MONITOR, it is a host identifier, chosen by GridWay. Otherwise it is ignored.
	-HOST: If the operation is MONITOR it specifies the host to monitor. Otherwise it is ignored.
			
	The format to receive a response from the MAD, through its standard output, is:
	    
	    OPERATION HID RESULT INFO
	    
	Where:
	-OPERATION: Is the operation specified in the request that originated the response.
	-HID: It is the host identifier, as provided in the submission request.
	-RESULT: It is the result of the operation. Could be SUCCESS or FAILURE.
	-INFO: If RESULT is FAILURE, it contains the cause of failure. Otherwise, if OPERATION 
		is   DISCOVER, it contains a list of discovered host, or if OPERATION is MONITOR, 
		it contains a list of host attributes.
    """

    logger  = logging.getLogger(__name__)
    message = Send()
    
    def __init__(self):
        self._min_thread = 4
        self._max_thread = 10
        self._resources  = dict()
 
    def do_INIT(self, args):
        """
        Initializes the MAD (i.e. INIT - - -)
        @param args : arguments of operation
        @type args : string
        """
        out = 'INIT - SUCCESS -'
        self.message.stdout(out)
        self.logger.debug(out)
        
    def do_DISCOVER(self, args):
        """
        Discovers hosts (i.e. DISCOVER - - -)
        @param args : arguments of operation
        @type args : string
        """
        OPERATION, HID, HOST, ARGS = args.split()
        try:
            config = Configuration()
            config.load()
            errors = config.check()
            assert not errors, ' '.join( errors )
            self._resources = config.make_resources()
            hosts = ""
            for resname, resdict in self._resources.iteritems() :
                hosts += " " + resdict['Resource'].hosts() 
            out = 'DISCOVER %s SUCCESS %s' % ( HID , hosts  )
        except Exception , err :
            out = 'DISCOVER - FAILURE %s' % str( err )
        self.message.stdout( out )
        self.logger.debug( out )
 
    def do_MONITOR(self, args):
        """
        Monitors a host (i.e. MONITOR HID HOST -)
        @param args : arguments of operation
        @type args : string
        """
        OPERATION, HID, HOST, ARGS = args.split()
        try:
            info = ""
            for resname, resdict in self._resources.iteritems() :
                if HOST in resdict['Resource'].host_list :
                    info = resdict['Resource'].host_properties( HOST )
                    break
            assert info, "Host '%s' is not avaible" % HOST
            out = 'MONITOR %s SUCCESS %s' % (HID , info )
        except Exception , err :
            out = 'MONITOR %s FAILURE %s' % (HID , str( err ) )
        self.message.stdout(out)
        self.logger.debug(out)
 
    def do_FINALIZE(self, args):
        """
        Finalizes the MAD (i.e. FINALIZE - - -)
        @param args : arguments of operation
        @type args : string
        """
        out = 'FINALIZE - SUCCESS -'
        self.message.stdout(out)
        self.logger.debug(out)
        sys.exit(0)
        
    methods = { 'INIT'	  : do_INIT,
                'DISCOVER': do_DISCOVER,
                'MONITOR' : do_MONITOR,
                'FINALIZE': do_FINALIZE,
                }
                
    def processLine(self):
        """
        Choose the OPERATION through the command line
        """
        try:
            pool = ThreadPool(self._min_thread, self._max_thread)
            while True:
                input = sys.stdin.readline().split()
                self.logger.debug(' '.join(input))
                OPERATION = input[0].upper()
                if len(input) == 4 and self.methods.has_key(OPERATION):
                    if OPERATION != 'MONITOR':
                        self.methods[OPERATION](self, ' '.join(input))
                    else:
                        pool.add_task(self.methods[OPERATION], self, ' '.join(input))
                else:
                    out = 'WRONG COMMAND'
                    self.message.stdout(out)
                    self.logger.debug(out)
        except Exception, e:
            self.logger.warning(str(e))
            
