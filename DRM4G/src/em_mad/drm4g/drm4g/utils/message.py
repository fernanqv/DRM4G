import threading
import sys

__version__ = '2.0.0'
__author__  = 'Carlos Blanco'
__revision__ = "$Id$"

class Send (object):
    """
    This class provides basic messaging 
    """	
    def __init__(self):
        self._lock = threading.Lock()

    def stdout(self, message):
        self._lock.acquire()
        try:
            sys.stdout.write(message + '\n')
            sys.stdout.flush()
        finally:
            self._lock.release()

    def stderr(self, message):
        self._lock.acquire()
        try:
            sys.stderr.write(message + '\n')
            sys.stderr.flus()
        finally:
            self._lock.release()

            
