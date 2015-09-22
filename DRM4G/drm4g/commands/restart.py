"""
Restart DRM4G daemon. 
    
Usage: 
    drm4g [ options ] restart
   
Options:
   --dbg    Debug mode.
"""
__version__  = '2.3.1'
__author__   = 'Carlos Blanco'
__revision__ = "$Id$"

import logging
from time                 import sleep
from drm4g                import logger
from drm4g.commands       import Daemon, Agent

def run( arg ) :
    try:
        if arg[ '--dbg' ] :
            logger.setLevel(logging.DEBUG)
        daemon = Daemon()
        daemon.stop()
        sleep( 2.0 )
        daemon.start()
    except Exception as err :
        logger.error( str( err ) )

