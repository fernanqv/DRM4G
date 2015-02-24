"""
Start DRM4G daemon and ssh-agent. 
    
Usage: 
    drm4g start [ --dbg ] 
   
Options:
   --dbg    Debug mode.
"""
__version__  = '2.3.0'
__author__   = 'Carlos Blanco'
__revision__ = "$Id$"

import logging
from drm4g.commands       import Daemon, Agent, logger

def run( arg ) :
    try:
        if arg[ '--dbg' ] :
            logger.setLevel(logging.DEBUG)
        Daemon().start()
        Agent().start()
    except Exception , err :
        logger.error( str( err ) )

