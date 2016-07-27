"""
Check DRM4G daemon and ssh-agent. 
    
Usage: 
    drm4g status [ options ] 
   
Options:
   --dbg    Debug mode.
"""
__version__  = '2.4.1'
__author__   = 'Carlos Blanco'
__revision__ = "$Id$"

import logging
from drm4g.commands       import Daemon, Agent, logger

def run( arg ) :
    try:
        if arg[ '--dbg' ] :
            logger.setLevel(logging.DEBUG)
        Daemon().status()
        Agent().status()
    except Exception as err :
        logger.error( str( err ) )

