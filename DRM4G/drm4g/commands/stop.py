"""
Stop DRM4G daemon and ssh-agent. 
    
Usage: 
    drm4g stop [ options ] 
   
Options:
   --dbg    Debug mode.
"""
__version__  = '2.3.1'
__author__   = 'Carlos Blanco'
__revision__ = "$Id$"

import logging
from drm4g                import logger
from drm4g.commands       import Daemon, Agent

def run( arg ) :
    try:
        if arg[ '--dbg' ] :
            logger.setLevel(logging.DEBUG)
        Daemon().stop()
        Agent().stop()
    except Exception , err :
        logger.error( str( err ) )
