"""
Manage computing resources on DRM4G.
    
Usage: 
    drm4g resource [ list | edit | check | create | destroy ] [ options ]
    
 Options:
    --dbg                   Debug mode.
    
Commands:
    list                    Show resources available.    
    edit                    Configure resouces.
    check                   Check out if configured resources are accessible.
    create
    destroy 
"""
__version__  = '2.4.1'
__author__   = 'Carlos Blanco'
__revision__ = "$Id$"

import logging
from drm4g                import logger
from drm4g.core.configure import Configuration
from drm4g.commands       import exec_cmd, Daemon, Resource

def run( arg ) :
    if arg[ '--dbg' ] :
        logger.setLevel(logging.DEBUG)
    try :
        config = Configuration()
        daemon = Daemon()
        if not daemon.is_alive() :
           raise Exception( 'DRM4G is stopped.' )
        resource = Resource( config )
        if arg[ 'edit' ] :
            resource.edit()
        elif arg[ 'check' ] :
            resource.check_frontends( )
        elif arg[ 'create' ] :
            resource.create_vms()
        elif arg[ 'destroy' ] :
            resource.destroy_vms( )
        else :
            resource.list()       
    except Exception as err :
        logger.error( str( err ) )

