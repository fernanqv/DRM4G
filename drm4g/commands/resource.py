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
__version__  = '2.5.0-beta'
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

