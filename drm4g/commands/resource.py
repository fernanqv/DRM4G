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
    drm4g resource [ list [ --all ] | edit | check | create | destroy ] [ options ]

 Options:
    -d --debug              Debug mode.
    --all                   Lists all of the created resources.

Commands:
    list                    Show resources available.
    edit                    Configure resouces.
    check                   Check if configured resources are accessible.
    create                  Create new virtual machines
    destroy                 Delete all virtual machines
"""

import logging
from drm4g                import logger
from drm4g.core.configure import Configuration
from drm4g.commands       import exec_cmd, Daemon, Resource

def run( arg ) :
    try :
        config = Configuration()
        resource = Resource( config )
        if arg[ 'edit' ] :
            resource.edit()
        else :
            daemon = Daemon()
            if not daemon.is_alive() :
               raise Exception( 'DRM4G is stopped.' )

            elif arg[ 'check' ] :
                resource.check_frontends( )
            elif arg[ 'create' ] :
                resource.create_vms()
            elif arg[ 'destroy' ] :
                resource.destroy_vms( )
            elif arg[ '--all' ] :
                resource.list_resources( )
            else :
                resource.list()
    except Exception as err :
        logger.error( str( err ) )

