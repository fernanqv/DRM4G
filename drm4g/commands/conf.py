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
Configure DRM4G's daemon, scheduler and logger parameters.

Usage:
   drm4g conf ( daemon | sched | logger ) [ options ]

Options:
   -d --debug    Debug mode
"""

import logging
import os
from drm4g  import DRM4G_DAEMON, DRM4G_LOGGER, DRM4G_SCHED, logger

def run( arg ) :
    if arg[ 'daemon' ] :
        conf_file = DRM4G_DAEMON
    elif arg[ 'logger' ]:
        conf_file = DRM4G_LOGGER
    else :
        conf_file = DRM4G_SCHED
    logger.debug( "Editing '%s' file" % conf_file )
    os.system( "%s %s" % ( os.environ.get('EDITOR', 'nano') , conf_file ) )
