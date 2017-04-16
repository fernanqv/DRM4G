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
   drm4g conf ( daemon | sched | cloud | logger ) [ options ]

Options:
   -d --debug    Debug mode
"""

import os
from drm4g  import DRM4G_DAEMON, DRM4G_LOGGER, DRM4G_CLOUD, DRM4G_SCHED, DRM4G_DIR, logger

def run( arg ) :
    if arg[ 'daemon' ] :
        conf_file = DRM4G_DAEMON
    elif arg[ 'logger' ]:
        conf_file = DRM4G_LOGGER
    elif arg[ 'cloud' ]:
        conf_file = DRM4G_CLOUD        
    else :
        conf_file = DRM4G_SCHED

    if os.path.exists( conf_file ): 
        logger.debug( "Editing '%s' file" % conf_file )
        os.system( "%s %s" % ( os.environ.get('EDITOR', 'nano') , conf_file ) )
    else:
        error_message = "The configuration file '%s' does not exist, please provide one\n" \
                        "    If you wish to restore your entire configuration folder you can run the command \033[93m'drm4g start --clear-conf'\033[0m, " \
                        "but bear in mind that this will overwrite or delete every configuration file in '%s'" % (conf_file, os.path.join(DRM4G_DIR, 'etc'))
        logger.error( error_message )
