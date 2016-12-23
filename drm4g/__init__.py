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

__all__ = ["communicators", "core", "managers", "utils", "commands", "api"]

__version__  = '2.6.1'
__author__   = 'Carlos Blanco and Antonio Minondo'
__revision__ = "$Id$"

import sys
import os
import logging.config
from os.path import dirname , join , expandvars , exists , abspath

if (sys.version_info[0]==2 and sys.version_info<=(2,5)) or (sys.version_info[0]==3 and sys.version_info<(3,3)):
    exit( 'The version number of Python has to be > = 2.6 and < = 3.3' )

########################################
# Default values used in DRM4G package.#
########################################
HOME              = os.environ.get( 'HOME' )
DRM4G_DIR         = os.environ[ 'GW_LOCATION' ] = join ( os.environ.get( 'DRM4G_DIR' , HOME ), '.drm4g' )
DRM4G_CONFIG_FILE = join( DRM4G_DIR , 'etc' , 'resources.conf' )
DRM4G_LOGGER      = join( DRM4G_DIR , 'etc' , 'logger.conf')
DRM4G_DAEMON      = join( DRM4G_DIR , 'etc' , 'gwd.conf')
DRM4G_SCHED       = join( DRM4G_DIR , 'etc' , 'sched.conf')

##
# Configure logger
##
logging.basicConfig( format='%(message)s', level = logging.INFO , stream = sys.stdout )
logger = logging.getLogger(__name__)

if exists( DRM4G_DIR ) is False  :
    logger.info( "Creating a DRM4G local configuration in '%s'" %  DRM4G_DIR )
    abs_dir = join ( DRM4G_DIR , 'var' , 'acct' )
    logger.info( "Creating '%s' directory" % abs_dir )
    os.makedirs( abs_dir )
    from  shutil import copytree
    src  = join ( abspath( dirname( __file__ ) ), 'conf' )
    dest = join ( DRM4G_DIR, 'etc' )
    logger.info( "Coping from '%s' to '%s'" % ( src , dest ) )
    copytree( src , dest )

REMOTE_JOBS_DIR = "~/.drm4g/jobs"
REMOTE_VOS_DIR  = "~/.drm4g/security"

# ssh communicator
SSH_PORT            = 22
SSH_CONNECT_TIMEOUT = 60 # seconds
SFTP_CONNECTIONS    = 3

# Proxy
PROXY_THRESHOLD     = 178 # Proxy threshold in hours.

COMMUNICATORS = {
                 "pk_ssh"    : "drm4g.communicators.ssh",
                 "op_ssh" : "drm4g.communicators.openssh",
                 "local"  : "drm4g.communicators.local",
                 }
RESOURCE_MANAGERS = {
                     "pbs"          : "drm4g.managers.pbs",
                     "sge"          : "drm4g.managers.sge",
                     "fork"         : "drm4g.managers.fork",
                     "none"         : "drm4g.managers.fork",
                     "lsf"          : "drm4g.managers.lsf",
                     "loadleveler"  : "drm4g.managers.loadleveler",
                     "cream"        : "drm4g.managers.cream",
                     "slurm"        : "drm4g.managers.slurm",
                     "mnslurm"      : "drm4g.managers.marenostrum",
                     "slurm_res"    : "drm4g.managers.slurm_res",
                     "neptuno"      : "drm4g.managers.neptuno",
                     "rocci"        : "drm4g.managers.rocci",
                     }


