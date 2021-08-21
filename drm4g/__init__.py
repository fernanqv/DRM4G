#
# Copyright 2021 Santander Meteorology Group (UC-CSIC)
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

from ._version import __version__
del _version

#__version__  = '2.6.15'
__author__   = 'Valvanuz Fernández-Quiruelas, Markel Garcia, Carlos Blanco, Antonio Minondo and Antonio S. Cofino (@cofinoa)'

import sys
import os
import logging.config
import logging
from os.path import dirname , join , expanduser , exists , abspath
from  shutil import copytree

if sys.version_info<(3,5):
    exit( 'The version number of Python has to be >= 3.5' )

##############################################
# Default values used in DRM4G ENV variables #
##############################################
HOME                 = expanduser(os.environ.get( 'HOME', '~' ))
DRM4G_DIR            = expanduser(os.environ.get( 'DRM4G_DIR'           , join( HOME     , '.drm4g' )))
DRM4G_DIR_VAR        = expanduser(os.environ.get( 'DRM4G_DIR_VAR'       , join( DRM4G_DIR, 'var' )))
DRM4G_DIR_ETC        = expanduser(os.environ.get( 'DRM4G_DIR_ETC'       , join( DRM4G_DIR, 'etc' )))
DRM4G_RESOURCES_CONF = expanduser(os.environ.get( 'DRM4G_RESOURCES_CONF', join( DRM4G_DIR_ETC, 'resources.conf' )))
DRM4G_LOGGER_CONF    = expanduser(os.environ.get( 'DRM4G_LOGGER_CONF'   , join( DRM4G_DIR_ETC, 'logger.conf')))
DRM4G_GWD_CONF       = expanduser(os.environ.get( 'DRM4G_GWD_CONF'      , join( DRM4G_DIR_ETC, 'gwd.conf')))
DRM4G_SCHED_CONF     = expanduser(os.environ.get( 'DRM4G_SCHED_CONF'    , join( DRM4G_DIR_ETC, 'sched.conf')))

#ENV variable shouldn't be used outside of GWD and its components. 
os.environ[ 'GW_LOCATION' ] = DRM4G_DIR

#Populate and prepare DRM4G_DIR
if exists( DRM4G_DIR ) is False  :
    logging.warning( "Creating a DRM4G local configuration in '%s'" %  DRM4G_DIR )
    acct_dir = join ( DRM4G_DIR_VAR, 'acct' )
    logging.warning( "Creating '%s' directory" % acct_dir )
    os.makedirs( acct_dir )
    src_dir  = join ( abspath( dirname( __file__ ) ), 'conf' )
    logging.warning( "Copying from '%s' to '%s'" % ( src_dir , DRM4G_DIR_ETC ) )
    copytree( src_dir , DRM4G_DIR_ETC )

##
# Configure logger
##
logging.config.fileConfig(DRM4G_LOGGER_CONF, {"DRM4G_DIR": DRM4G_DIR})
console_logger = logging.getLogger('console')

REMOTE_VOS_DIR  = join( DRM4G_DIR , 'security') #TODO: TO BE DEPRECATED

COMMUNICATORS = {
    "ssh"          : "drm4g.communicators.ssh",
    "ssh_fabric"   : "drm4g.communicators.ssh_fabric",
    "pk_ssh"       : "drm4g.communicators.ssh",
    "op_ssh"       : "drm4g.communicators.openssh",
    "local"        : "drm4g.communicators.local",
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
    #"rocci"        : "drm4g.managers.rocci",
}


