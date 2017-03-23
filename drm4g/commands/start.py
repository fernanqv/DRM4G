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
Start DRM4G's daemon and ssh-agent.

Usage:
    drm4g start [ options ]

Options:
   -d --debug    Debug mode.
   --clear-conf  Clear the DRM4G's settings stored in .drm4g directory.
"""

from os                   import makedirs
from os.path              import exists, join, abspath, dirname
from drm4g                import logger, DRM4G_DIR
from drm4g.commands       import Daemon #, Agent

def run( arg ) :
    try:
        if arg[ '--clear-conf' ] :
            from shutil import copytree, rmtree
            if exists( DRM4G_DIR ) :
                logger.debug( "Removing DRM4G local configuration in '%s'" %  DRM4G_DIR )
                rmtree( DRM4G_DIR )
            logger.debug( "Creating a DRM4G local configuration in '%s'" %  DRM4G_DIR )
            abs_dir = join ( DRM4G_DIR , 'var' , 'acct' )
            logger.info( "Creating '%s' directory" % abs_dir )
            makedirs( abs_dir )
            src  = abspath( join ( abspath( dirname( __file__ ) ), '..', 'conf' ) )
            dest = join ( DRM4G_DIR, 'etc' )
            logger.info( "Coping from '%s' to '%s'" % ( src , dest ) )
            copytree( src , dest )
            Daemon().start()
        else:
            Daemon().start()
        #Agent().start()
    except Exception as err :
        logger.error( str( err ) )

