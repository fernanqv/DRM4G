#!/usr/bin/env python

import sys
from logging.config import fileConfig
from os.path        import dirname, join , abspath

try:
    sys.path.insert( 0 , join( dirname( dirname( abspath( __file__ ) ) ) , 'libexec' ) )
    from drm4g import FILE_LOGGER, DRM4G_DIR  
    try:
        fileConfig( FILE_LOGGER , { "DRM4G_DIR": DRM4G_DIR } )
    except :
        pass
except Exception , err :
    print 'Caught exception: %s' % str( e )
    sys.exit( -1 )

from drm4g.commands import management

if __name__ == "__main__":
    management.execute_from_command_line( sys.argv )

