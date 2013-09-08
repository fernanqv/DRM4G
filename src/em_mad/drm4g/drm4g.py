#!/usr/bin/env python

import sys
import logging
from os.path        import dirname, join , abspath

try:
    sys.path.insert( 0 , join( dirname( dirname( abspath( __file__ ) ) ) , 'libexec' ) )
    from drm4g import FILE_LOGGER, DRM4G_DIR  
    try:
        logging.basicConfig( format='%(levelname)s:%(message)s', level=logging.DEBUG )
    except :
        pass
except Exception , err :
    print 'Caught exception: %s' % str( e )
    sys.exit( -1 )

from drm4g.commands import execute_from_command_line

if __name__ == "__main__":
    execute_from_command_line( sys.argv )

