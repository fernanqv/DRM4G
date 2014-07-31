#!/usr/bin/env python

__version__  = '2.1.0'
__author__   = 'Carlos Blanco'
__revision__ = "$Id$"

import sys
from os.path import dirname, join , abspath
sys.path.insert( 0 , join( dirname( dirname( abspath( __file__ ) ) ) , 'libexec' ) )

from drm4g.commands import execute_from_command_line

if __name__ == "__main__":
    try :
        execute_from_command_line( sys.argv )
    except Exception , err :
        exit( err )

