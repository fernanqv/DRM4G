#!/usr/bin/env python

__version__  = '2.4.1'
__author__   = 'Carlos Blanco'
__revision__ = "$Id$"

from drm4g_env import *
from drm4g.core.em_mad import GwEmMad

def main():
    parser = OptionParser(description = 'Execution manager MAD',
            prog = 'gw_em_mad_drm4g.py', version = '0.1',
            usage = 'Usage: %prog')
    options, args = parser.parse_args()
    try:
        GwEmMad().processLine()
    except exceptions.KeyboardInterrupt as e:
        sys.exit(-1)
    except Exception as e:
        traceback.print_exc(file=sys.stdout)
        exit( 'Caught exception: %s: %s' % (e.__class__, str(e)) )


if __name__ == '__main__':
    main()
