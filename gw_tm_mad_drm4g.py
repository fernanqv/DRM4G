#!/usr/bin/env python

from drm4g_env import *
from drm4g.core.tm_mad import GwTmMad

def main():
    parser = OptionParser(description = 'Transfer manager MAD',
            prog = 'gw_tm_mad_drm4g.py', version = '0.1',
            usage = 'Usage: %prog')
    options, args = parser.parse_args()
    try:
        GwTmMad().processLine()
    except exceptions.KeyboardInterrupt, e:
        sys.exit(-1)
    except Exception, e:
        traceback.print_exc(file=sys.stdout)
        exit( 'Caught exception: %s: %s' % (e.__class__, str(e)) )


if __name__ == '__main__':
    main()
