#!/usr/bin/env python
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

__version__  = '2.6.1'
__author__   = 'Carlos Blanco'
__revision__ = "$Id$"

from drm4g             import DRM4G_LOGGER, DRM4G_DIR
from drm4g.core.im_mad import GwImMad
from optparse import OptionParser
import exceptions, sys, traceback, logging

def main():
    parser = OptionParser(description = 'Information manager MAD',
            prog = 'gw_im_mad_drm4g.py', version = '0.1',
            usage = 'Usage: %prog ')
    options, args = parser.parse_args()
    try:
        try:
            logging.config.fileConfig(DRM4G_LOGGER, {"DRM4G_DIR": DRM4G_DIR})
        except :
            pass
        GwImMad().processLine()
    except exceptions.KeyboardInterrupt as e:
        sys.exit(-1)
    except exceptions.SystemExit as e:
        traceback.print_exc(file=sys.stdout)
        exit( 'Caught exception: %s: %s' % (e.__class__, str(e)) )


if __name__ == '__main__':
    main()

