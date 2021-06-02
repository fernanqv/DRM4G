#!/usr/bin/env python
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

from drm4g.core.im_mad import GwImMad
from optparse import OptionParser
import sys, traceback

def main():
    parser = OptionParser(description = 'Information manager MAD',
            prog = 'gw_im_mad_drm4g.py', version = '0.1',
            usage = 'Usage: %prog')
    options, args = parser.parse_args()
    try:
        GwImMad().processLine()
    except KeyboardInterrupt as e:
        sys.exit(-1)
    except Exception as e:
        traceback.print_exc(file=sys.stdout)
        exit('Caught exception: %s: %s' % (e.__class__, str(e)))


if __name__ == '__main__':
    main()

