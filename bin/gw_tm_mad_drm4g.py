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

import sys
import traceback
from argparse import ArgumentParser,SUPPRESS

from drm4g.core.tm_mad import GwTmMad

def main():
    parser = ArgumentParser(
               description = 'Transfer manager MAD',
            )
    parser.add_argument('-v', '--version', action='version', version='0.1')
    #workaround for issue 
    #   https://github.com/SantanderMetGroup/DRM4G/issues/27
    parser.add_argument('null', nargs="*", type=str, help=SUPPRESS)
    parser.parse_args()
    try:
        GwTmMad().processLine()
    except KeyboardInterrupt:
        return -1
    except Exception as e:
        traceback.print_exc(file=sys.stderr)
        return 'Caught exception: %s: %s' % (e.__class__, str(e))

if __name__ == '__main__':
    sys.exit(main())
