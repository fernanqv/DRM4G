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

__version__  = '2.5.0-beta'
__author__   = 'Carlos Blanco'
__revision__ = "$Id$"

import sys
import os
import traceback
import logging.config
from os.path import dirname, join

if sys.version_info >= (2,5) and sys.version_info >= (3,3):
    exit( 'The version number of the Python has to be >= 2.6 and >= 3.3' )
try:
    sys.path.insert(0, join(dirname(dirname(os.path.abspath(__file__))), 'libexec'))
    from drm4g import DRM4G_LOGGER, DRM4G_DIR  
    try:
        logging.config.fileConfig(DRM4G_LOGGER, {"DRM4G_DIR": DRM4G_DIR})
    except :
        pass
except Exception as e:
    traceback.print_exc(file=sys.stdout)
    exit( 'Caught exception: %s: %s' % (e.__class__, str(e)) )
from optparse import OptionParser
import exceptions

