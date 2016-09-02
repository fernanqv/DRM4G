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
Stop DRM4G daemon and ssh-agent.

Usage:
    drm4g stop [ options ]

Options:
   --dbg    Debug mode.
"""
__version__  = '2.5.0-0b2'
__author__   = 'Carlos Blanco'
__revision__ = "$Id$"

import logging
from drm4g                import logger
from drm4g.commands       import Daemon, Agent

def run( arg ) :
    try:
        if arg[ '--dbg' ] :
            logger.setLevel(logging.DEBUG)
        Daemon().stop()
        Agent().stop()
    except Exception as err :
        logger.error( str( err ) )
