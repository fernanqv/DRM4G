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

"""
Print information about the hosts available on DRM4G.

Usage:
    drm4g [ options ] host [ list ] [ <hid> ]

Arguments:
    <hid>         Host identifier.

Options:
    -d --debug         Debug mode.

Host field information:
    HID           Host identifier.
    ARCH          Architecture.
    JOBS(R/T)     Number of jobs: R = running, T = total.
    LRMS          Local Resource Management System.
    HOSTNAME      Host name.
    QUEUENAME     Queue name.
    WALLT         Queue wall time.
    CPUT          Queue cpu time.
    MAXR          Max. running jobs.
    MAXQ          Max. queued jobs.
"""

from drm4g                import console_logger
from drm4g.commands       import exec_cmd, Daemon

def run( arg ):
    try :
        daemon = Daemon()
        if not daemon.is_alive() :
            raise Exception('DRM4G is stopped.')
        cmd = 'gwhost '
        if arg[ '<hid>' ] :
            cmd = cmd + arg[ '<hid>' ]
        out , err = exec_cmd( cmd )
        console_logger.info( out )
        if err :
            console_logger.info( err )
    except Exception as err :
        console_logger.error( str( err ) )

