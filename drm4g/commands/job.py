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
Submit, get status and history and cancel jobs.

Usage:
    drm4g job submit  [ options ] [ --ntasks <total_tasks> ] [ --dep <job_id> ... ] <template>
    drm4g job list    [ options ] [ <job_id> ]
    drm4g job cancel  [ options ] <job_id> ...
    drm4g job log     [ options ] <job_id>
    drm4g job history [ options ] <job_id>

Arguments:
    <job_id>               Job identifier.
    <template>             Job template.
    <total_tasks>          Total number of tasks in the job array.

Options:
    --ntasks <total_tasks> Number of tasks to submit.
    --dep=<job_id> ...     Define the job dependency list of the job.
    -d --debug             Debug mode.

Commands:
    submit                 Command for submitting jobs.
    list                   Monitor jobs previously submitted.
    cancel                 Cancel jobs.
    log                    Keep track of a job.
    history                Get information about the execution history of a job.

Job field information:
    JID                    Job identification.
    DM 	                   Dispatch Manager state, one of the following: pend, hold, prol, prew, wrap, epil, canl, stop, migr, done, fail.
    EM 	                   Execution Manager state: pend, susp, actv, fail or done.
    START                  The time the job entered the system.
    END                    The time the job reached a final state (fail or done).
    EXEC                   Total execution time, includes suspension time in the remote queue system.
    XFER                   Total file transfer time, includes stage-in and stage-out phases.
    EXIT                   Job exit code.
    TEMPLATE               Filename of the job template used for this job.
    HOST                   Hostname where the job is being executed.
    HID                    Host identification.
    PROLOG                 Total prolog (file stage-in phase) time.
    WRAPPER                Total wrapper (execution phase) time.
    EPILOG                 Total epilog (file stage-out phase) time.
    MIGR                   Total migration time.
    REASON                 The reason why the job left this host.
    QUEUE                  Queue name.
"""

import logging
from os.path              import join, exists
from drm4g                import DRM4G_DIR, logger
from drm4g.commands       import exec_cmd, Daemon

def run( arg ) :
    try :
        daemon = Daemon( )
        if not daemon.is_alive() :
            raise Exception( 'DRM4G is stopped. ')
        if arg['submit']:
            dependencies = '-d "%s"' % ' '.join( arg['--dep'] ) if arg['--dep'] else ''
            number_of_tasks = '-n %s' % arg['--ntasks'] if arg['--ntasks'] else ''
            cmd = 'gwsubmit %s -v %s %s' % ( dependencies, arg['<template>'], number_of_tasks )
        elif arg['list']:
            cmd = 'gwps -o Jsetxjh '
            if arg['<job_id>'] :
                cmd = cmd + arg['<job_id>'][0]
        elif arg['history']:
            cmd = 'gwhistory %s' % ( arg['<job_id>'][ 0 ] )
        elif arg['log']:
            directory = join(
                              DRM4G_DIR ,
                              'var' ,
                              '%d00-%d99' % ( int(int(float(arg['<job_id>'][0]))/100) , int(int(float(arg['<job_id>'][0]))/100) ) ,
                              arg['<job_id>'][0] ,
                              'job.log'
                            )
            if not exists( directory ) :
                raise Exception( 'There is not a log available for this job.')
            cmd = 'cat %s' % ( directory )
        else :
            cmd = 'gwkill -9 %s' % ( ' '.join( arg['<job_id>'] ) )
        out , err = exec_cmd( cmd )
        logger.info( out )
        if err :
            logger.info( err )
    except Exception as err :
        logger.error( str( err ) )
