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

import re
import sys
import logging
import drm4g.managers
from os.path         import basename , dirname , exists, join
from drm4g           import REMOTE_VOS_DIR
from drm4g.managers  import JobException

__version__  = '2.5.0-0b2'
__author__   = 'Carlos Blanco'
__revision__ = "$Id$"

logger = logging.getLogger(__name__)

X509_USER_PROXY = 'X509_USER_PROXY=' +  join( REMOTE_VOS_DIR , 'x509up.%s ' )
# The programs needed by these utilities. If they are not in a location
# accessible by PATH, specify their location here.
CREAM_DELEGATE = X509_USER_PROXY + 'glite-ce-delegate-proxy'
CREAM_PX_RENEW = X509_USER_PROXY + 'glite-ce-proxy-renew'
CREAM_SUBMIT   = X509_USER_PROXY + 'glite-ce-job-submit'
CREAM_STATUS   = X509_USER_PROXY + 'glite-ce-job-status'
CREAM_DEL      = X509_USER_PROXY + 'glite-ce-job-cancel'
CREAM_PURGE    = X509_USER_PROXY + 'glite-ce-job-purge'
GLOBUS_CP      = X509_USER_PROXY + 'globus-url-copy'

# Regular expressions for parsing.
re_status          = re.compile( "Current Status\s*=\s*\[(.*)\]" )
re_input_files     = re.compile( "GW_INPUT_FILES\s*=\s*\"(.*)\"" )
re_output_files    = re.compile( "GW_OUTPUT_FILES\s*=\s*\"(.*)\"" )
re_executable_file = re.compile( "GW_EXECUTABLE\s*=\s*\"(.*)\"" )
re_obs_url         = re.compile( "CREAM OSB URI\s*=\s*\[(.*)\]" )


def sandbox_files(env_file):

    def parse_files(env, type, re_exp):
        files = []
        match = re_exp.search(env)
        if match :
            for file in match.group( 1 ).split(','):
                if file.startswith( "gsiftp://" ) or file.startswith( "lfn://" ) :
                    continue
                if " " in file:
                    if type == 'output' :
                        file , _  = file.split()
                    else:
                        _  , file = file.split()
                files.append( basename( file ) )
        return files

    with open( env_file , "r" ) as f :
        line_env = ' '.join( f.readlines() )
    input_files      = parse_files( line_env , 'input' ,  re_input_files )
    output_files     = parse_files( line_env , 'output' , re_output_files )
    return input_files, output_files

class Resource (drm4g.managers.Resource):
    pass

class Job (drm4g.managers.Job):

    default_output_files = [
                            'stdout.execution',
                            'stderr.execution',
                            'stdout.wrapper',
                            'stderr.wrapper'
                            ]
    #cream job status <--> GridWay job status
    cream_states = {
                    "REGISTERED"    : "PENDING",
                    "PENDING"       : "PENDING",
                    "IDLE"          : "PENDING",
                    "RUNNING"       : "ACTIVE",
                    "REALLY-RUNNING": "ACTIVE",
                    "HELD"          : "PENDING",
                    "CANCELLED"     : "DONE",
                    "DONE-OK"       : "DONE",
                    "DONE-FAILED"   : "FAILED",
                    "ABORTED"       : "FAILED",
                    }

    def _renew_voms_proxy(self):
        output = "The proxy 'x509up.%s' has probably expired" %  self.resfeatures[ 'vo' ]
        logger.debug( output )
        if 'myproxy_server' in self.resfeatures :
            LOCAL_X509_USER_PROXY = "X509_USER_PROXY=%s" % join ( REMOTE_VOS_DIR , self.resfeatures[ 'myproxy_server' ] )
        else :
            LOCAL_X509_USER_PROXY = "X509_USER_PROXY=%s/${MYPROXY_SERVER}" % ( REMOTE_VOS_DIR )
        cmd = "%s voms-proxy-init -ignorewarn -timeout 30 -valid 24:00 -q -voms %s -noregen -out %s" % (
                                                                                                        LOCAL_X509_USER_PROXY ,
                                                                                                        self.resfeatures[ 'vo' ] ,
                                                                                                        join( REMOTE_VOS_DIR , 'x509up.%s ' ) % self.resfeatures[ 'vo' ]
                                                                                                        )
        logger.debug( "Executing command: %s" % cmd )
        out, err = self.Communicator.execCommand( cmd )
        logger.debug( out + err )
        if err :
            output = "Error renewing the proxy(%s): %s" % ( cmd , err )
            logger.error( output )

    def jobSubmit(self, wrapper_file):
        cmd = '%s -e %s delegete-proxy' % (
                                           CREAM_DELEGATE % self.resfeatures[ 'vo' ] ,
                                           self.resfeatures[ 'host' ]
                                           )
        logger.debug( "Executing command: %s" % cmd )
        out, err = self.Communicator.execCommand( cmd )
        logger.debug( out + err )
        if ( 'The proxy has EXPIRED' in out ) or ( 'is not accessible' in err ) :
            self._renew_voms_proxy()
            logger.debug( "Executing command: %s" % cmd )
            out , err = self.Communicator.execCommand( cmd )
            logger.debug( out + err )
        if ( not 'succesfully delegated' in out ) and ( not 'already exists' in out ) :
            logger.error( out )
            raise JobException( out )
        cmd = '%s -D delegete-proxy -r %s:8443/%s-%s %s' % (
                                     CREAM_SUBMIT % self.resfeatures[ 'vo' ] ,
                                     self.resfeatures[ 'host' ] ,
                                     self.resfeatures[ 'jm' ] ,
                                     self.resfeatures[ 'queue' ] ,
                                     wrapper_file
                                     )
        logger.debug( "Executing command: %s" % cmd )
        out, err = self.Communicator.execCommand( cmd )
        logger.debug( out + err )
        if ( 'The proxy has EXPIRED' in out ) or ( 'is not accessible' in err ) :
            self._renew_voms_proxy()
            logger.debug( "Executing command: %s" % cmd )
            out , err = self.Communicator.execCommand( cmd )
            logger.debug( out + err )
        if not 'https://' in out :
            output = "Error submitting job: %s %s" % ( out, err )
            logger.error( output )
            raise JobException( output )
        return out[ out.find("https://"): ].strip() #cream_id

    def jobStatus(self):
        cmd = '%s -e %s delegete-proxy' % (
                                           CREAM_PX_RENEW % self.resfeatures[ 'vo' ] ,
                                           self.resfeatures[ 'host' ]
                                           )
        logger.debug( "Executing command: %s" % cmd )
        out, err = self.Communicator.execCommand( cmd )
        logger.debug( out + err )
        if not 'succesfully renewed' in out :
            logger.error( out )
            return 'FAILED'
        cmd = '%s %s -L 2' % ( CREAM_STATUS % self.resfeatures[ 'vo' ] , self.JobId )
        logger.debug( "Executing command: %s" % cmd )
        out, err = self.Communicator.execCommand( cmd )
        logger.debug( out + err )
        if 'The proxy has EXPIRED' in out :
            self._renew_voms_proxy()
            logger.debug( "Executing command: %s" % cmd )
            out , err = self.Communicator.execCommand( cmd )
            logger.debug( out + err )
        if "ERROR" in err:
            output = "Error checking '%s' job: %s" % ( self.JobId , err )
            logger.error( output )
            return 'FAILED'
        mo = re_status.search(out)
        if mo:
            job_status = self.cream_states.setdefault(mo.groups()[0], 'UNKNOWN')
            if job_status == 'DONE' or job_status == 'FAILED' :
               output_url = self._getOutputURL( out )
               self._getOutputFiles( output_url )
            return job_status
        else:
            return 'UNKNOWN'

    def jobCancel(self):
        cmd = '%s -N %s' % (CREAM_DEL % self.resfeatures[ 'vo' ]  , self.JobId )
        logger.debug( "Executing command: %s" % cmd )
        out, err = self.Communicator.execCommand( cmd )
        logger.debug( out + err )
        if 'The proxy has EXPIRED' in out :
            self._renew_voms_proxy()
            logger.debug( "Executing command: %s" % cmd )
            out , err = self.Communicator.execCommand( cmd )
            logger.debug( out + err )
        if "ERROR" in err:
            output = "Error canceling '%s' job: %s" % ( self.JobId , err )
            logger.error( output )
            raise JobException( output )

    def jobPurge(self):
        cmd = '%s -N %s' % (CREAM_PURGE % self.resfeatures[ 'vo' ] , self.JobId )
        logger.debug( "Executing command: %s" % cmd )
        out, err = self.Communicator.execCommand( cmd )
        logger.debug( out + err )
        if 'The proxy has EXPIRED' in out :
            self._renew_voms_proxy()
            logger.debug( "Executing command: %s" % cmd )
            out , err = self.Communicator.execCommand( cmd )
        if "ERROR" in err:
            output = "Error purging '%s' job: %s" % ( self.JobId , err )
            logger.error( output )
            raise JobException( output )

    def jobTemplate(self, parameters):
        dir_temp   = self.local_output_directory = dirname( parameters['executable'] )
        executable = basename( parameters['executable'] )
        stdout     = basename( parameters['stdout'] )
        stderr     = basename( parameters['stderr'] )
        count      = parameters['count']
        ppn        = parameters.get( 'ppn', '1' )

        input_sandbox, output_sandbox = sandbox_files( self.resfeatures[ 'env_file' ] )
        if input_sandbox :
            input_files = ' '.join( [ ',"%(dir_temp)s/' + '%s"' % (f) for f in input_sandbox] ) % {'dir_temp':dir_temp }
        else :
            input_files = ''

        self.default_output_files.extend( output_sandbox )
        output_files = ','.join( [ '"%s"' % (f) for f in self.default_output_files ] )

        requirements = ''
        if 'maxWallTime' in parameters:
            requirements += '(other.GlueCEPolicyMaxWallClockTime <= %s)' % parameters['maxWallTime']
        if 'maxCpuTime' in parameters:
            if requirements:
                requirements += ' && '
            requirements += '(other.GlueCEPolicyMaxCPUTime <= %s)' % parameters['maxCpuTime']
        if 'maxMemory' in parameters:
            if requirements:
                requirements += ' && '
            requirements += ' (other.GlueHostMainMemoryRAMSize <= %s)' % parameters['maxMemory']
        Requirements = 'Requirements=%s;' % (requirements) if requirements else ''

        env = ','.join(['"%s=%s"' %(k, v) for k, v in list(parameters['environment'].items())])

        return """
[
JobType = "Normal";
Executable = "%(executable)s";
StdOutput = "%(stdout)s";
StdError = "%(stderr)s";
CpuNumber = %(count)s;
SMPGranularity = %(ppn)s;
OutputSandboxBaseDestURI = "gsiftp://localhost";
InputSandbox = { "%(dir_temp)s/job.env", "%(dir_temp)s/%(executable)s"  %(input_files)s };
OutputSandbox = { %(output_files)s };
Environment = { %(env)s };
%(Requirements)s
]""" % {
        'executable'   : executable,
        'stdout'       : stdout,
        'stderr'       : stderr,
        'dir_temp'     : dir_temp,
        'count'        : count,
        'ppn'          : ppn,
        'input_files'  : input_files,
        'output_files' : output_files,
        'Requirements' : Requirements,
        'env'          : env,
        }

    def _getOutputURL( self, status_output ):
        """
        Resolve the URL for the output files
        """
        match = re_obs_url.search( status_output )
        if match :
            url = match.group( 1 )
            return url
        else :
            output = "Output URL not found in '%s'" % ( status_output )
            logger.error( output )
            raise JobException( output )

    def _getOutputFiles( self, output_url ):
        """
        Get output files from the remote output_url
        """
        for file in self.default_output_files :
            cmd = '%s %s file://%s' % (
                                       GLOBUS_CP % self.resfeatures[ 'vo' ],
                                       join( output_url , file ) ,
                                       join( self.local_output_directory , file )
                                       )
            logger.debug( "Coping file '%s' : %s" % ( file , cmd ) )
            out, err = self.Communicator.execCommand( cmd )
            if 'error' in err :
                output = "Error coping file '%s' : %s" % ( file , err )
                logger.error( output )
                raise JobException( output )


