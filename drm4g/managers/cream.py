import re
import drm4g.managers
from os.path        import basename
from drm4g          import REMOTE_VOS_DIR
from drm4g.managers import JobException

__version__  = '1.0'
__author__   = 'Carlos Blanco'
__revision__ = "$Id$"

X509_USER_PROXY = 'X509_USER_PROXY=%s/x509up.%s' % REMOTE_VOS_DIR
# The programs needed by these utilities. If they are not in a location
# accessible by PATH, specify their location here.
CREAM_SUBMIT = '%s glite-ce-job-submit' % X509_USER_PROXY  
CREAM_STATUS = '%s glite-ce-job-status' % X509_USER_PROXY    
CREAM_DEL    = '%s glite-ce-job-cancel' % X509_USER_PROXY
CREAM_PURGE  = '%s glite-ce-job-purge'  % X509_USER_PROXY

# Regular expressions for parsing.
re_status       = re.compile( "Status\s*=\s*\[(.*)\]" )
re_input_files  = re.compile( "GW_INPUT_FILES\s*=\s*\"(.*)\"" )
re_output_files = re.compile( "GW_OUTPUT_FILES\s*=\s*\"(.*)\"" )

def sandbox_files(env_file):
    def parse_files(env, type, re_exp):
        files_to_copy = []
        files         = re_exp.search(env)
        if files:
            print files.groups()[0]
            for file in files.groups()[0].split(','):
                print file
                if "gsiftp://" in file: 
                    continue
                if " " in file:
                    file0, file1 = file.split()
                    if type == 'output' :
                        file = file0
                    else:
                        file = file1
                files_to_copy.append( basename( file ) )
        return files_to_copy
    with open( env_file , "r" ) as f :
        line_env = ' '.join( f.readlines() )
    f.close()
    input_files  = parse_files( line_env , 'input' , re_input_files )
    output_files = parse_files( line_env , 'output' , re_output_files )
    return input_files, output_files

class Resource (drm4g.managers.Resource):
    pass

class Job (drm4g.managers.Job):
   
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

    def _renew_proxy(self):
        output = "The proxy 'x509up.%s' has probably expired" %  self.resfeatures[ 'vo' ]  
        logger.debug( output )
        X509_USER_PROXY = "X509_USER_PROXY=%s/proxy" % REMOTE_VOS_DIR
        cmd = "%s voms-proxy-init -ignorewarn -timeout 30 -valid 24:00 -voms %s -noregen -out %s/x509up.%s" % (
                                                                                                         X509_USER_PROXY ,
                                                                                                         self.resfeatures[ 'vo' ] ,
                                                                                                         REMOTE_VOS_DIR ,
                                                                                                         self.resfeatures[ 'vo' ] ,
                                                                                                         )
        out, err = self.Communicator.execCommand( cmd )
        if err :
            output = "Error renewing the proxy: %s" % err
            logger.error( output )
            raise JobException( output )

    def jobSubmit(self, wrapper_file):
        cmd = 's -a -r %s-%s %s' % ( 
                                     CREAM_SUBMIT % self.resfeatures[ 'vo' ] , 
                                     self.resfeatures[ 'jm' ] , 
                                     self.resfeatures[ 'queue' ] ,
                                     wrapper_file 
                                     )
        out, err = self.Communicator.execCommand( cmd )
        if ( 'The proxy has EXPIRED' in err ) or ( ' is not accessible' in err ) :
            self._renew_proxy()
            out , err = self.Communicator.execCommand( cmd )
            if err : 
                output = "Error submitting job after renewing the proxy: %s" %  err 
                logger.error( output )
                raise JobException( output )
        else :
            output = "Error submitting job: %s" % err
            logger.error( output )
            raise JobException( output )
        return out[ out.find("https://"): ].strip() #cream_id

    def jobStatus(self):
        cmd = '%s %s' % ( CREAM_STATUS % self.resfeatures[ 'vo' ] , self.JobId )
        out, err = self.Communicator.execCommand( cmd )
        if 'The proxy has EXPIRED' in err :
            self._renew_proxy()
            out , err = self.Communicator.execCommand( cmd )
            if "ERROR" in err: 
                 output = "Error checking '%s' job after renewing the proxy: %s" % ( self.JobId , err )
                 logger.error( output )
                 raise JobException( output )
        if "ERROR" in err:
            output = "Error checking '%s' job: %s" % ( self.JobId , err )
            logger.error( output )
            raise JobException( output )
        mo = re_status.search(out)
        if mo:
            return self.cream_states.setdefault(mo.groups()[0], 'UNKNOWN')
        else:
            return 'UNKNOWN'
    
    def jobCancel(self):
        cmd = '%s -N %s' % (CREAM_DEL % self.resfeatures[ 'vo' ]  , self.JobId )
        out, err = self.Communicator.execCommand( cmd )
        if 'The proxy has EXPIRED' in err :
            self._renew_proxy()
            out , err = self.Communicator.execCommand( cmd )
            if "ERROR" in err:
                 output = "Error canceling '%s' job after renewing the proxy: %s" % ( self.JobId , err )
                 logger.error( output )
                 raise JobException( output )
        if "ERROR" in err: 
            output = "Error canceling '%s' job: %s" % ( self.JobId , err )
            logger.error( output )
            raise JobException( output )
		
    def jobPurge(self):
        cmd = '%s -N %s' % (CREAM_PURGE % self.resfeatures[ 'vo' ] , self.JobId )
        out, err = self.Communicator.execCommand( cmd )
        if 'The proxy has EXPIRED' in err :
            self._renew_proxy()
            out , err = self.Communicator.execCommand( cmd )
            if "ERROR" in err:
                 output = "Error purging '%s' job after renewing the proxy: %s" % ( self.JobId , err )
                 logger.error( output )
                 raise JobException( output )
        if "ERROR" in err: 
            output = "Error purging '%s' job: %s" % ( self.JobId , err )
            logger.error( output )
            raise JobException( output )
        
    def jobTemplate(self, parameters):
        executable = basename( parameters['executable'] ) 
        stdout     = basename( parameters['stdout'] ) 
        stderr     = basename( parameters['stderr'] ) 
        args  = '[\n'
        args += 'JobType = "Normal";\n'
        args += 'Executable = "%s";\n' % executable
        args += 'StdOutput = "%s";\n'  % stdout
        args += 'StdError = "%s";\n'   % stderr
        args += 'QueueName = "$queue";\n'
        args += 'CpuNumber = $count;\n'

        input_sandbox, output_sandbox = sandbox_files(self.job_env_file)
        if input_sandbox:
            args += 'InputSandbox = {%s};' % (','.join(['"%s"' % (f) for f in input_sandbox]))
        if output_sandbox:
            args += 'OutputSandbox = {"%s", "%s", %s};' % (
                                                           stdout ,
                                                           stderr ,
                                                           ', '.join( [ '"%s"' % (f) for f in output_sandbox ] ) ,
                                                           )
        else:
            args += 'OutputSandbox = {"%s", "%s"};' % ( stdout , stderr )                           
        requirements = ''
        if parameters.has_key('maxWallTime'):  
            requirements += '(other.GlueCEPolicyMaxWallClockTime <= $maxWallTime)' 
        if parameters.has_key('maxCpuTime'):
            if requirements: 
                requirements += ' && '
            requirements += '(other.GlueCEPolicyMaxCPUTime <= $maxCpuTime)' 
        if parameters.has_key('maxMemory'):
            if requirements: 
                requirements += ' && '
            requirements += ' (other.GlueHostMainMemoryRAMSize <= $maxMemory) '
        args += 'Requirements=%s;\n' % (requirements)
        args += 'Environment={%s};\n' % (','.join(['"%s=%s"' %(k, v) for k, v in parameters['environment'].items()]))
        args += ']'
        return Template(args).safe_substitute(parameters)


 
