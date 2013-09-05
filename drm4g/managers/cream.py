import drm4g.managers
import re

__version__  = '1.0'
__author__   = 'Carlos Blanco'
__revision__ = "$Id$"

X509_USER_PROXY='~/.drm4g/security/x509up.%s'
# The programs needed by these utilities. If they are not in a location
# accessible by PATH, specify their location here.
CREAM_SUBMIT = '%s glite-ce-job-submit' % X509_USER_PROXY  
CREAM_STATUS = '%s glite-ce-job-status' % X509_USER_PROXY    
CREAM_DEL    = '%s glite-ce-job-cancel' % X509_USER_PROXY
CREAM_PURGE  = '%s glite-ce-job-purge' % X509_USER_PROXY

# Regular expressions for parsing.
re_status       = re.compile("Status\s*=\s*\[(.*)\]")
re_input_files  = re.compile("GW_INPUT_FILES\s*=\s*\"(.*)\"")
re_output_files = re.compile("GW_OUTPUT_FILES\s*=\s*\"(.*)\"")

def sandbox_files(env_file):
    def parse_files(env, type, re_exp):
        files_to_copy = []
        files = re_exp.search(env)
        if files:
            for file in files.groups()[0].split(','):
                if "gsiftp://" in file: continue
                elif " " in file:
                    file0, file1 = file.split()
                if type == output:
                    file = file0
                else:
                    file = file1
            files_to_copy.append(os.path.basename(file))
        return files_to_copy
    with open(job_env_file, "r") as f :
        line_env = ''.join(f.readlines())
    f.close()
    input_files  = parse_files(line_env, input, re_input_files)
    output_files = parse_files(line_env, output, re_output_files)
    return input_files, output_files

class Resource (drm4g.managers.Resource):
    pass

class Job (drm4g.managers.Job):
   
    #cream job status <--> GridWay job status
    cream_states = {"REGISTERED"    : "PENDING",
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

    def jobSubmit(self, wrapper_file):
        cmd1 = 's -a -r %s-%s %s' % ( 
                                     CREAM_SUBMIT % self.resfeatures[ 'vo' ] , 
                                     self.resfeatures[ 'jm' ] , 
                                     self.resfeatures[ 'queue' ] ,
                                     wrapper_file 
                                     )
        out1, err1 = self.Communicator.execCommand( cmd1 )
        if not out :
            if 'The proxy has EXPIRED' in err1 :
                logger.debug( "The proxy 'x509up.%s' has probably expired: %s" % ( self.resfeatures[ 'vo' ] , err1 ) )
                X509_USER_PROXY='~/.drm4g/security/proxy'
                cmd2 = "%s voms-proxy-init -ignorewarn -timeout 30 -voms $VO -noregen -out x509up.%s" % ( 
                                                                                                         X509_USER_PROXY , 
                                                                                                         self.resfeatures[ 'vo' ]  
                                                                                                         )
                out2, err2 = self.Communicator.execCommand( cmd2 )
                if not out :
                    out3, err3 = self.Communicator.execCommand( cmd2 )
                else :
                    output = "Error submitting job after renewing the proxy: %s" % ' '.join( err3.split( '\n' ) )
                    logger.error( output )
                    raise drm4g.managers.JobException( output )
            output = "Error submitting job: %s" % ' '.join( err1.split( '\n' ) )
            logger.error( output )
            raise drm4g.managers.JobException( output )
        return out[out.find("https://"):].strip() #cream_id

    def jobStatus(self):
        out, err = self.Communicator.execCommand('%s %s' % ( CREAM_STATUS % self.resfeatures[ 'vo' ] , self.JobId ) )
        if "ERROR" in err:
            raise drm4g.managers.JobException(' '.join(err.split('\n')))
        mo = re_status.search(out)
        if mo:
            return self.cream_states.setdefault(mo.groups()[0], 'UNKNOWN')
        else:
            return 'UNKNOWN'
    
    def jobCancel(self):
        out, err = self.Communicator.execCommand('%s -N %s' % (CREAM_DEL % self.resfeatures[ 'vo' ]  , self.JobId ) )
        if "ERROR" in err: 
            raise drm4g.managers.JobException(' '.join(err.split('\n')))
		
    def jobPurge(self):
        out, err = self.Communicator.execCommand('%s -N %s' % (CREAM_PURGE % self.resfeatures[ 'vo' ] , self.JobId ) )
        if "ERROR" in err:
            raise drm4g.managers.JobException(' '.join(err.split('\n')))
        
    def jobTemplate(self, parameters):
        args ="""
        [
        JobType = "Normal";
        Executable = "$executable";
        StdOutput = "$stdout";
        StdError = "$stderr";
        QueueName = "$queue";
        CpuNumber = $count;
        """
        input_sandbox, output_sandbox = sandbox_files(self.job_env_file)
        if input_sandbox:
            args += 'InputSandbox = {%s};' % (','.join(['"%s"' % (f) for f in input_sandbox]))
        if output_sandbox:
            args += 'OutputSandbox = {"$stdout", "$stderr", %s};' % (','.join(['"%s"' % (f) for f in output_sandbox]))
        else:
            args += 'OutputSandbox = {"$stdout", "$stderr"};'                           
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


 
