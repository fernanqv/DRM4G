import drm4g.managers 
import re

__version__ = '0.1'
__author__  = 'Santander Meteorology Group'
__revision__ = "$Id:$"

# The programs needed by these utilities. If they are not in a location
# accessible by PATH, specify their location here.
CREAM_SUBMIT = 'LANG=POSIX glite-ce-job-submit'    
CREAM_STATUS = 'LANG=POSIX glite-ce-job-status'    
CREAM_DEL    = 'LANG=POSIX glite-ce-job-cancel'
CREAM_PURGE  = 'LANG=POSIX glite-ce-job-purge' 

# Regular expressions for parsing job status.
re_status = re.compile("Status\s*=\s*\[(.*)\]")

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

    def jobSubmit(self, host_jm, path_script):
        out, err = self.Communicator.execCommand('%s -a -r %s %s' % (CREAM_SUBMIT, host_jm, path_script))
        if not out: 
            raise drm4g.managers.JobException(' '.join(err.split('\n')))
        self.JobId = out.strip() #cream_id

    def jobStatus(self):
        out, err = self.Communicator.execCommand('%s %s' % (CREAM_STATUS, self.JobId))
        if "ERROR" in err:
            raise drm4g.managers.JobException(' '.join(err.split('\n')))
        mo = re_status.search(line)
        if mo:
            return self.cream_states.setdefault(mo.groups()[0], 'UNKNOWN')
        else:
            return 'UNKNOWN'
    
    def jobCancel(self):
        out, err = self.Communicator.execCommand('%s -N %s' % (CREAM_DEL, self.JobId))
        if "ERROR" in err: 
            raise drm4g.managers.JobException(' '.join(err.split('\n')))
		
    def jobPurge(self):
        out, err = self.Communicator.execCommand('%s -N %s' % (CREAM_PURGE, self.JobId))
        if "ERROR" in err:
            raise drm4g.managers.JobException(' '.join(err.split('\n')))



 
