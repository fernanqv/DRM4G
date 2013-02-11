import drm4g.managers 
from string import Template
import re

__version__ = '0.1'
__author__  = 'Carlos Blanco'
__revision__ = "$Id$"

# The programs needed by these utilities. If they are not in a location
# accessible by PATH, specify their location here.
LLCLASS  = 'llclass'    #show class information
LLSUBMIT = 'llsubmit'   #submit ajob
LLQ      = 'llq'        #show jobs' status
LLCANCEL = 'llcancel'   #delete ajob

class Resource (drm4g.managers.Resource):

    def hostProperties(self):
        return ('NULL', 'NULL', 'NULL', 'NULL')

    def cpuProperties(self):
        return ('NULL', '0')

    def memProperties(self):
        return ('0', '0')
    
    def diskProperties(self):
        return ('0', '0')

    def lrmsProperties(self):
        return ('Loadleveler', 'Loadleveler') 
 
    def queueProperties(self, queueName):
        queue              = drm4g.managers.Queue()
        queue.DispatchType = 'batch'
        queue.Name         = queueName
        queue.Nodes        = self.TotalCpu
        queue.FreeNodes    = self.FreeCpu
        return queue

class Job (drm4g.managers.Job):
   
    #loadleveler job status <--> GridWay job status
    states_loadleveler = {'CA': 'DONE',
                  'CK': 'ACTIVE',
                  'C' : 'PENDING',
                  'CP': 'ACTIVE',
                  'D' : 'PENDING',
                  'I' : 'PENDING',
                  'NQ': 'SUSPENDED',
                  'NR': 'SUSPENDED',
                  'P' : 'ACTIVE',
                  'E' : 'ACTIVE',
                  'EP': 'ACTIVE',
                  'X' : 'DONE',
                  'XP': 'SUSPENDED',
                  'RM': 'DONE',
                  'RP': 'SUSPENDED',
                  'MP': 'ACTIVE',
                  'R' : 'ACTIVE', 
                  'ST': 'ACTIVE',
                  'S' : 'PENDING',
                  'TX': 'PENDING', 
                  'HS': 'PENDING',  
                  'H' : 'PENDING',
                  'V' : 'DONE',
                  'VP': 'PENDING',
                }
    re_submit=re.compile(r"The job \"(\S+)\" has been submitted")

    def jobSubmit(self, pathScript):
        out, err = self.Communicator.execCommand('%s %s' % (LLSUBMIT, pathScript))
        job_id = self.re_submit.search(out).group(1)
        return job_id

    def jobStatus(self):
        command = LLQ + ' -f %st ' + self.JobId
        out, err = self.Communicator.execCommand(command)
        if "There is currently no job status to report" in out :
            return 'DONE'
        else:
            status = out.split('\n')[2].strip()
            return self.states_loadleveler.setdefault(status, 'UNKNOWN')
    
    def jobCancel(self):
        out, err = self.Communicator.execCommand('%s %s' % (LLCANCEL, self.JobId))
        if err: 
            raise drm4g.managers.JobException(' '.join(err.split('\n')))

    def jobTemplate(self, parameters):
        args  = '#!/bin/bash\n'
        args += '#@ job_name = JID_%s\n' % (parameters['environment']['GW_JOB_ID'])
        if parameters['queue'] != 'default':
            args += '#@ class    = $queue\n'
        args += '#@ output   = $stdout\n'
        args += '#@ error    = $stderr\n'
        if int(parameters['count']) > 1 :
            args += '#@ job_type  = parallel\n'
        else:
            args += '#@ job_type  = serial\n'
        args += '#@ node = $count\n'
        if parameters.has_key('maxWallTime'): 
            args += '#@ wall_clock_limit = $maxWallTime\n'
        if parameters.has_key('maxCpuTime'):
            args += '#@ job_cpu_limit = $maxCpuTime\n' 
        if parameters.has_key('maxMemory'):
            args += '#@ resources = ConsumableMemory($maxMemory)\n'
        if parameters.has_key('ppn'):
            args += '#@ tasks_per_node = $ppn'
        if parameters['PROJECT']:
            args += '#@ account_no = $PROJECT\n'
        args += '#@ queue\n'
        args += ''.join(['export %s=%s\n' % (k, v) for k, v in parameters['environment'].items()])
        args += 'cd $directory\n'
        args += '$executable\n'
        return Template(args).safe_substitute(parameters)

