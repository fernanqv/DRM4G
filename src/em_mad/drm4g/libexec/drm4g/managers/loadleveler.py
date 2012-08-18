import drm4g.managers 
from string import Template
from drm4g.managers import sec_to_H_M_S
import re

__version__ = '0.1'
__author__  = 'Carlos Blanco'
__revision__ = "$Id:$"

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
 
    def dynamicNodes(self):
        out, err = self.Communicator.execCommand("%s -l | egrep 'Maximum_slots' | awk '{print $2}'" % (LLCLASS))
        if err:
            raise drm4g.managers.ResourceException(' '.join(err.split('\n')))
        total_cpu  = sum([int(elem) for elem in out.split()])
        out, err = self.Communicator.execCommand("%s -l | egrep 'Free_slots' | awk '{print $2}'" % (LLCLASS))
        if err:
            raise drm4g.managers.ResourceException(' '.join(err.split('\n')))
        free_cpu  = sum([int(elem) for elem in out.split()])
        return (str(total_cpu), str(free_cpu))

    def queuesProperties(self, searchQueue, project):
        queue  = drm4g.managers.Queue()
        queue.DispatchType = 'batch'
        if searchQueue:
            out, err = self.Communicator.execCommand("%s -l %s | -e Free_slots -e Maximum_slots | awk '{print $2}'" % (LLCLASS, searchQueue))
            if err:
                raise drm4g.managers.ResourceException(' '.join(err.split('\n')))
            if self.TotalCpu != "0":
                queue.FreeNodes, queue.Nodes = out.split()  
            else:
                queue.Nodes        = self.TotalCpu
                queue.FreeNodes    = self.FreeCpu
        else:
            queue.Name         = 'default'
            queue.Nodes        = self.TotalCpu
            queue.FreeNodes    = self.FreeCpu
        return [queue]

class Job (drm4g.managers.Job):
   
    #loadleveler job status <--> GridWay job status
    states_pbs = {'CA': 'DONE',
                  'CK': 'ACTIVE',
                  'C' : 'DONE',
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

    def jobSubmit(self, path_script):
        out, err = self.Communicator.execCommand('%s %s' % (LLSUBMIT, path_script))
        if err: 
            raise drm4g.managers.JobException(' '.join(err.split('\n')))
        job_id = self.re_submit.search(out).group(1)
        return job_id

    def jobStatus(self):
        out, err = self.Communicator.execCommand('%s -f %st %s' % (LLQ, self.JobId))
        if err :
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
        args += '# @ job_name = JID_%s\n' % (parameters['environment']['GW_JOB_ID'])
        if parameters.has_key('queue') != 'default':
            args += '# @ class    = $queue\n'
        args += '# @ output   = $stdout\n'
        args += '# @ error    = $stderr\n'
        if int(parameters['count']) > 1 :
            args += '# @ job_type  = parallel'
        else:
            args += '# @ job_type  = serial'
        args += '# @ node = $count\n'
        if parameters.has_key('maxWallTime'): 
            args += '# @ wall_clock_limit = %s\n' % (sec_to_H_M_S(parameters['maxWallTime']))
        if parameters.has_key('maxCpuTime'):
            args += '# @ job_cpu_limit = %s\n' % (sec_to_H_M_S(parameters['maxCpuTime']))
        if parameters.has_key('maxMemory'):
            args += '# @ resources = ConsumableMemory(%s)\n' % (parameters['maxMemory'])
        if parameters.has_key('tasksPerNode'):
            args += '# @ tasks_per_node = $tasksPerNode\n' % (parameters['tasksPerNode'])
        if parameters.has_key('PROJECT'):
            args += '#@ account_no = $PROJECT\n'
        args  = '#@ queue\n'
        args += ''.join(['export %s=%s\n' % (k, v) for k, v in parameters['environment'].items()])
        args += 'cd $directory\n'
        if parameters['jobType'] == "mpi":
            args += 'mpiexec -np $count $executable\n'
        else:
            args += '$executable\n'
        return Template(args).safe_substitute(parameters)

