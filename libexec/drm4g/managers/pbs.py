import drm4g.managers 
from string import Template
from drm4g.managers import sec_to_H_M_S
import re
import xml.dom.minidom

__version__ = '0.1'
__author__  = 'Carlos Blanco'
__revision__ = "$Id$"

# The programs needed by these utilities. If they are not in a location
# accessible by PATH, specify their location here.
PBSNODES = 'LANG=POSIX pbsnodes' #pbsnodes - pbs node manipulation
QSUB     = 'LANG=POSIX qsub'     #qsub - submit pbs job
QSTAT    = 'LANG=POSIX qstat'    #qstat - show status of pbs batch jobs
QDEL     = 'LANG=POSIX qdel'     #qdel - delete pbs batch job

class Resource (drm4g.managers.Resource):

    def lrmsProperties(self):
        return ('PBS', 'PBS') 
 
    def dynamicNodes(self):
        out, err = self.Communicator.execCommand('%s -x' % (PBSNODES))
        if err: 
            raise drm4g.managers.ResourceException(' '.join(err.split('\n')))
        out_parser = xml.dom.minidom.parseString(out)
        total_cpu  = sum([int(elem.getElementsByTagName('np')[0].firstChild.data) \
            for elem in out_parser.getElementsByTagName('Node')])
        auxCpu = ','.join([elem.getElementsByTagName('jobs')[0].firstChild.data \
            for elem in out_parser.getElementsByTagName('Node') \
                if elem.getElementsByTagName('jobs')]).count(',')
        if auxCpu != 0 : free_cpu = total_cpu - (auxCpu + 1)
        else : free_cpu = total_cpu - auxCpu
        return (str(total_cpu), str(free_cpu))

    def queuesProperties(self, searchQueue, project):
        out, err = self.Communicator.execCommand('%s -q' % (QSTAT))
        #output line --> Queue Memory CPU_Time Walltime Node Run Que Lm State
        if err:
            raise drm4g.managers.ResourceException(' '.join(err.split('\n')))
        queues = []
        for val in out.split('\n')[5:-3]:
            queueName, _, cpuTime, wallTime, _, _, _, lm = val.split()[0:8]
            if queueName == searchQueue or not searchQueue:
                queue              = drm4g.managers.Queue()
                queue.Name         = queueName
                queue.Nodes        = self.TotalCpu
                queue.FreeNodes    = self.FreeCpu
                queue.DispatchType = 'batch'
                time = re.compile(r'(\d+):\d+:\d+')
                if cpuTime != '--':
                    queue.MaxCpuTime = str(int(time.search(cpuTime).group(1)) * 60)
                if wallTime != '--':
                    queue.MaxTime    = str(int(time.search(cpuTime).group(1)) * 60)
                if lm != '--': 
                    queue.MaxRunningJobs = lm
                queues.append(queue)
        return queues

class Job (drm4g.managers.Job):
   
    #pbs job status <--> GridWay job status
    states_pbs = {'E': 'ACTIVE',    #Job is exiting after having run.
                  'H': 'SUSPENDED', #Job is held.
                  'Q': 'PENDING',   #Job is queued, eligable to run or routed.
                  'R': 'ACTIVE',    #Job is running.
                  'T': 'PENDING',   #Job is being moved to new location.
                  'W': 'PENDING',   #Job is waiting for its execution time to be reached.
                  'S': 'SUSPENDED', #Job is suspend.
                  'C': 'DONE',	    #Job finalize.
                }

    def jobSubmit(self, path_script):
        out, err = self.Communicator.execCommand('%s %s' % (QSUB, path_script))
        if err: 
            raise drm4g.managers.JobException(' '.join(err.split('\n')))
        return out.strip() #job_id

    def jobStatus(self):
        out, err = self.Communicator.execCommand('%s %s -x' % (QSTAT, self.JobId))
        if 'qstat: Unknown Job Id' in err :
            return 'DONE'
        elif err:
            return 'UNKNOWN'
        else:
            out_parser = xml.dom.minidom.parseString(out)
            state = out_parser.getElementsByTagName('job_state')[0].firstChild.data
            return self.states_pbs.setdefault(state, 'UNKNOWN')
    
    def jobCancel(self):
        out, err = self.Communicator.execCommand('%s %s' % (QDEL, self.JobId))
        if err: 
            raise drm4g.managers.JobException(' '.join(err.split('\n')))

    def jobTemplate(self, parameters):
        args  = '#!/bin/bash\n'
        args += '#PBS -N JID_%s\n' % (parameters['environment']['GW_JOB_ID'])
        args += '#PBS -q $queue\n'
        args += '#PBS -o $stdout\n'
        args += '#PBS -e $stderr\n'
        if parameters.has_key('maxWallTime'): 
            args += '#PBS -l walltime=%s\n' % (sec_to_H_M_S(parameters['maxWallTime']))
        if parameters.has_key('maxCpuTime'): 
            args += '#PBS -l cput=%s\n' % (sec_to_H_M_S(parameters['maxCpuTime']))
        if parameters.has_key('maxMemory'):
            args += '#PBS -l mem=%smb\n' % (parameters['maxMemory'])
        if parameters.has_key('tasksPerNode'):
            cpus = int(parameters['tasksPerNode']) * int(parameters['count'])
            args += '#PBS -l nodes=%d:ppn=$tasksPerNode\n' % (cpus)
        else:
            args += '#PBS -l nodes=$count\n'
        args += '#PBS -v %s\n' % (','.join(['%s=%s' %(k, v) for k, v in parameters['environment'].items()]))
        args += 'cd $directory\n'
        if parameters['jobType'] == "mpi":
            args += 'mpi -np $count $executable\n'
        else:
            args += '$executable\n'
        return Template(args).safe_substitute(parameters)


