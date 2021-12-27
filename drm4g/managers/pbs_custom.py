# The location of custom managers has to be added to the environment variable PYTHONPATH

from drm4g.managers.pbs import *

class Job(drm4g.managers.pbs.Job):
    def jobTemplate(self, parameters):
        
        # Dynamic directives
        args  = '#!/bin/bash\n'
        args += '#PBS -N SYNC_%s\n' % (parameters['environment']['GW_JOB_ID'])
        args += '#PBS -v %s\n' % (','.join(['%s=%s' %(k, v) for k, v in list(parameters['environment'].items())]))
        args += '#PBS -o $stdout\n'
        args += '#PBS -e $stderr\n'

        # Conditional directives
        if 'project' in parameters :
            args += '#PBS -P $project\n'
        if parameters['queue'] != 'default':
            args += '#PBS -q $queue\n'
        if 'maxWallTime' in parameters :
            args += '#PBS -l walltime=$maxWallTime\n'
        if 'maxCpuTime' in parameters :
            args += '#PBS -l cput=$maxCpuTime\n'
        if 'maxMemory' in parameters :
            args += '#PBS -l vmem=${maxMemory}MB\n'
        if 'ppn' in parameters and 'nodes' in parameters :
            args += '#PBS -l nodes=$nodes:ppn=$ppn\n'
        elif 'ppn' in parameters :
            node_count = int(parameters['count']) / int(parameters['ppn'])
            if node_count == 0:
                node_count = 1
            args += '#PBS -l nodes=%d:ppn=$ppn\n' % (node_count)
        else:
            args += '#PBS -l nodes=$count\n'

        # Static directives
        args += '#PBS -l mem=10G\n'

        # Wrapper content
        args += '$executable\n'
        return Template(args).safe_substitute(parameters)