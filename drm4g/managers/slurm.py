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
import drm4g.managers
from string import Template


# The programs needed by these utilities. If they are not in a location
# accessible by PATH, specify their location here.
SBATCH  = 'sbatch'   #submit a job
SQUEUE  = 'squeue'   #show status of jobs
SCANCEL = 'scancel'  #delete a job

class Resource (drm4g.managers.Resource):
    pass

class Job (drm4g.managers.Job):

    #job status <--> GridWay job status
    states_SLURM = {'CANCELLED': 'DONE',
                  'COMPLETED' : 'DONE',
                  'COMPLETING': 'ACTIVE',
                  'RUNNING'   : 'ACTIVE',
                  'NODE_FAIL' : 'FAILED',
                  'FAILED'    : 'FAILED',
                  'PENDING'   : 'PENDING',
                  'SUSPENDED' : 'SUSPENDED',
                  'TIMEOUT'   : 'FAILED',
                }

    def jobSubmit(self, pathScript):
        out, err = self.Communicator.execCommand('%s %s' % (SBATCH, pathScript))
        re_job_id = re.compile(r'Submitted batch job (\d*)').search(out)
        if re_job_id:
            return re_job_id.group(1)
        else:
            raise drm4g.managers.JobException(' '.join(err.split('\n')))

    def jobStatus(self):
        out, err = self.Communicator.execCommand('squeue -h -o %T -j ' + self.JobId)
        if err or not out:
            return 'DONE'
        else:
            return self.states_SLURM.setdefault(out.rstrip('\n'), 'UNKNOWN')

    def jobCancel(self):
        out, err = self.Communicator.execCommand('%s %s' % (SCANCEL, self.JobId))
        if err:
            raise drm4g.managers.JobException(' '.join(err.split('\n')))

    def jobTemplate(self, parameters):
        args  = '#!/bin/bash\n'
        args += '#SBATCH --job-name=JID_%s\n' % (parameters['environment']['GW_JOB_ID'])
        args += '#SBATCH --output=$stdout\n'
        args += '#SBATCH --error=$stderr\n'
        if parameters['queue'] != 'default':
            args += '#SBATCH -p $queue\n'
        if 'maxWallTime' in parameters :
            args += '#SBATCH --time=%s\n' % (parameters['maxWallTime'])
        if 'maxMemory' in parameters :
            args += '#SBATCH --mem=%s\n' % (parameters['maxMemory'])
        if 'ppn' in parameters :
            args += '#SBATCH --ntasks-per-node=$ppn'
        if 'nodes' in parameters :
            args += '#SBATCH --nodes=$nodes'
        args += '#SBATCH --ntasks=$count\n'
        args += ''.join(['export %s=%s\n' % (k, v) for k, v in list(parameters['environment'].items())])
        args += '\n'
        args += '$executable\n'
        return Template(args).safe_substitute(parameters)


