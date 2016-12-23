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
BSUB  = 'bsub'   #submit a job
BJOBS = 'bjobs'  #show status of jobs
BKILL = 'bkill'  #delete a job

class Resource (drm4g.managers.Resource):
    pass

class Job (drm4g.managers.Job):

    #job status <--> GridWay job status
    states_LSF = {'DONE'  : 'DONE',
                  'EXIT'  : 'DONE',
                  'RUN'   : 'ACTIVE',
                  'ZOMBI' : 'FAILED',
                  'PEND'  : 'PENDING',
                  'PSUSP' : 'SUSPENDED',
                  'USUSP' : 'SUSPENDED',
                  'SSUSP' : 'SUSPENDED',
                  'UNKWN' : 'UNKNOWN',
                  }

    def jobSubmit(self, pathScript):
        out, err = self.Communicator.execCommand('%s < %s' % (BSUB, pathScript))
        reJobId = re.compile(r'Job <(\d*)> is submitted').search(out)
        if reJobId:
            return reJobId.group(1)
        else:
            raise drm4g.managers.JobException(' '.join(err.split('\n')))

    def jobStatus(self):
        out, err = self.Communicator.execCommand('%s %s' % (BJOBS, self.JobId))
        if err:
            return 'UNKNOWN'
        else:
            return self.states_LSF.setdefault(out.split()[10], 'UNKNOWN')

    def jobCancel(self):
        out, err = self.Communicator.execCommand('%s %s' % (BKILL, self.JobId))
        if err:
            raise drm4g.managers.JobException(' '.join(err.split('\n')))

    def jobTemplate(self, parameters):
        args  = '#!/bin/bash\n'
        args += '#BSUB -J JID_%s\n' % (parameters['environment']['GW_JOB_ID'])
        args += '#BSUB -o $stdout\n'
        args += '#BSUB -e $stderr\n'
        args += '#BSUB -n $count\n'
        if parameters['queue'] != 'default':
            args += '#BSUB -q $queue\n'
        args += '#BSUB -W $maxWallTime\n'
        if 'ppn' in parameters:
            args += '#BSUB -R"span[ptile=$ppn]"'
        args += ''.join(['export %s=%s\n' % (k, v) for k, v in list(parameters['environment'].items())])
        args += '\n'
        args += '$executable\n'
        return Template(args).safe_substitute(parameters)
