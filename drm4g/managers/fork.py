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

import os
import drm4g.managers
from string import Template


SH = '/bin/bash'

class Resource (drm4g.managers.Resource):
    pass

class Job (drm4g.managers.Job):

    def jobSubmit(self, pathScript):
        out, err = self.Communicator.execCommand('%s %s' % (SH, pathScript))
        if err:
            raise drm4g.managers.JobException(' '.join(err.split('\n')))
        job_id = out.rstrip('\n')
        return job_id

    def jobStatus(self):
        out, err = self.Communicator.execCommand('ps --no-heading -p %s' %(self.JobId))
        if out:
            return 'ACTIVE'
        else:
            return 'DONE'

    def jobCancel(self):
        jobs_to_kill = [self.JobId]
        while jobs_to_kill:
            for job in jobs_to_kill:
                 out, err = self.Communicator.execCommand('ps ho pid --ppid %s' % (job))
                 jobs_to_kill = [line.lstrip() for line in out.splitlines()] + jobs_to_kill
                 out, err = self.Communicator.execCommand('kill -9 %s' % (job))
                 if err:
                     raise drm4g.managers.JobException('Could not kill %s : %s' % (job, ' '.join(err.split('\n'))))
                 jobs_to_kill.remove(job)

    def jobTemplate(self, parameters):
        line  = '#!/bin/bash\n'
        line += ''.join(['export %s=%s\n' % (k, v) for k, v in list(parameters['environment'].items())])
        line += '\n'
        line += 'nohup $executable 2> $stderr > $stdout &\n'
        line += 'echo $$!\n'
        return Template(line).safe_substitute(parameters)


