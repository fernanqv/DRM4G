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
import drm4g.managers.slurm
from string         import Template
from drm4g.managers import sec_to_H_M_S


# The programs needed by these utilities. If they are not in a location
# accessible by PATH, specify their location here.
MSUB    = 'msub'   #submit a job
SQUEUE  = 'squeue'   #show status of jobs
SCANCEL = 'scancel'  #delete a job

class Resource (drm4g.managers.slurm.Resource):
    pass

class Job (drm4g.managers.slurm.Job):

    def jobSubmit(self, pathScript):
        out, err = self.Communicator.execCommand('%s %s' % (MSUB, pathScript))
        if out:
            return out.split()[0]
        else:
            raise drm4g.managers.JobException(' '.join(err.split('\n')))

    def jobTemplate(self, parameters):
        args  = '#!/bin/bash\n'
        args += '#MOAB -N JID_%s\n' % (parameters['environment']['GW_JOB_ID'])
        args += '#MOAB -o $stdout\n'
        args += '#MOAB -e $stderr\n'
        args += '#MOAB -l nodes=$count\n'
        args += '#MOAB -V'
        args += ''.join(['export %s=%s\n' % (k, v) for k, v in list(parameters['environment'].items())])
        args += '\n'
        args += '$executable\n'
        return Template(args).safe_substitute(parameters)


