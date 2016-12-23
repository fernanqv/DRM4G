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
import os
import logging
import subprocess
from distutils import spawn


r = re.compile(r'[:,\s]') # match whitespac, coma or :

def parse(output):
    output = [r.split(line) for line in output.splitlines()]
    # now we have a list of lists, but it may contain empty strings
    for line in output:
        while '' in line:
            line.remove('')
    # turn into dict and return
    return dict([(line[0],line[1:]) for line in output])

def exec_cmd( cmd, stdout = subprocess.PIPE, stderr = subprocess.PIPE, env = os.environ ):
    logging.debug( "Executing command ... " + cmd )
    p      = subprocess.Popen( cmd, shell = True, stdout = stdout,
                               stderr = stderr, env = env )
    output = p.stdout.read().strip() + p.stderr.read().strip()
    code   = p.wait()
    return code, output

def which( command ):
    """
    Locate commands
    """
    return spawn.find_executable( command )
