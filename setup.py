#
# Copyright 2021 Santander Meteorology Group (UC-CSIC)
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

from setuptools import find_packages
from setuptools.command.install import install
import os
import subprocess
import glob
import sys
import drm4g

#To ensure a script runs with a minimal version requirement of the Python interpreter
#assert sys.version_info >= (2,5)
if (sys.version_info[0]==2 and sys.version_info<=(2,5)) or (sys.version_info[0]==3 and sys.version_info<(3,3)):
  exit( 'The version number of Python has to be >= 2.6 or >= 3.3' )

here = os.path.abspath(path.dirname(__file__))

# read the contents of your README file
with open(os.path.join(here, 'README'), encoding='utf-8') as f:
  long_description = f.read()

def build(self):
  gridway_src=os.path.join( here, "gridway-5.8")
  current_path = os.getcwd()
  self.prefix_option()
  
  if not os.path.exists(gridway) :
      raise Exception("The specified directory %s doesn't exist" % gridway_src)
  os.chdir( gridway_src )
  
  exit_code = subprocess.call('chmod +x ./configure && ./configure', shell=True)
  if exit_code:
    raise Exception("Configure failed - check config.log for more detailed information")
  
  exit_code = subprocess.call('make', shell=True)
  if exit_code:
    raise Exception("make failed")
  os.chdir( current_path )


class build_wrapper(install):
  def run(self):
    build()
    install.run(self)

bin_scripts = glob.glob(os.path.join('bin', '*'))
bin_scripts.append('LICENSE')

setup(
  name='drm4g',
  packages=find_packages(),
  include_package_data=True,
  package_data={'drm4g' : ['conf/*.conf', 'conf/job_template.default', 'conf/*.sh']},
  data_files=[('bin', [
    'gridway-5.8/src/cmds/gwuser', 'gridway-5.8/src/cmds/gwacct', 'gridway-5.8/src/cmds/gwwait', 
    'gridway-5.8/src/cmds/gwhost', 'gridway-5.8/src/cmds/gwhistory', 'gridway-5.8/src/cmds/gwsubmit', 
    'gridway-5.8/src/cmds/gwps', 'gridway-5.8/src/cmds/gwkill', 'gridway-5.8/src/gwd/gwd', 
    'gridway-5.8/src/scheduler/gw_flood_scheduler', 'gridway-5.8/src/scheduler/gw_sched'])],
  version=drm4g.__version__,
  author='Santander Meteorology Group (UC-CSIC)',
  author_email='antonio.cofino@unican.es',
  url='https://meteo.unican.es/trac/wiki/DRM4G',
  license='European Union Public License 1.1',
  description='DRM4G is an open platform for DCIs.',
  long_description=long_description,
  long_description_content_type='text/markdown',
  classifiers=[
    "Intended Audience :: Science/Research",
    "Programming Language :: Python",
    "Topic :: Scientific/Engineering",
    "Topic :: Office/Business :: Scheduling",
    "Programming Language :: Python :: 2.6",
    "Programming Language :: Python :: 2.7",
    "Programming Language :: Python :: 3.3",
    "Programming Language :: Python :: 3.4",
    "Programming Language :: Python :: 3.5",
    "Programming Language :: Python :: 3.6",
    "Programming Language :: Python :: 3.7",
    "Programming Language :: Python :: 3.8",
  ],
  install_requires=['fabric', 'docopt', 'openssh-wrapper'],
  scripts=bin_scripts,
  cmdclass={
    'install': build_wrapper,
  },
)