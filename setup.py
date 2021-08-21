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

from setuptools import setup
from setuptools import find_packages
from setuptools.command.install import install
from setuptools.command.build_ext import build_ext
from setuptools.command.develop import develop
import os
import subprocess
import glob
import sys
import stat
from pprint import pprint

#To ensure a script runs with a minimal version requirement of the Python interpreter
#assert sys.version_info >= (2,5)
if sys.version_info<(3,5):
  exit( 'The version number of Python has to be >= 3.5' )

here = os.path.abspath(os.path.dirname(__file__))
gridway_src = "gridway-5.8"
  
# read the contents of your README file
with open(os.path.join(here, 'README'), encoding='utf-8') as f:
  long_description = f.read()

MAKE_CLEAN = False
def build():
  current_path = os.getcwd()
  if not os.path.exists(gridway_src) :
      raise Exception("The specified directory %s doesn't exist" % gridway_src)
  
  #setting same mtime to gridway sources
  mtime_ref = os.path.getmtime(gridway_src)
  for root, dirs, files in os.walk(gridway_src):
    for name in files + dirs:
      os.utime(os.path.join(root, name), (mtime_ref, mtime_ref))
  os.utime(gridway_src, (mtime_ref, mtime_ref))
  #

  os.chdir( gridway_src )
  #to avoid re-run configure each time.
  if(not os.path.isfile('config.log') or os.path.getmtime('config.log') <= os.path.getmtime('configure') ):
    st = os.stat('configure')
    os.chmod('configure', st.st_mode | stat.S_IEXEC)
    os.makedirs('build',exist_ok=True)
    buildPath = os.path.abspath('build')
    exit_code = subprocess.call('./configure --prefix="%s"' % buildPath, shell=True)
    if exit_code:
      raise Exception("Configure failed - check config.log for more detailed information")
  
  exit_code = subprocess.call('make', shell=True)
  if exit_code:
    raise Exception("make failed")
  exit_code = subprocess.call('make install', shell=True)
  if exit_code:
    raise Exception("make install failed")
  if MAKE_CLEAN:
    exit_code = subprocess.call('make clean', shell=True)
    if exit_code:
      raise Exception("make clean failed")
  os.chdir( current_path )

gw_files = ('bin',
    [
      gridway_src + '/build/bin/gwuser',
      gridway_src + '/build/bin/gwacct',
      gridway_src + '/build/bin/gwwait',
      gridway_src + '/build/bin/gwhost',
      gridway_src + '/build/bin/gwhistory',
      gridway_src + '/build/bin/gwsubmit',
      gridway_src + '/build/bin/gwps',
      gridway_src + '/build/bin/gwkill',
      gridway_src + '/build/bin/gwd',
      gridway_src + '/build/bin/gw_flood_scheduler',
      gridway_src + '/build/bin/gw_sched',
    ])

#OLD WAY
#gw_files = ('bin',
#    [
#      gridway_src + '/src/cmds/gwuser',
#      gridway_src + '/src/cmds/gwacct',
#      gridway_src + '/src/cmds/gwwait',
#      gridway_src + '/src/cmds/gwhost',
#      gridway_src + '/src/cmds/gwhistory',
#      gridway_src + '/src/cmds/gwsubmit',
#      gridway_src + '/src/cmds/gwps',
#      gridway_src + '/src/cmds/gwkill',
#      gridway_src + '/src/gwd/gwd',
#      gridway_src + '/src/scheduler/gw_flood_scheduler',
#      gridway_src + '/src/scheduler/gw_sched',
#    ])

#pprint(vars(self))
class build_ext_wrapper(build_ext):
  def run(self):
    print("[running build_ext_wrapper.run() ...]")
    if(self.verbose):
      pprint(vars(self))
    build()
    build_ext.run(self)

class install_wrapper(install):
  def run(self):
    print("[running install_wrapper.run() ...]")
    if(self.verbose):
      pprint(vars(self))
    build()
    install.run(self)

class develop_wrapper(develop):
  def run(self):
    print("[running develop_wrapper.run() ...]")
    develop.run(self)
    if(self.verbose):
      pprint(vars(self))
    for filename in gw_files[1]:
      dst = os.path.join(self.script_dir, os.path.basename(filename))
      if(os.path.lexists(dst)):
        if(self.verbose):
          print("[removing %s]" % dst)
        os.remove(dst)
      src = os.path.abspath(filename)
      if(not self.uninstall):
        if(self.verbose):
          print("[creating symlink: %s -> %s]" % (dst, src))
        os.symlink(src,dst)


bin_scripts = glob.glob(os.path.join('bin', '*'))
#bin_scripts.append('LICENSE')

# FROM: https://github.com/jbweston/miniver
def get_version_and_cmdclass(package_name):
    import os
    from importlib.util import module_from_spec, spec_from_file_location
    spec = spec_from_file_location('version',
                                   os.path.join(package_name, '_version.py'))
    module = module_from_spec(spec)
    spec.loader.exec_module(module)
    return module.__version__, module.cmdclass
version, cmdclass = get_version_and_cmdclass('drm4g')

setup(
  name='drm4g',
  version=version,
  python_requires=">=3.5",
  packages=find_packages(),
  include_package_data=True,
  package_data={'drm4g' : ['conf/*.conf', 'conf/job_template.default', 'conf/*.sh']},
  data_files = [gw_files],
  author='Santander Meteorology Group (UC-CSIC)',
  author_email='antonio.cofino@unican.es',
  url='https://github.com/SantanderMetGroup/DRM4G',
  project_urls = {
    'Documentation' : 'https://meteo.unican.es/trac/wiki/DRM4G'           ,
    'Source'        : 'https://github.com/SantanderMetGroup/DRM4G'        ,
    'Tracker'       : 'https://github.com/SantanderMetGroup/DRM4G/issues' ,
    'Download'      : 'https://pypi.org/project/drm4g/#files'             , 
    'Twitter'       : 'https://twitter.com/SantanderMeteo'

  },
  license='European Union Public License 1.1',
  description='Meta-scheduling framework for distributed computing infrastructures',
  long_description=long_description,
  long_description_content_type='text/markdown',
  classifiers=[
    "Development Status :: 4 - Beta",
    "Environment :: Console",
    "Intended Audience :: Science/Research",
    "License :: OSI Approved :: European Union Public Licence 1.1 (EUPL 1.1)",
    "Operating System :: POSIX",
    "Topic :: Scientific/Engineering",
    "Topic :: Office/Business :: Scheduling",
    "Programming Language :: Python",
    "Programming Language :: Python :: 3.6",
    "Programming Language :: Python :: 3.7",
    "Programming Language :: Python :: 3.8",
    "Programming Language :: Python :: 3.9",
  ],
  install_requires=['fabric', 'docopt', 'openssh-wrapper','scp'],
  scripts=bin_scripts,
  cmdclass={
    'build_ext' : build_ext_wrapper,
    'install'   : install_wrapper,
    'develop'   : develop_wrapper,
    'sdist'     : cmdclass['sdist'],
    'build_py'  : cmdclass['build_py'],
  },
)
