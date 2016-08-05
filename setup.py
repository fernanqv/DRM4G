from setuptools import setup
from setuptools import find_packages
from distutils.command.build import build
import os
import subprocess
import glob


def get_long_description():
    readme_file = 'README'
    if not os.path.isfile(readme_file):
        return ''
    # Try to transform the README from Markdown to reStructuredText.
    try:
        import pandoc
        pandoc.core.PANDOC_PATH = 'pandoc'
        doc = pandoc.Document()
        doc.markdown = open(readme_file).read()
        description = doc.rst
    except Exception:
        description = open(readme_file).read()
    return description



config={'pre': '','post': '--prefix=$HOME/Documentos/drm4g_source/build_3/drm4g'}
drm4g_dir='$HOME/Documentos/drm4g_source/build_3/drm4g_dir'
drm4g_install_dir='/home/antonio/Documentos/drm4g_source/build_3/drm4g'

class Builder(object):

    def __init__(self, lib):''

    def call(self, cmd):
        return subprocess.call(cmd, shell=True)

    
    def build(self):
        # self.call('sudo rm %s' % filename) --- filename = '/usr/local/lib/libhdf5.so.8.0.1
        #cd gridway-5.8; ./configure --prefix=$HOME/Documentos/drm4g_source/build_3/drm4g; make; make install; cd ..; cp ./bin/* $HOME/Documentos/drm4g_source/build_3/drm4g/bin; 
        #export PYTHONPATH=%s:$PYTHONPATH && -- lo anadi antes porque me d
        #self.call('export PYTHONPATH=%s:$PYTHONPATH' % drm4g_install_dir) #se necesita antes - That .pth error is a build check that python-setuptools has.
        #self.call('echo $PYTHONPATH')
        self.call(('cd gridway-5.8 && ./configure %s && make && make install && make clear') % (config['post']))
        #self.call('echo "#####################\nSKIPPING CONFIGURE && MAKE\n#####################"')


class build_wrapper(build):
    def initialize_options(self):
        # Deploy all the described libraries in the BINARIES dictionary.
        libs='drm4g'
        build_lib = lambda lib: Builder(lib).build()
        #map(build_lib, libs)
        build_lib(libs)
        return build.initialize_options(self)


#version='2.0', # or the method found in netcdf's implementation could work 'calculate_version'
#packages=['drm4g'], #probably shouldn't add this (I had gridway-5.8 in there, but this is only to address pure Python modules, not C)
#py_modules=,['bin.drm4g.py'], #no since there is no __init__.py in the folder
#hacia falta instalar/incluir un header file?
#requires='paramiko<2.0',
#2.7 Installing Additional Files, me incumbe?
bin_scripts= glob.glob(os.path.join('bin', '*.sh'))

setup(
    name='drm4g',
    packages=find_packages(),
    version='2.0',
    author='Meteorology Group UC',
    author_email='josecarlos.blanco@unican.es',
    url='https://meteo.unican.es/trac/wiki/DRM4G',
    license='GNU Affero General Public License',
    description='Placeholder.',
    long_description = get_long_description(),
    classifiers=[
        "Intended Audience :: Science/Research",
        "Programming Language :: Python",
        "Topic :: Scientific/Engineering",
        "Topic :: Office/Business :: Scheduling",     
    ],
    install_requires=['paramiko<2.0',],
    scripts=bin_scripts,
    cmdclass={
        'build': build_wrapper,
    },
)