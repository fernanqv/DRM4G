from setuptools import setup
from setuptools import find_packages
from distutils.command.build import build
from os import path
import os
import subprocess
import glob
import sys
import ast


here = path.abspath(path.dirname(__file__))

directory=''
arguments=str(sys.argv)
arguments=ast.literal_eval(arguments) #convert from string to list

#Going through the whole list since the options can be defined in different ways (--prefix=dir> or --prefix <dir>)
#Could also do it with a while and make it stop if it finds '--prefix' or '--home'
for i in range(len(arguments)):
    option=arguments[i]
    if '--prefix' in option or '--home' in option:
        #TODO
        if '=' in option:
            #this is assuming that spaces are not permited TEST IT!! (--prefix=/home)
            #directory=option[option.find('=')+1:] #substring that goes from the position after the '=' till the end 
            directory=arguments[i]
        else:
            directory=arguments[i+1]

#print '\n\n\n\n{}\n\n\n\n'.format(directory)

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


class Builder(object):

    def __init__(self, lib):''

    def call(self, cmd):
        return subprocess.call(cmd, shell=True)

    []
    def build(self):
        # self.call('sudo rm %s' % filename) --- filename = '/usr/local/lib/libhdf5.so.8.0.1
        #cd gridway-5.8; ./configure --prefix=$HOME/Documentos/drm4g_source/build_3/drm4g; make; make install; cd ..; cp ./bin/* $HOME/Documentos/drm4g_source/build_3/drm4g/bin; 
        #export PYTHONPATH=%s:$PYTHONPATH && -- lo anadi antes porque me d
        #self.call('export PYTHONPATH=%s:$PYTHONPATH' % drm4g_install_dir) #se necesita antes - That .pth error is a build check that python-setuptools has.
        #self.call('echo $PYTHONPATH')
        #self.call(('cd gridway-5.8 && ./configure %s && make && make install && make clear') % (config['post']))
        gridway=path.join( here, "gridway-5.8")
        try:
            if not path.exists(gridway) :
                raise Exception("The specified directory %s doesn't exist" % gridway)

            current_path = os.getcwd()
            os.chdir( gridway )


            self.call('pwd')
            exit_code = self.call('./configure %s' % directory)
            if exit_code:
                raise Exception("Configure failed - check config.log for more detailed information")
            
            exit_code = self.call('make')
            if exit_code:
                raise Exception("make failed")
            
            exit_code = self.call('make install')
            if exit_code:
                raise Exception("make install failed")
                        
            exit_code = self.call('make clean')
            if exit_code:
                print("make clean failed")
            
        except Exception as exc:
            print('#####################\nAN ERROR OCCURED WHILE DOING THE BUILD\n#####################')
            print(exc)
            raise
        finally:
            os.chdir( current_path )


        
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
bin_scripts= glob.glob(os.path.join('bin', '*'))
conf_files= glob.glob(os.path.join('etc', '*'))

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
        "Programming Language :: Python :: 2.6",
        "Programming Language :: Python :: 2.7",
        "Programming Language :: Python :: 3.5",
        "Programming Language :: Python :: 3.6",
    ],
    install_requires=['paramiko<2.0',],
    scripts=bin_scripts,
    data_files=[('etc', conf_files)],
    cmdclass={
        'build': build_wrapper,
    },
)