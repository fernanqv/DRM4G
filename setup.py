from setuptools import setup
from setuptools import find_packages
from distutils.command.install import install as DistutilsInstall
from os import path
import os
import subprocess
import glob
import sys
import ast
#from distutils.core import setup
#from distutils.command.build import build

#To ensure a script runs with a minimal version requirement of the Python interpreter
#assert sys.version_info >= (2,5)
'''
>>> from platform import python_version
>>> python_version()
'2.7.12'

>>> print sys.version_info
sys.version_info(major=2, minor=7, micro=12, releaselevel='final', serial=0)

>>> sys.version
'2.7.12 (default, Jul  1 2016, 15:12:24) \n[GCC 5.4.0 20160609]'
'''

python_ver=sys.version[:3]
user_shell=os.environ['SHELL']

#I make bash a special case because of what is said here http://superuser.com/questions/49289/what-is-the-bashrc-file
if 'bash' in user_shell:
    user_shell='.bashrc'
else:
    user_shell='.profile'

here = path.abspath(path.dirname(__file__))


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

    prefix_directory=''
    arguments=str(sys.argv)
    arguments=ast.literal_eval(arguments) #convert from string to list

    #def __init__(self, lib):''

    def call(self, cmd):
        return subprocess.call(cmd, shell=True)

    def prefix_option(self):
        #Going through the whole list since the options can be defined in different ways (--prefix=dir> or --prefix <dir>)
        #Which is why I'm not using self.arguments.index('--prefix') to find it, since it doesn't check if it's a substring
        #Could also do it with a while and make it stop if it finds '--prefix' or '--home'
        for i in range(len(self.arguments)):
            option=self.arguments[i]
            #folder name can't contain '--prefix' or '--home'
            if '--prefix' in option or '--home' in option:
                export_dir=''
                lib_dir=''
                #I'm working under the impression that the path passed on to prefix has to be an absolute path CHECK IT
                if '=' in option:
                    #this is assuming that spaces are not permited (--prefix=/home) TESTED
                    #self.prefix_directory=option #now that I added 'home' in the if I can't have it like this, since configure doesn't recognize --home as an option
                    export_dir=option[option.find('=')+1:] #substring that goes from the position after the '=' till the end 
                    self.prefix_directory='--prefix '+export_dir
                else:
                    #self.prefix_directory=option+' '+self.arguments[i+1]
                    export_dir=self.arguments[i+1]
                    self.prefix_directory='--prefix '+export_dir

                if '--prefix' in option:
                    #maybe it would be better to just use export_dir instead of lib_dir
                    lib_dir=os.path.join(export_dir,'lib/python{}/site-packages'.format(python_ver))
                elif '--home' in option:
                    lib_dir=os.path.join(export_dir,'lib/python')

                try:
                    os.makedirs(lib_dir)
                except OSError:
                    print('\nDirectory already exists\n')
                '''
                Creo que es este el que me ha soltado este error
                antonio@diluvio:~/Documentos/drm4g_source/build_3/source_code$ python setup.py install --prefix=/home/antonio/Documentos/drm4g_source/build_3/drm4g
                running install
                #####################
                AN ERROR OCCURED WHILE DOING THE BUILD
                #####################
                [Errno 17] File exists: '/home/antonio/Documentos/drm4g_source/build_3/drm4g/lib/python2.7/site-packages'
                error: [Errno 17] File exists: '/home/antonio/Documentos/drm4g_source/build_3/drm4g/lib/python2.7/site-packages'
                '''
                #ahora seria crear un fichero drm4g.pth en site-packages/drm4g, el fichero tendra una sloa linea 'drm4g'
                #depues llamar al metodo site.addsitedir para que lo ejecute
                '''
                site.addsitedir(sitedir, known_paths=None)

                    Add a directory to sys.path and process its .pth files. Typically used in sitecustomize or usercustomize (see above).
                '''

                '''
                python_path=os.environ.get('PYTHONPATH')
                if python_path:
                    python_path=python_path.split(':') #os.environ.get returns a string of paths separated by ':', here it's being converted to a list
                    if not lib_dir in python_path:
                        #if lib_dir happens to be a subpath(substring) from a path(string) in the variable, this will still give true (which is bad)


                if not export_dir in sys.path:
                    #sys.path.append('lib_dir') #didn't work
                    self.call('echo "export PYTHONPATH={}:$PYTHONPATH" >> ~/{}'.format(export_dir,user_shell))
                    #self.call('source ~/.bashrc') #need to reload .bashrc so that PYTHONPATH gets defined
                
                #as I said before, it didn't work
                print '\n\n\n#############\n'+','.join(sys.path)+'\n#############\n\n'+lib_dir+'\n'+sys.executable
                #sys.path.append(lib_dir)
                sys.path.insert(1,lib_dir)
                print '\n\n\n#############\n'+','.join(sys.path)+'\n#############\n\n'+lib_dir+'\n'+sys.executable
                '''
                #RAW_INPUT - 2.7 ---- INPUT - 3.5
                ans=''
                print("\nWe are about to modify your {} file.\n" \
                    "If we don't you'll have to define PYTHONPATH" \
                    " or manually find and execute DRM4G everytime you wish to use it.\n" \
                    "Do you want us to continue?\ny -- yes\nn -- no".format(user_shell))
                if python_ver[0] == '2':
                    while((ans!='y') and (ans!='Y') and (ans!='yes') and (ans!='Yes') and (ans!='n') and (ans!='N') and (ans!='no') and (ans!='No')):
                        ans=raw_input()
                else:
                    while((ans!='y') and (ans!='Y') and (ans!='yes') and (ans!='Yes') and (ans!='n') and (ans!='N') and (ans!='no') and (ans!='No')):
                        ans=input()
                
                if((ans=='y') or (ans=='Y') or (ans=='yes') or (ans=='Yes')):
                    #when installing they'll have to define the variable PYTHONPATH, but with this, they won't have to do it again in order to use DRM4G
                    self.call('echo "export PYTHONPATH={}:$PYTHONPATH" >> ~/{}'.format(export_dir,user_shell))

            #if '--home' in option:
                
        #print '\n\n\n\n{}\n\n\n\n'.format(prefix_directory)


    [] #I don't know what's this doing here (it was uncommented)
    def build(self):
        # self.call('sudo rm %s' % filename) --- filename = '/usr/local/lib/libhdf5.so.8.0.1
        #cd gridway-5.8; ./configure --prefix=$HOME/Documentos/drm4g_source/build_3/drm4g; make; make install; cd ..; cp ./bin/* $HOME/Documentos/drm4g_source/build_3/drm4g/bin; 
        #export PYTHONPATH=%s:$PYTHONPATH && -- lo anadi antes porque me d
        #self.call('export PYTHONPATH=%s:$PYTHONPATH' % drm4g_install_dir) #se necesita antes - That .pth error is a build check that python-setuptools has.
        #self.call('echo $PYTHONPATH')
        #self.call(('cd gridway-5.8 && ./configure %s && make && make install && make clear') % (config['post']))
        gridway=path.join( here, "gridway-5.8")
        current_path = os.getcwd()
        try:

            self.prefix_option()
            
            if not path.exists(gridway) :
                raise Exception("The specified directory %s doesn't exist" % gridway)

            os.chdir( gridway )


            #self.call('pwd')
            #print '\n\n\n\n{}\n\n\n\n'.format(self.prefix_directory)
            exit_code = self.call('./configure %s' % self.prefix_directory)
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


'''class build_wrapper(build):
    def initialize_options(self):
        # Deploy all the described libraries in the BINARIES dictionary.
        #libs='drm4g'
        #build_lib = lambda lib: Builder(lib).build()
        #build_lib = lambda lib: Builder().build()
        #map(build_lib, libs)
        #build_lib(libs)
        Builder().build()
        return build.initialize_options(self)
'''
class build_wrapper(DistutilsInstall):
    def run(self):
        Builder().build()
        DistutilsInstall.run(self)

#version='2.0', # or the method found in netcdf's implementation could work 'calculate_version'
#packages=['drm4g'], #probably shouldn't add this (I had gridway-5.8 in there, but this is only to address pure Python modules, not C)
#py_modules=,['bin.drm4g.py'], #no since there is no __init__.py in the folder
#hacia falta instalar/incluir un header file?
#requires='paramiko<2.0',
#2.7 Installing Additional Files, me incumbe?
bin_scripts= glob.glob(os.path.join('bin', '*'))
#conf_files= glob.glob(os.path.join('etc', '*'))

setup(
    name='drm4g',
    packages=find_packages(),
    include_package_data=True,
    package_data={'drm4g' : ['conf/*.conf','conf/job_template.default']},
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
        "Programming Language :: Python :: 3.4",
        "Programming Language :: Python :: 3.5",
        "Programming Language :: Python :: 3.6",
    ],
    install_requires=['paramiko<2.0',],
    scripts=bin_scripts,
    #data_files=[('etc', conf_files)],
    cmdclass={
        #'build': build_wrapper,
        'install': build_wrapper,
    },
)