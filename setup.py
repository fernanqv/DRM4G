from setuptools import setup
from setuptools import find_packages
from distutils.command.install import install as DistutilsInstall
from os import path
import os
import subprocess
import glob
import sys
import ast
import shutil #this package might not work on Mac
#from distutils.core import setup
#from distutils.command.build import build

try: 
    input = raw_input
except NameError:
    pass

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

here = path.abspath(path.dirname(__file__))
python_ver=sys.version[:3]
user_shell=os.environ['SHELL']

#I consider bash a special case because of what is said here http://superuser.com/questions/49289/what-is-the-bashrc-file
if 'bash' in user_shell:
    user_shell='.bashrc'
else:
    user_shell='.profile'

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

def yes_no_choice( message,  default = 'y') :
    """
    Ask for Yes/No questions
    """
    choices = 'Y/n' if default.lower() in ( 'y', 'yes' ) else 'y/N'
    values = ( 'y', 'yes', 'n', 'no' )
    choice = ''
    while not choice.strip().lower() in values:
        choice = input( "%s \n(%s) " % ( message, choices ) )
    return choice.strip().lower()

class Builder(object):

    export_dir=''
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
                lib_dir=''
                #I'm working under the impression that the path passed on to prefix has to be an absolute path - for the moment, if you use a relative path, gridway's binary files will be copied to a directory relative to where ./gridway-5.8 is
                if '=' in option:
                    #this is assuming that spaces are not permited (--prefix=/home) TESTED
                    #self.prefix_directory=option #now that I added 'home' in the if I can't have it like this, since configure doesn't recognize --home as an option
                    self.export_dir=option[option.find('=')+1:] #substring that goes from the position after the '=' till the end 
                    self.prefix_directory='--prefix '+self.export_dir
                else:
                    #self.prefix_directory=option+' '+self.arguments[i+1]
                    self.export_dir=self.arguments[i+1]
                    self.prefix_directory='--prefix '+self.export_dir

                if '--prefix' in option:
                    lib_dir=os.path.join(self.export_dir,'lib/python{}/site-packages'.format(python_ver))
                elif '--home' in option:
                    lib_dir=os.path.join(self.export_dir,'lib/python')

                try:
                    os.makedirs(lib_dir)
                except OSError:
                    print('\nDirectory {} already exists'.format(lib_dir))

                #ahora seria crear un fichero drm4g.pth en site-packages/drm4g, el fichero tendra una sloa linea 'drm4g'
                #depues llamar al metodo site.addsitedir para que lo ejecute
                '''
                site.addsitedir(sitedir, known_paths=None)

                    Add a directory to sys.path and process its .pth files. Typically used in sitecustomize or usercustomize (see above).
                '''

                message="\nWe are about to modify your {} file.\n" \
                    "If we don't you'll have to define PYTHONPATH" \
                    " or manually find and execute DRM4G everytime you wish to use it.\n" \
                    "Do you want us to continue?".format(user_shell)

                ans=yes_no_choice(message)
                if ans[0]=='y':
                    #when installing they'll have to define the variable PYTHONPATH, but with this, they won't have to do it again in order to use DRM4G
                    #self.call('echo "export PYTHONPATH={}:$PYTHONPATH" >> ~/{}'.format(self.export_dir,user_shell))
                    home=os.path.expanduser('~') #to ensure that it will find $HOME directory in all platforms
                    with open('{}/{}'.format(home,user_shell),'a') as f:
                        f.write('export PYTHONPATH={}:$PYTHONPATH'.format(self.export_dir))

    def build(self):
        gridway=path.join( here, "gridway-5.8")
        current_path = os.getcwd()

        try:
            self.prefix_option()
            
            if not path.exists(gridway) :
                raise Exception("The specified directory %s doesn't exist" % gridway)

            os.chdir( gridway )

            exit_code = self.call( './configure' )
            if exit_code:
                raise Exception("Configure failed - check config.log for more detailed information")
            
            exit_code = self.call('make')
            if exit_code:
                raise Exception("make failed")

            drm4g_bin=path.join(self.export_dir,'bin')
            if not path.exists(drm4g_bin) : 
                #raise Exception("The specified directory %s doesn't exist" % path.join(self.export_dir,'bin'))
                try:
                    os.makedirs(drm4g_bin)
                except OSError:
                    print('\nDirectory {} already exists'.format(drm4g_bin))

            files=glob.glob('src/cmds/gw[!_]*') #files=glob.glob('src/cmds/gw[^_]*')
            print files
            for f in files:
                shutil.copy(f,drm4g_bin)
            files=['src/gwd/gwd','src/scheduler/gw_flood_scheduler','src/scheduler/gw_sched']
            for f in files:
                #shutil.copy(path.join(gridway,f),path.join(self.export_dir,'bin'))
                shutil.copy(f,path.join(self.export_dir,'bin'))

            '''
            this function works but it copies a few unnecessary files as well
            dirs=['src/cmds','src/gwd','src/scheduler']
            def copy_binary(dirs):
                 for d in dirs:
                     files=os.listdir(d)
                     for f in files:
                         if os.path.isfile(os.path.join(d,f)):
                             if mimetypes.guess_type(os.path.join(d,f))[0] == None:
                                 shutil.copy(os.path.join(d,f),os.path.join(self.export_dir,'bin'))
            '''

            '''
            exit_code = self.call('make install')
            if exit_code:
                raise Exception("make install failed")
            '''            
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

class build_wrapper(DistutilsInstall):
    def run(self):
        Builder().build()
        DistutilsInstall.run(self)

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
        "Programming Language :: Python :: 3.3",
        "Programming Language :: Python :: 3.4",
        "Programming Language :: Python :: 3.5",
        "Programming Language :: Python :: 3.6",
    ],
    install_requires=['paramiko<2.0', 'docopt' ],
    scripts=bin_scripts,
    #data_files=[('etc', conf_files)],
    cmdclass={
        #'build': build_wrapper,
        'install': build_wrapper,
    },
)

