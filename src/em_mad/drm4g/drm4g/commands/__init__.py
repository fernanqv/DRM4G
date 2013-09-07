import cmd
import os

__version__  = '1.0'
__author__   = 'Carlos Blanco'
__revision__ = "$Id:$"


class ManagementUtility( cmd.Cmd ):
    """
    Encapsulates the logic of the drm4g.py utilities.
    """
    prompt = "> "
    
    def do_shell(self, line):
        output = os.popen(line).read()
        print output
        
    def help_shell(self):
        print "Run a shell command"
    
    def do_quit (self , line ):
        return True

    def help_quit (self):
        print "Quits the console"

    do_EOF   = do_quit
    help_EOF = help_quit

def execute_from_command_line( argv ):
    """
    A method that runs a ManagementUtility.
    """
    if len( argv ) > 1:
        ManagementUtility().onecmd( ' '.join( argv[ 1: ] ) )
    else:
        ManagementUtility().cmdloop()

