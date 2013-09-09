import cmd
import os
import drm4g.core.configure
from drm4g import REMOTE_VOS_DIR

__version__  = '1.0'
__author__   = 'Carlos Blanco'
__revision__ = "$Id:$"

class ManagementUtility( cmd.Cmd ):
    """
    Encapsulates the logic of the drm4g.py utilities.
    """
    prompt = "> "
    
    config = drm4g.core.configure.Configuration()

    def do_myproxy_check(self, line):
        """
        Check if user certificate is valid.
        """
        self.do_check_resources( line )
        if not self.config.resources.has_key( line ) :
            print "'%s' is not a resource. The resources avaible are: " % line
            self.do_list_resources( line )
        else :
            resource  = self.config.resources[ line ]
            if not resource.has_key( 'myproxy_server' ) :
                print "Please, check '%s' configuration. The key 'myproxy_server' is not available" % line
            else :
                communicator = self.config.make_communicators()[ line ]
                communicator.connect()
                cmd = "X509_USER_PROXY=%s/proxy MYPROXY_SERVER=%s myproxy-info" % ( REMOTE_VOS_DIR , resource[ 'myproxy_server' ] )
                print "Executing command ... ", cmd 
                out, err = communicator.execCommand( cmd )
                communicator.close()
                print out, err
    
    def do_myproxy_upload(self, line ):
        """
        It uploads the credential to a myproxy-server.
        """
        import getpass
        self.do_check_resources( line )
        if not self.config.resources.has_key( line ) :
            print "'%s' is not a resource. The resources avaible are: " % line
            self.do_list_resources( line )
        else :
            resource  = self.config.resources[ line ]
            if not resource.has_key( 'myproxy_server' ) :
                print "Please, check '%s' configuration. The key 'myproxy_server' is not available" % line
            else :    
                communicator = self.config.make_communicators()[ line ]
                communicator.connect()
                print "Creating '%s' directory to store the proxy ... " % REMOTE_VOS_DIR
                cmd = "mkdir -p %s" % REMOTE_VOS_DIR
                print "Executing command ... ", cmd 
                communicator.execCommand( cmd )
                out, err = communicator.execCommand( cmd )
                if not err :
                    print out
                
                    message      = 'Insert your GRID pass: '
                    grid_passwd  = getpass.getpass(message)
        
                    message      = 'Insert MyProxy password: '
                    proxy_passwd = getpass.getpass(message)
        
                    cmd = "MYPROXY_SERVER=%s myproxy-init" % resource[ 'myproxy_server' ]
                    print "Executing command ... ", cmd 
                    out , err = communicator.execCommand( cmd , input = '\n'.join( [ grid_passwd, proxy_passwd, proxy_passwd ] ) )
                    communicator.close()
                    print out , err
                else :
                    communicator.close()
                    print err  

    def do_myproxy_download(self, line ):
        """
        It  retrieves  a  proxy  credential  from  the myproxy-server.
        """
        import getpass
        self.do_check_resources( line )
        if not self.config.resources.has_key( line ) :
            print "'%s' is not a resource. The resources avaible are: " % line
            self.do_list_resources( line )
        else :
            resource  = self.config.resources[ line ]
            if not resource.has_key( 'myproxy_server' ) :
                print "Please, check '%s' configuration. The key 'myproxy_server' is not available" % line
            else :    
                communicator = self.config.make_communicators()[ line ]
                communicator.connect()
                
                message      = 'Insert MyProxy password: '
                proxy_passwd = getpass.getpass(message)
        
                cmd = "X509_USER_PROXY=%s/proxy MYPROXY_SERVER=%s myproxy-logon" % ( 
                                                                                REMOTE_VOS_DIR , 
                                                                                resource[ 'myproxy_server' ] 
                                                                                )
                print "Executing command ... ", cmd 
                out, err = communicator.execCommand( cmd , input = proxy_passwd )
                communicator.close()
                print out , err  
        
    def do_check_frontends(self , line ):
        """
        Check if all frontends are reachable.
        """
        self.do_check_resources( line )
        communicators = self.config.make_communicators()
        for resname, communicator in communicators.iteritems() :
            print "  --> Resource '%s' ... " % resname
            communicator.connect()
            communicator.close()

    def do_list_resources(self, line):
        """
        Check if the drm4g.conf file has been configured well and list the resources available.
        """
        for resname, resdict in self.config.resources.iteritems() :
            print "  --> Resource '%s' ... " % resname
            for key , val in resdict.iteritems() :
                print "    --> '%s' : '%s' " % ( key , val )

    def do_check_resources(self, line):
        """
        Check if the drm4g.conf file has been configured well.
        """
        self.config.load()
        errors = self.config.check()
        if errors :
            print "Please, review your configuration file"
    
    def do_exit (self , line ):
        """
        Quits the console.
        """
        return True

    do_EOF = do_exit

def execute_from_command_line( argv ):
    """
    A method that runs a ManagementUtility.
    """
    if len( argv ) > 1:
        ManagementUtility().onecmd( ' '.join( argv[ 1: ] ) )
    else:
        ManagementUtility().cmdloop()

