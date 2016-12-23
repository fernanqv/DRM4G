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

"""
Manage identities for resources. That involves managing private/public keys
and grid credentials, depending on the resource configuration.

Usage:
    drm4g id <resource_name> init   [ options ] [ --lifetime=<hours> ]
    drm4g id <resource_name> info   [ options ]
    drm4g id <resource_name> delete [ options ]

 Options:
    -l --lifetime=<hours>   Duration of the identity's lifetime [default: 168].
    -d --debug              Debug mode.

Commands:

    init                    Create an identity for a certain period of time, by
                            default 168 hours (1 week). Use the option --lifetime
                            to modify this value. It adds the configured private
                            key to a ssh-agent and creates a grid proxy using
                            myproxy server.
                            It appends the public key to the remote user's
                            ~/.ssh/authorized_keys file (creating the file, and
                            directory, if necessary). It tries to load the public
                            key obtained by appending *.pub to the name of the
                            configured private key file. Alternative the public
                            key can be given by public_key variable.
                            It also configures the user's grid certificate
                            under ~/.globus directory (creating directory,
                            if necessary) if grid_cert variable is defined.

    info                    Get some information about the identity's status.

    delete                  Remove the identity from the ssh-agent and the
                            myproxy server.
"""

import logging
from os.path              import expanduser, exists, expandvars, join
from drm4g.core.configure import Configuration
from drm4g.commands       import exec_cmd, Daemon, Agent, Proxy
from drm4g                import logger, DRM4G_DIR

def run( arg ) :
    try :
        daemon = Daemon()
        if not daemon.is_alive() :
           raise Exception( 'DRM4G is stopped.' )
        config = Configuration()
        config.load( )
        if config.check( ) :
            raise Exception( "Review the configuration of '%s'." % ( arg['<resource_name>'] ) )
        if arg['<resource_name>'] not in config.resources :
            raise Exception( "'%s' is not a configured resource." % ( arg['<resource_name>'] ) )
        lrms         = config.resources.get( arg['<resource_name>'] )[ 'lrms' ]
        communicator = config.resources.get( arg['<resource_name>'] )[ 'communicator' ]
        if lrms != 'cream' and lrms != 'rocci' and ( communicator != 'pk_ssh' or communicator != 'op_ssh' ) :
            raise Exception( "'%s' does not have an identity to configure." % ( arg['<resource_name>'] ) )
        if lrms == 'cream' or lrms == 'rocci' :
            comm = config.make_communicators()[ arg['<resource_name>'] ]
            if communicator == 'op_ssh' :
                #comm.parent_module = 'id'
                #comm.configfile = join(DRM4G_DIR,'etc','openssh_id.conf')
                #paramiko will always be used to renew the grid certificate
                communicator = 'pk_ssh'
                config.resources.get( arg['<resource_name>'] )[ 'communicator' ] = 'pk_ssh'
                comm = config.make_communicators()[ arg['<resource_name>'] ]
                #~logger.debug( "COMMUNICATOR IS %s" % comm.communicator )
                #config.resources.get( arg['<resource_name>'] )[ 'communicator' ] = 'op_ssh'
            proxy = Proxy( config.resources[ arg['<resource_name>'] ] ,
                           comm
                           )
            config.resources.get( arg['<resource_name>'] )[ 'communicator' ] = 'op_ssh'
            config.make_communicators()
        if communicator != 'local' :
            agent = Agent( config.resources[ arg['<resource_name>'] ] )
        if arg[ 'init' ] :
            if communicator != 'local' :
                agent.start( )
                agent.add_key( arg[ '--lifetime' ] )
                agent.copy_key( )
            if lrms == 'cream' or lrms == 'rocci' :
                proxy.configure( )
                proxy.create( arg[ '--lifetime' ] )
        elif arg[ 'delete' ] :
            if lrms == 'cream' or lrms == 'rocci' :
                proxy.destroy( )
            if communicator != 'local' :
                agent.delete_key( )
        else :
            if communicator != 'local' :
                agent.list_key( )
            if lrms == 'cream' or lrms == 'rocci' :
                proxy.check( )
    except Exception as err :
        logger.error( str( err ) )
