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

import socket
from drm4g                                  import REMOTE_VOS_DIR
from os.path                                import join
from time                                   import sleep 
from drm4g.managers import logger
#import logging
#logger = logging.getLogger(__name__)

def _renew_voms_proxy(com_object, myproxy_server, vo, cont=0):
    try:
        proxy_file = join( REMOTE_VOS_DIR , 'x509up.%s ' ) % vo
        
        logger.debug( "Running rocci's _renew_voms_proxy function" )
        logger.debug( "_renew_voms_proxy count = %s" % str( cont ) )
        logger.warning( "The proxy '%s' has probably expired" %  proxy_file )
        logger.info( "Renewing proxy certificate" )

        cmd = "rm %s" % proxy_file
        com_object.execCommand( cmd )
        if myproxy_server:
            LOCAL_X509_USER_PROXY = "X509_USER_PROXY=%s" % join ( REMOTE_VOS_DIR , myproxy_server )
        else :
            LOCAL_X509_USER_PROXY = "X509_USER_PROXY=%s/${MYPROXY_SERVER}" % ( REMOTE_VOS_DIR )
        cmd = "%s voms-proxy-init -ignorewarn " \
        "-timeout 30 -valid 24:00 -q -voms %s -noregen -out %s --rfc" % (
            LOCAL_X509_USER_PROXY ,
            vo ,
            proxy_file )

        logger.debug( "Executing command: %s" % cmd )
        out, err = com_object.execCommand( cmd )
        #log_output("_renew_voms_proxy", out, err)

        if err:
            logger.error( "Error renewing the proxy(%s): %s" % ( cmd , err ) )
            if cont<4:
                sleep(2.0)
                _renew_voms_proxy(com_object, proxy_file, myproxy_server, vo, cont+1)
            else:
                raise Exception("Probably the proxy certificate hasn't been created. Be sure to run the the following command before trying again:" \
                "\n    \033[93mdrm4g id <resource_name> init\033[0m")
        logger.info( "The proxy certificate will be operational for 24 hours" )
    except socket.timeout:
        logger.debug("Captured a socket.time exception")
        if cont<4:
            _renew_voms_proxy(com_object, proxy_file, myproxy_server, vo, cont+1)
        else:
            raise
        
        