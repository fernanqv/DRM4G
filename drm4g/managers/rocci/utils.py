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

import os
import re
import json
import logging
import subprocess

__version__  = '2.6.0'
__author__   = 'Carlos Blanco and Antonio Minondo'
__revision__ = "$Id$"

def exec_cmd( cmd, stdin = subprocess.PIPE, stdout = subprocess.PIPE,
              stderr = subprocess.PIPE, env = os.environ ):
    """
    Execute shell commands.
    """
    logging.info( "Executing command ... " + cmd )
    if type( stdin ) == str :
        p      = subprocess.Popen( cmd, shell = True, stdout = stdout,
                                   stdin = subprocess.PIPE, stderr = stderr, env = env )
        p.stdin.write( stdin + "\n" )
        p.stdin.flush()
    else :
        p      = subprocess.Popen( cmd, shell = True, stdout = stdout,
                               stdin = stdin, stderr = stderr, env = env )
    output = p.stdout.read().strip() + p.stderr.read().strip()
    code   = p.wait()
    return code, output

def is_ip_private(ip):
    """
    Determinate if an IP is private
    """
    priv_lo = re.compile("^127\.\d{1,3}\.\d{1,3}\.\d{1,3}$")
    priv_24 = re.compile("^10\.\d{1,3}\.\d{1,3}\.\d{1,3}$")
    priv_20 = re.compile("^192\.168\.\d{1,3}.\d{1,3}$")
    priv_16 = re.compile("^172.(1[6-9]|2[0-9]|3[0-1]).[0-9]{1,3}.[0-9]{1,3}$")
    return True if priv_lo.match(ip) or priv_24.match(ip) or priv_20.match(ip) or priv_16.match(ip) else False

def download_proxy( myproxy_server, myproxy_password, vo, proxy_file, username ) :
    """
    Function to download proxies.
    """
    cmd = "MYPROXY_SERVER=%s myproxy-logon --voms %s -o %s --username %s -S" % ( myproxy_server, vo, proxy_file, username )
    return exec_cmd( cmd, stdin = myproxy_password )

def voms_proxy( proxy, vo, signed_proxy ) :
    """
    Function to sign proxies.
    """
    cmd = "X509_USER_PROXY=%s voms-proxy-init -ignorewarn " \
          "-timeout 30 -valid 24:00 -q -voms %s -noregen -out %s" % ( proxy, vo, signed_proxy )
    return exec_cmd( cmd )

def generate_key( file_name ) :
    """
    Function to generate an authentication key.
    """
    cmd = 'rm -rf %s; ssh-keygen -t rsa -b 2048 -f %s -N ""' % ( file_name, file_name )
    return exec_cmd( cmd )

def read_key( file_name ) :
    """
    Function to read a file key.
    """
    with open( file_name, "r" ) as file :
        pub = file.readline().rstrip()
    return pub

def save_pkl( obj_config, file_name ) :
    """
    Save a python object into a pickle file.
    """
    with open( file_name, "w" ) as f :
        pickle.dump( obj_config, f )

def load_pkl( file_name ) :
    """
    Load a python object back from the pickle file.
    """
    with open( file_name, "r" ) as f :
        return pickle.load( f )

def _decode_list(data):
    rv = []
    for item in data:
        if isinstance(item, unicode):
            item = item.encode('utf-8')
        elif isinstance(item, list):
            item = _decode_list(item)
        elif isinstance(item, dict):
            item = _decode_dict(item)
        rv.append(item)
    return rv

def _decode_dict(data):
    rv = {}
    for key, value in data.iteritems():
        if isinstance(key, unicode):
            key = key.encode('utf-8')
        if isinstance(value, unicode):
            value = value.encode('utf-8')
        elif isinstance(value, list):
            value = _decode_list(value)
        elif isinstance(value, dict):
            value = _decode_dict(value)
        rv[key] = value
    return rv

def save_json( obj_config, file_name ) :
    """
    Save a python object into a json file.
    """
    with open( file_name, "w" ) as f :
        json.dump( obj_config, f )

def load_json( file_name ) :
    """
    Load a python object back from the json file.
    """
    with open( file_name, "r" ) as f :
        return json.load( f, object_hook=_decode_dict)

