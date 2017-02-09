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

import sys
import os
import threading
import logging
import time
import pickle
import sqlite3
import subprocess
from drm4g                              import DRM4G_DIR
from threading                          import Thread
from drm4g.core.configure               import Configuration
from drm4g.managers.cloud_providers     import logger as log3
from drm4g.utils.message                import Send
from drm4g.managers.cloud_providers     import rocci
from math                               import ceil

pickled_file = os.path.join(DRM4G_DIR, "var", "rocci_pickled")
resource_conf_db = os.path.join(DRM4G_DIR, "var", "resource_conf.db")

class GwImMad (object):
    """
    Information manager MAD

    The format to send a request to the Information MAD, through its standard input, is:

        OPERATION HID HOST ARGS

    Where:
    -OPERATION: Can be one of the following:
        -INIT: Initializes the MAD (i.e. INIT - - -).
        -DISCOVER: Discovers hosts (i.e. DISCOVER - - - ).
        -MONITOR: Monitors a host (i.e. MONITOR HID HOST -).
        -FINALIZE: Finalizes the MAD (i.e. FINALIZE - - -).
    -HID: if the operation is MONITOR, it is a host identifier, chosen by GridWay. Otherwise it is ignored.
    -HOST: If the operation is MONITOR it specifies the host to monitor. Otherwise it is ignored.

    The format to receive a response from the MAD, through its standard output, is:

        OPERATION HID RESULT INFO

    Where:
    -OPERATION: Is the operation specified in the request that originated the response.
    -HID: It is the host identifier, as provided in the submission request.
    -RESULT: It is the result of the operation. Could be SUCCESS or FAILURE.
    -INFO: If RESULT is FAILURE, it contains the cause of failure. Otherwise, if OPERATION
        is   DISCOVER, it contains a list of discovered host, or if OPERATION is MONITOR,
        it contains a list of host attributes.
    """

    logger  = logging.getLogger(__name__)
    message = Send()
    lock = threading.Lock()

    def __init__(self):
        self._resources  = dict()
        self._config     = None
        self.pend_jobs_time = 0.0
        self.max_pend_jobs_time = 0.0
        self.max_pend_jobs_limit = 10
        self.schedule_interval = 5 #related with SCHEDULE_INTERVAL value in gwd.conf
        self.node_poll_time = self.schedule_interval*5
        self.idle_vms = dict()

    def do_INIT(self, args):
        """
        Initializes the MAD (i.e. INIT - - -)
        @param args : arguments of operation
        @type args : string
        """
        out = 'INIT - SUCCESS -'
        self.message.stdout(out)
        self.logger.debug(out)

    def _call_create_vms(self, resname, num_instances):
        self._config.resources[ resname ]['vm_instances'] += num_instances
        self.lock.acquire()
        try:
            conn = sqlite3.connect(resource_conf_db)
            with conn:
                cur = conn.cursor()
                cur.execute("SELECT count(*) FROM Resources WHERE name = '%s'" % resname)
                data=cur.fetchone()[0]
                if data==0:
                    cur.execute("INSERT INTO Resources (name, vms) VALUES ('%s', %d)" % (resname, num_instances))
                    self._config.resources[ resname ][ 'vm_instances' ] = num_instances
                else:
                    cur.execute("SELECT vms FROM Resources WHERE name='%s'" % (resname))
                    vms = cur.fetchone()[0]
                    vms += num_instances
                    cur.execute("UPDATE Resources SET vms = %d WHERE name = '%s'" % (vms, resname))
                    self._config.resources[ resname ][ 'vm_instances' ] = vms
        except Exception as err:
            self.logger.error( "Error updating SQLite database %s\n%s" % (resource_conf_db, str( err )) )
        finally:
            self.lock.release()
        background_thread = Thread(target=rocci.create_num_instances, args=(num_instances, resname, self._config.resources[resname]))
        background_thread.start()

    def _dynamic_vm_creation(self, resname):
        if self._config.resources[resname]['vm_instances'] < int(self._config.resources[resname]['min_nodes']):
            num_instances = int(self._config.resources[resname]['min_nodes']) - self._config.resources[resname]['vm_instances']
            self._call_create_vms(resname, num_instances)
            
        log3.info("\nTotal VMs creadas para %s: %s\n" % (resname, self._config.resources[resname]['vm_instances']))
        
        #get the number of pending jobs
        command1 = "gwps -n -s i"
        command2 = "wc -l"
        pipe = subprocess.Popen(command1.split(), stdout=subprocess.PIPE)
        pending_jobs = subprocess.check_output(command2.split(), stdin=pipe.stdout)
        _, _ = pipe.communicate() #just to ensure that the process is closed
        pending_jobs = int(pending_jobs.strip())
        
        if pending_jobs:
            if not self.pend_jobs_time:
                self.pend_jobs_time = time.time()
            #create VM if min_nodes == 0 and pending_jobs
            if int(self._config.resources[resname]['min_nodes']) == 0 and self._config.resources[resname]['vm_instances'] == 0:
                self._call_create_vms(resname, 1)

            #create VM if pending jobs is low but it's taking too long
            if pending_jobs < self.max_pend_jobs_limit and (time.time() - self.pend_jobs_time) >= self.node_poll_time * 3:
                if self._config.resources[resname]['vm_instances'] < int(self._config.resources[resname]['max_nodes']):
                    self._call_create_vms(resname, 1)
        else:
            self.pend_jobs_time = 0.0

        #create VM if pending jobs is too high
        if pending_jobs >= self.max_pend_jobs_limit:
            if self.max_pend_jobs_time == 0.0:
                self.max_pend_jobs_time = time.time()
            elif (time.time() - self.max_pend_jobs_time) >= self.node_poll_time:
                if self._config.resources[resname]['vm_instances'] < int(self._config.resources[resname]['max_nodes']):
                    self._call_create_vms(resname, 1)
        else:
            self.max_pend_jobs_time = 0.0
            
    def running_time(self, start_time):
        if not start_time:
            return 0
        else:
            return (time.time() - start_time)/3600
        
    def current_balance(self, pricing, start_time):
        running_hours = ceil(self.running_time(start_time))
        return running_hours * pricing
    
    def _call_destroy_vms(self, resname, num_instances):
        self._config.resources[ resname ]['vm_instances'] += num_instances
        self.lock.acquire()
        try:
            conn = sqlite3.connect(resource_conf_db)
            with conn:
                cur = conn.cursor()
                cur.execute("SELECT count(*) FROM Resources WHERE name = '%s'" % resname)
                data=cur.fetchone()[0]
                if data==0:
                    cur.execute("INSERT INTO Resources (name, vms) VALUES ('%s', %d)" % (resname, num_instances))
                    self._config.resources[ resname ][ 'vm_instances' ] = num_instances
                else:
                    cur.execute("SELECT vms FROM Resources WHERE name='%s'" % (resname))
                    vms = cur.fetchone()[0]
                    vms += num_instances
                    cur.execute("UPDATE Resources SET vms = %d WHERE name = '%s'" % (vms, resname))
                    self._config.resources[ resname ][ 'vm_instances' ] = vms
        except Exception as err:
            self.logger.error( "Error updating the SQLite database %s\n%s" % (resource_conf_db, str( err )) )
        finally:
            self.lock.release()
        background_thread = Thread(target=rocci.destroy_num_instances, args=(num_instances, resname, self._config.resources[resname]))
        background_thread.start()

    def vm_is_idle(self, vm_name):
        vm_is_idle = True
        for job_state in ['i', 'p', 'w', 'e']:
            command1 = "gwps -n -r %s -s %s" % (vm_name, job_state)
            command2 = "wc -l"
            pipe = subprocess.Popen(command1.split(), stdout=subprocess.PIPE)
            running_jobs = subprocess.check_output(command2.split(), stdin=pipe.stdout)
            _, _ = pipe.communicate() #just to ensure that the process is closed
            running_jobs = int(running_jobs.strip())
            #total_running_jobs += running_jobs
            if running_jobs:
                vm_is_idle = False
                break        
        return vm_is_idle

    def _dynamic_vm_deletion(self, resname):
        #if self._config.resources[ resname ][ 'vm_instances' ] > self._config.resources[ resname ][ 'max_nodes' ]:
        #    num_instances = self._config.resources[resname]['vm_instances'] - int(self._config.resources[resname]['min_nodes'])
        #    self._call_destroy_vms(resname, num_instances)
        if os.path.exists( resource_conf_db ):
            with self.lock:
                total_spent = 0
                conn = sqlite3.connect(resource_conf_db)
                with conn:
                    cur = conn.cursor()
                    cur.execute("SELECT id FROM Resources WHERE name = '%s'" % resname )
                    resource_id = cur.fetchone()[0]
                    for row in cur.execute("SELECT name, state, pricing, start_time FROM VM_Pricing WHERE resource_id = %d" % resource_id ):
                        vm_name, state, pricing, start_time = row
                        running_hours = ceil(self.running_time(start_time))
                        current_balance = self.current_balance(pricing, start_time)
                        total_spent += current_balance
                        #if total expenditure is over the limit destroy all VMs
                        log3.info("Total gastado : %s" % total_spent)
                        log3.info("Maximo permitdo : %s\n" % self._config.resources[resname]['hard_billing'])
                        if total_spent >= float(self._config.resources[resname]['hard_billing']) :
                            log3.info("\nEliminando todas las VMs\n")
                            rocci.manage_instances('stop', resname, self._config.resources[resname])
                            break
                        
                        #get the number of running jobs in VM
                        #total_running_jobs = 0
                        if vm_name not in self.idle_vms.keys():
                            #set state to idle
                            if self.vm_is_idle(vm_name): #if total_running_jobs == 0:
                                #cur.execute("UPDATE VM_Pricing SET state = '%s' WHERE name = '%s'" % ('idle', vm_name))
                                self.idle_vms[vm_name] = {'state':'idle', 'idle_since':time.time()}
                        elif (time.time() - self.idle_vms[vm_name]['idle_since']) >= self.node_poll_time * 3:
                            if not self.vm_is_idle(vm_name):
                                del(self.idle_vms[vm_name])
                        
                        node_safe_time = (self._config.resources[ resname ]['node_safe_time'])/60.0 #turn it into hours
                        #if time left for another hour to be reached is smaller than node_safe_time but bigger than one minute 
                        if (1-(running_hours-int(running_hours))) < node_safe_time and (1-(running_hours-int(running_hours))) > 1/60.0:
                            background_thread = Thread(target=rocci.destroy_vm_by_name, args=(resname, vm_name))
                            background_thread.start()
        '''
        if os.path.exists( os.path.join( pickled_file+"_"+resname ) ):
            try:
                instances = []
                total_spent = 0
                with open( pickled_file+"_"+resname, "r" ) as pf :
                    while True :
                        try:
                            instances.append( pickle.load( pf ) )
                        except EOFError :
                            break
                if instances:
                    for instance in instances :
                        total_spent += instance.current_balance()
                
                #if total expenditure is over the limit destroy all VMs
                if total_spent >= instance.hard_billing :
                    rocci.manage_instances('stop', resname, self._config[resname])
            except Exception as err:
                raise Exception( "An error occurred while trying to automatically delete a VMs from the resource %s:\n%s" % (resname, str(err)) )
        '''
        
    def do_DISCOVER(self, args, output=True):
        """
        Discovers hosts (i.e. DISCOVER - - -)
        @param args : arguments of operation
        @type args : string
        """
        OPERATION, HID, HOST, ARGS = args.split()
        try:
            self._config  = Configuration()
            self._config.load()
            errors        = self._config.check()
            assert not errors, ' '.join( errors )
            
            self._resources  = self._config.make_resources()
            communicators    = self._config.make_communicators()
            hosts = ""
            for resname in sorted( self._resources.keys() ) :
                if self._config.resources[ resname ][ 'enable' ].lower()  == 'false' :
                    continue
                if 'cloud_provider' in self._config.resources[ resname ].keys(): 
                    self._dynamic_vm_creation(resname)
                    self._dynamic_vm_deletion(resname)
                    continue
                try :
                    self._resources[ resname ][ 'Resource' ].Communicator = communicators[ resname ]
                    if self._config.resources[ resname ][ 'communicator' ] == 'op_ssh' :
                        self._resources[ resname ][ 'Resource' ].Communicator.configfile=os.path.join(DRM4G_DIR, 'etc', 'openssh_im.conf')
                        self._resources[ resname ][ 'Resource' ].Communicator.parent_module='im'
                    self._resources[ resname ][ 'Resource' ].Communicator.connect()
                    hosts = hosts + " " + self._resources[ resname ] [ 'Resource' ].hosts()
                    self._resources[ resname ][ 'Resource' ].Communicator.close()
                except Exception as err :
                    self.logger.error( err , exc_info=1 )
            out = 'DISCOVER %s SUCCESS %s' % ( HID , hosts  )
        except Exception as err :
            out = 'DISCOVER - FAILURE %s' % str( err )
        if output:
            self.message.stdout( out )
        self.logger.debug( out , exc_info=1 )

    def do_MONITOR(self, args, output=True):
        """
        Monitors a host (i.e. MONITOR HID HOST -)
        @param args : arguments of operation
        @type args : string
        """
        OPERATION, HID, HOST, ARGS = args.split()
        try:
            info = ""
            for resname, resdict in list(self._resources.items()) :
                if self._config.resources[ resname ][ 'enable' ].lower() == 'false':
                    raise Exception( "Resource '%s' is not enable" % resname )
                if HOST in resdict['Resource'].host_list :
                    info = resdict['Resource'].host_properties( HOST )
                    resdict['Resource'].Communicator.close()
                    break
            assert info, "Host '%s' is not available" % HOST
            out = 'MONITOR %s SUCCESS %s' % (HID , info )
        except Exception as err :
            out = 'MONITOR %s FAILURE %s' % (HID , str(err) )
        if output:
            self.message.stdout(out)
        self.logger.debug( out , exc_info=1 )

    def do_FINALIZE(self, args):
        """
        Finalizes the MAD (i.e. FINALIZE - - -)
        @param args : arguments of operation
        @type args : string
        """
        out = 'FINALIZE - SUCCESS -'
        self.message.stdout(out)
        self.logger.debug(out)
        sys.exit(0)

    methods = { 'INIT'    : do_INIT,
                'DISCOVER': do_DISCOVER,
                'MONITOR' : do_MONITOR,
                'FINALIZE': do_FINALIZE,
                }

    def processLine(self):
        """
        Choose the OPERATION through the command line
        """
        try:
            while True:
                input = sys.stdin.readline().split()
                self.logger.debug(' '.join(input))
                OPERATION = input[0].upper()
                if len(input) == 4 and OPERATION in self.methods:
                    self.methods[OPERATION](self, ' '.join(input))
                else:
                    out = 'WRONG COMMAND'
                    self.message.stdout(out)
                    self.logger.debug(out)
        except Exception as e:
            self.logger.warning(str(e))

