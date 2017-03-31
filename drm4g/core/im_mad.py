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
import sys
import logging
import time
#import pickle
import sqlite3
import subprocess
import drm4g.managers.cloud_providers   as     cloud_conn
from drm4g                              import DRM4G_DIR, CLOUD_CONNECTORS
from threading                          import Thread, Lock
from drm4g.core.configure               import Configuration
from drm4g.managers.cloud_providers     import logger as log3
from drm4g.utils.message                import Send
from drm4g.utils.importlib              import import_module
#from drm4g.managers.cloud_providers     import rocci
from math                               import ceil

pickled_file = os.path.join(DRM4G_DIR, "var", "%s_pickled")
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
    lock = Lock()

    def __init__(self):
        self._resources  = dict()
        self._config     = None
        self.pend_jobs_time = 0.0
        self.max_pend_jobs_time = 0.0
        self.max_pend_jobs_limit = 10
        self.schedule_interval = 5 #related with SCHEDULE_INTERVAL value in gwd.conf
        self.node_poll_time = self.schedule_interval*6
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
        try:
            conn = sqlite3.connect(resource_conf_db)
            with self.lock:
                with conn:
                    cur = conn.cursor()
                    cur.execute("SELECT vms FROM Resources WHERE name='%s'" % (resname))
                    vms = cur.fetchone()[0]
                    vms += num_instances
                    cur.execute("UPDATE Resources SET vms = %d WHERE name = '%s'" % (vms, resname))
                    self._config.resources[ resname ][ 'vm_instances' ] = vms
        except Exception as err:
            self.logger.error( "Error updating SQLite database %s: %s" % (resource_conf_db, str( err )) )

        cloud_conn.create_num_instances(num_instances, resname, self._config.resources[resname])
        
    def _dynamic_vm_creation(self, resname):
        total_spent = 0
        if os.path.exists( resource_conf_db ):
            with self.lock:
                conn = sqlite3.connect(resource_conf_db)
                with conn:
                    cur = conn.cursor()
                    #cur.execute("SELECT id FROM Resources WHERE name = '%s'" % resname )
                    #resource_id = cur.fetchone()[0]
                    cur.execute("SELECT id, past_expenditure FROM Resources WHERE name = '%s'" % resname )
                    resource_id, total_spent = cur.fetchone()
                    for row in cur.execute("SELECT pricing, start_time FROM VM_Pricing WHERE resource_id = %d" % resource_id ):
                        pricing, start_time = row
                        running_hours = self.running_time(start_time)
                        total_spent += self.instance_expenditure(pricing, running_hours)

        total_spent += float(self._config.resources[resname]['pricing'])
        if total_spent == 0 or total_spent < float(self._config.resources[resname]['hard_billing']) :
            if self._config.resources[resname]['vm_instances'] < int(self._config.resources[resname]['node_min_pool_size']) :
                num_instances = int(self._config.resources[resname]['node_min_pool_size']) - self._config.resources[resname]['vm_instances']
                if float(self._config.resources[resname]['pricing']) * num_instances < float(self._config.resources[resname]['hard_billing']) or float(self._config.resources[resname]['pricing']) == 0 :
                    log3.info("Creating %s VMs for the resource %s" % (num_instances, resname))
                    cloud_conn.create_num_instances(num_instances, resname, self._config.resources[resname])
                    self._config.resources[ resname ][ 'vm_instances' ] += num_instances
            
            #get the number of pending jobs
            command = "gwps -n -s i"
            pipe = subprocess.Popen(command.split(), stdout=subprocess.PIPE)
            out, err = pipe.communicate()
            if err:
                raise Exception ("Couldn't get the number of pending jobs")
            output_list = out.strip().split('\n')
            pending_jobs = len(output_list)
            if output_list.count(''):
                pending_jobs -= 1
            
            if pending_jobs:
                if not self.pend_jobs_time:
                    self.pend_jobs_time = time.time()
                #create VM if node_min_pool_size == 0 and pending_jobs
                if int(self._config.resources[resname]['node_min_pool_size']) == 0 and self._config.resources[resname]['vm_instances'] == 0:
                    cloud_conn.create_num_instances(1, resname, self._config.resources[resname])
                    self._config.resources[ resname ][ 'vm_instances' ] += 1
    
                #create VM if pending jobs is low but it's taking too long
                if pending_jobs < self.max_pend_jobs_limit and (time.time() - self.pend_jobs_time) >= self.node_poll_time * 3:
                    if self._config.resources[resname]['vm_instances'] < int(self._config.resources[resname]['node_max_pool_size']):
                        cloud_conn.create_num_instances(1, resname, self._config.resources[resname])
                        self._config.resources[ resname ][ 'vm_instances' ] += 1
            else:
                self.pend_jobs_time = 0.0
    
            #create VM if pending jobs is too high
            if pending_jobs >= self.max_pend_jobs_limit:
                if self.max_pend_jobs_time == 0.0:
                    self.max_pend_jobs_time = time.time()
                elif (time.time() - self.max_pend_jobs_time) >= self.node_poll_time:
                    if self._config.resources[resname]['vm_instances'] < int(self._config.resources[resname]['node_max_pool_size']):
                        cloud_conn.create_num_instances(1, resname, self._config.resources[resname])
                        self._config.resources[ resname ][ 'vm_instances' ] += 1
            else:
                self.max_pend_jobs_time = 0.0
            
    def running_time(self, start_time):
        '''
        Given start time in seconds since epoch, returns running time in hours   
        '''
        if not start_time:
            return 0
        else:
            #return (time.time() - start_time)/3600.0 #
            return (time.time() - start_time)/360.0 #for every 6 min it will mark as if an hour had passed  

    '''  
    def current_balance(self, pricing, start_time):
        running_hours = ceil(self.running_time(start_time))
        return running_hours * pricing
    '''

    def instance_expenditure(self, pricing, running_hours):
        running_hours = ceil(running_hours)
        return running_hours * pricing

    def vm_is_idle(self, vm_name):
        vm_is_idle = True
        for job_state in ['i', 'p', 'w', 'e']:
            command = "gwps -n -r %s -s %s" % (vm_name, job_state)
            pipe = subprocess.Popen(command.split(), stdout=subprocess.PIPE)
            out, err = pipe.communicate()
            if err:
                raise Exception ("Couldn't get the number of pending jobs")
            output_list = out.strip().split('\n')
            running_jobs = len(output_list)
            if output_list.count(''):
                running_jobs -= 1
            if running_jobs:
                vm_is_idle = False
                return vm_is_idle       
        return vm_is_idle
    
    def _dynamic_vm_deletion(self, resname):
        """
        For a specified resource, it will check if any of its VMs should be deleted 
        @param resname : name of the resource
        @type resname : string
        """
        if os.path.exists( resource_conf_db ):
            with self.lock:
                conn = sqlite3.connect(resource_conf_db)
                with conn:
                    cur = conn.cursor()
                    #cur.execute("SELECT id FROM Resources WHERE name = '%s'" % resname )
                    #resource_id = cur.fetchone()[0]
                    cur.execute("SELECT id, past_expenditure FROM Resources WHERE name = '%s'" % resname )
                    resource_id, total_spent = cur.fetchone()
                    #total_spent = 0
                    rows = cur.execute("SELECT name, state, pricing, start_time FROM VM_Pricing WHERE resource_id = %d" % resource_id )
            for row in rows:
                vm_name, state, pricing, start_time = row
                
                #running_hours = self.running_time(start_time)
                running_hours = self.running_time(start_time + (int(self._config.resources[ resname ]['node_safe_time'])*60.0))
                total_spent += self.instance_expenditure(pricing, running_hours)
                if total_spent != 0 and total_spent >= float(self._config.resources[resname]['hard_billing']) :
                    log3.info("_dynamic_vm_deletion - deleting all vms because expenditure is over the limit")
                    log3.info("Total spent : %s" % total_spent)
                    log3.info("Maximum limit : %s" % self._config.resources[resname]['hard_billing'])
                    self._config.resources[ resname ]['vm_instances'] = 0
                    cloud_conn.manage_instances('stop', resname, self._config.resources[resname])
                    break
                if state == 'active':
                    log3.info("%s _dynamic_vm_deletion - idle_vms before = %s" % (resname, self.idle_vms.items()))
                    if vm_name not in self.idle_vms.keys():
                        #set state to idle
                        if self.vm_is_idle(vm_name):
                            self.idle_vms[vm_name] = {'state':'idle', 'idle_since':time.time()}
                    elif (time.time() - self.idle_vms[vm_name]['idle_since']) >= self.node_poll_time * 3:
                        #after waiting for 3 times the node pole time (1'30"), it will then check each time if it's still idle, until it's not or it's deleted
                        if not self.vm_is_idle(vm_name):
                            del(self.idle_vms[vm_name])
                        else:
                            #node_safe_time = int(self._config.resources[ resname ]['node_safe_time'])/60.0 #turns node_safe_time into hours
                            node_safe_time = int(self._config.resources[ resname ]['node_safe_time'])/6.0
                            #one_min = 1/60.0
                            one_min = 1/6.0
                            one_hour = 1
                            if self._config.resources[ resname ]['vm_instances'] > int(self._config.resources[ resname ]['node_min_pool_size']):
                                running_hours = self.running_time(start_time)
                                #if time left for another hour to be reached is smaller than node_safe_time but bigger than one minute 
                                if (one_hour-(running_hours-int(running_hours))) < node_safe_time and (one_hour-(running_hours-int(running_hours))) > one_min:
                                    #this verifies that the VM "vm_name" still exists, since it could have been destroyed with the command "drm4g resource destroy"
                                    cur.execute("SELECT count(*) FROM VM_Pricing WHERE name = '%s'" % vm_name)
                                    data = cur.fetchone()[0]
                                    if data:
                                        log3.info("Deleting for been idle for too long - number of hits in VM_Pricing = %s for %s VM" % (data, vm_name))
                                        log3.info("self._config.resources[ resname ]['vm_instances'] before = %s" % self._config.resources[ resname ]['vm_instances'])
                                        background_thread = Thread(target=cloud_conn.destroy_vm_by_name, args=(resname, vm_name, self._config.resources[resname]['cloud_connector']))
                                        background_thread.start()
                                        self._config.resources[ resname ]['vm_instances'] -= 1
                                        log3.info("self._config.resources[ resname ]['vm_instances'] after = %s" % self._config.resources[ resname ]['vm_instances'])
                                    del(self.idle_vms[vm_name])
                    log3.info("%s _dynamic_vm_deletion - idle_vms after = %s" % (resname, self.idle_vms.items()))

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
            
            checked_for_non_active_vms = False
            for resname in sorted( self._resources.keys() ) :
                if self._config.resources[ resname ][ 'enable' ].lower()  == 'false' :
                    self.logger.debug( "The resource %s is not enabled" % resname )
                    continue
                if 'cloud_connector' in self._config.resources[ resname ].keys():
                    #this will only be checked once per IM cycle
                    if not checked_for_non_active_vms:
                        if os.path.exists( resource_conf_db ):
                            data = []
                            with self.lock:
                                conn = sqlite3.connect(resource_conf_db)
                                with conn:
                                    cur = conn.cursor()
                                    cur.execute("SELECT resource_name, cloud_connector FROM Non_Active_VMs")
                                    data = cur.fetchall()
                            if data:
                                checked_for_non_active_vms = cloud_conn.check_if_vms_active(data)
                        #checked_for_non_active_vms = True
                    if self._config.resources[ resname ]['vm_instances'] <= int(self._config.resources[ resname ]['node_max_pool_size']):
                        log3.info("do_DISCOVER - %s's vm_instances before _dynamic_vm_creation = %s" % (resname, self._config.resources[ resname ]['vm_instances']))
                        self._dynamic_vm_creation(resname)
                        log3.info("do_DISCOVER - %s's vm_instances after _dynamic_vm_creation = %s" % (resname, self._config.resources[ resname ]['vm_instances']))
                    #if there are existing VMs for this resname
                    if os.path.exists(pickled_file % self._config.resources[ resname ]['cloud_connector'] + "_" + resname):
                        log3.info("do_DISCOVER - %s's vm_instances before _dynamic_vm_deletion = %s" % (resname, self._config.resources[ resname ]['vm_instances']))
                        self._dynamic_vm_deletion(resname)
                        log3.info("do_DISCOVER - %s's vm_instances after _dynamic_vm_deletion = %s" % (resname, self._config.resources[ resname ]['vm_instances']))
                    continue
                try :
                    self._resources[ resname ][ 'Resource' ].Communicator = communicators[ resname ]
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
                    self.logger.debug( "The resource '%s' is not enabled" % resname )
                    continue
                if 'cloud_connector' in self._config.resources[ resname ].keys():
                    continue
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
            
            

