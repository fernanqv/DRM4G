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

try :
    import Queue as queue
except :
    import queue
import sys
import traceback
from threading        import Thread
from threading        import Lock



"""
Use example
from dynamic import ThreadPool
pool = ThreadPool(2, 10)
pool.add_task(func, arg)
"""

threads_active = 0
threads_min    = 1
timeout        = 1
lock           = Lock()

class Worker(Thread):
    """
    Thread executing tasks from a given tasks queue
    """
    def __init__(self, tasks):
        Thread.__init__(self)
        self.tasks = tasks
        self.setDaemon(True)
        self.start()

    def run(self):
        while True:
            try:
                try:
                    func, args, kargs = self.tasks.get( timeout  )
                except queue.Empty:
                    with lock :
                        global threads_active, threads_min
                        if threads_active > threads_min:
                            threads_active -= 1
                            break
                        else :
                            continue
                except Exception:
                    sys.exit(-1)
                else:
                    try:
                        func(*args, **kargs)
                    except :
                        traceback.print_exc(file=sys.stdout)
            except Exception:
                pass

class ThreadPool:
    """
    Pool of threads consuming tasks from a queue
    """
    def __init__(self, num_threads_min, num_threads_max):
        self.threads_max = num_threads_max
        self.tasks = queue.Queue()
        with lock :
            global threads_active, threads_min
            threads_active =  num_threads_min
            threads_min    = num_threads_min
        for _ in range(num_threads_min):
            Worker(self.tasks)

    def add_task(self, func, *args, **kargs):
        """
        Add a task to the queue
        """
        with lock :
            global threads_active
            try:
                if threads_active < self.threads_max:
                    Worker(self.tasks)
                    threads_active += 1
                self.tasks.put((func, args, kargs))
            except :
                traceback.print_exc(file=sys.stdout)

