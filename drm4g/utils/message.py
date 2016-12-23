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

import threading
import sys


class Send (object):
    """
    This class provides basic messaging
    """
    def __init__(self):
        self._lock = threading.Lock()

    def stdout(self, message):
        self._lock.acquire()
        try:
            sys.stdout.write(message + '\n')
            sys.stdout.flush()
        finally:
            self._lock.release()

    def stderr(self, message):
        self._lock.acquire()
        try:
            sys.stderr.write(message + '\n')
            sys.stderr.flush()
        finally:
            self._lock.release()


