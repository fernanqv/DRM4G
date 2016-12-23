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


class List (object):
    """
    Dictionary self-protected.
    """
    def __init__(self):
        self._map = { }
        self._lock = threading.Lock()

    def put(self, key, value):
        self._lock.acquire()
        try:
            self._map[key] = value
        finally:
            self._lock.release()

    def get(self, key):
        self._lock.acquire()
        try:
            return self._map.get(key, None)
        finally:
            self._lock.release()

    def delete(self, key):
        self._lock.acquire()
        try:
            try:
                del self._map[key]
            except KeyError:
                pass
        finally:
            self._lock.release()

    def has_key(self, key):
        self._lock.acquire()
        try:
            return key in self._map
        finally:
            self._lock.release()

    def values(self):
        self._lock.acquire()
        try:
            return list(self._map.values())
        finally:
            self._lock.release()

    def items(self):
        self._lock.acquire()
        try:
            return list(self._map.items())
        finally:
            self._lock.release()




