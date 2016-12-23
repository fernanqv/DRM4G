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

import logging



DEBUG = logging.DEBUG
INFO = logging.INFO
WARNING = logging.WARNING
ERROR = logging.ERROR
CRITICAL = logging.CRITICAL

def log_to_file(filename, level=DEBUG):
    l = logging.getLogger("drm4g")
    if len(l.handlers) > 0:
        return
    l.setLevel(level)
    f = open(filename, 'w')
    lh = logging.StreamHandler(f)
    lh.setFormatter(logging.Formatter('%(levelname)-.3s [%(asctime)s.%(msecs)03d] %(name)s: %(message)s',
                                      '%Y%m%d-%H:%M:%S'))
    l.addHandler(lh)

def get_logger(name):
    return logging.getLogger(name)
