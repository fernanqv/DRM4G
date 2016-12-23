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

import xml.dom.minidom


class Rsl2Parser(object):

  """
   Parser of RSL2 files. It is used like this:

           rsl    = RSL2Parser(filename)
           stdout = rsl.parse()
  """
  elems_to_parse = ['executable','stdout','stderr','directory','count','jobType',
                      'queue','maxTime','maxWallTime','maxCpuTime','minMemory','maxMemory','ppn', 'nodes']

  def __init__(self, filename):
    """
    @param filename : file name or file
    @type filename : string
    """
    self._values = { }
    self._str = xml.dom.minidom.parse(filename)

  def parseValue(self, name):
    """
    Gets data from <job><name></name></job> and adds it to self._values
    @param name : name of value
    @type name : string
    """
    val = self._str.getElementsByTagName(name)
    if val:
        self._values[name] = val[0].firstChild.data

  def parseEnvironment(self):
    """
    Gets all the data enclosed in <job><environment>...</environment></job> and adds
    it to self._values
    """
    environments = self._str.getElementsByTagName('environment')
    if environments:
        self._values['environment'] = dict((elem.getElementsByTagName('name')[0].firstChild.data,
                                       elem.getElementsByTagName('value')[0].firstChild.data) \
                                       for elem in environments)

  def parser(self):
    """
    Does the parsing. It is necessary to call this method after creating the object
    @return  : dictionary with RSL values
    """
    [self.parseValue(name) for name in self.elems_to_parse]
    self.parseEnvironment()
    return self._values

