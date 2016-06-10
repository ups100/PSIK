# Copyright 2016 Krzysztof Opasiak
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at:
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

from pox.core import core                     # Main POX object
import pox.openflow.libopenflow_01 as of      # OpenFlow 1.0 library
import pox.lib.packet as pkt                  # Packet parsing/construction
from pox.lib.addresses import EthAddr, IPAddr # Address types
import pox.lib.util as poxutil                # Various util functions
import pox.lib.revent as revent               # Event library
import pox.lib.recoco as recoco               # Multitasking library

class DecisionType:
    DEC_STATIC = 1
    DEC_DYNAMIC = 2

log = core.getLogger()

class PSIKComponent (object):
    class PSIKSwitch:
        def __init__(self, sid, dpid, connection = None):
            self.name = sid
            self.dpid = dpid
            self.connection = connection

    def __init__(self, mss_dpid, mcs_dpid, dcs_dpids, decision_type, dcs_load):
        self.switches = set()
        self.mcs = self.PSIKSwitch("mcs", mcs_dpid)
        self.mss = self.PSIKSwitch("mss", mss_dpid)
        self.dcs = list()
        i = 1
        for dpid in dcs_dpids:
            self.dcs.append(self.PSIKSwitch("dc" + str(i), dpid))
            i += 1

        core.openflow.addListeners(self)

    def _handle_ConnectionUp(self, event):
        log.debug("Connection %s" % (event.connection,))

        dpid = event.connection.dpid
        if dpid == self.mss.dpid:
            log.debug("Main server switch found: %s" % (event.connection,))
            self.mss.connection = event.connection
        elif dpid == self.mcs.dpid:
            log.debug("Main client switch found: %s" % (event.connection,))
            self.mcs.connection = event.connection
        else:
            found = False
            for switch in self.dcs:
                if switch.dpid == dpid:
                    found = True
                    log.debug("%s switch found: %s" % (switch.name, event.connection,))
                    switch.connection = event.connection
            if not found:
                log.error("Unable to identify switch: %s" % (event.connection,))

def launch (mss_dpid = "00-00-00-01-00-00|1", mcs_dpid = "00-00-00-02-00-00|2",
            dcs_dpids=["00-00-00-01-01-00|101", "00-00-00-01-02-00|102",
                       "00-00-00-01-03-00|103"],
            dcs_load=[1.0/3, 1.0/3, 1.0/3]):

    mss_dpid = poxutil.str_to_dpid(mss_dpid)
    mcs_dpid = poxutil.str_to_dpid(mcs_dpid)
    for i in range(len(dcs_dpids)):
        dcs_dpids[i] = poxutil.str_to_dpid(dcs_dpids[i])

    core.registerNew(PSIKComponent, mss_dpid, mcs_dpid, dcs_dpids, DecisionType.DEC_STATIC, dcs_load)

