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

class PSIKSwitch(object):
    def __init__(self, sid, dpid, connection = None):
        self.name = sid
        self.dpid = dpid
        self.connection = connection

    def set_connection(self, connection):
        self.connection = connection
        connection.addListeners(self)

class PSIKLearningSwitch(PSIKSwitch):
    def __init__(self, sid, dpid, connection = None):
        super(PSIKLearningSwitch, self).__init__(sid, dpid, connection)
        self.macToPort = {}

    def _handle_PacketIn(self, event):
        packet = event.parsed

        def flood():
            msg = of.ofp_packet_out()
            msg.actions.append(of.ofp_action_output(port = of.OFPP_FLOOD))
            msg.data = event.ofp
            msg.in_port = event.port
            self.connection.send(msg)

        def drop(duration = None):
            msg = None
            if duration is not None:
                if not isinstance(duration, tuple):
                    duration = (duration,duration)
                msg = of.ofp_flow_mod()
                msg.match = of.ofp_match.from_packet(packet)
                msg.idle_timeout = duration[0]
                msg.hard_timeout = duration[1]
                msg.buffer_id = event.ofp.buffer_id
            elif event.ofp.buffer_id is not None:
                msg = of.ofp_packet_out()
                msg.buffer_id = event.ofp.buffer_id
                msg.in_port = event.port

            self.connection.send(msg)

        #here function really starts
        self.macToPort[packet.src] = event.port

        if packet.dst.is_multicast:
            flood()
        elif packet.dst not in self.macToPort:
            log.debug("Route to %s not found flooding" % (packet.dst,))
            flood()
        else:
            port = self.macToPort[packet.dst]
            if port == event.port:
                log.warning("Same port for packet from %s -> %s on %s.%s.  Drop."
                            % (packet.src, packet.dst, dpid_to_str(event.dpid), port))
                drop(10)
                return

        log.debug("installing flow for %s.%i -> %s.%i" %
                  (packet.src, event.port, packet.dst, port))
        msg = of.ofp_flow_mod()
        msg.match = of.ofp_match.from_packet(packet, event.port)
        msg.idle_timeout = 10
        msg.hard_timeout = 30
        msg.actions.append(of.ofp_action_output(port = port))
        msg.data = event.ofp
        self.connection.send(msg)

            


class PSIKComponent (object):
    def __init__(self, mss_dpid, mcs_dpid, dcs_dpids, decision_type, dcs_load):
        self.switches = set()
        self.mcs = PSIKLearningSwitch("mcs", mcs_dpid)
        self.mss = PSIKLearningSwitch("mss", mss_dpid)
        self.dcs = list()
        i = 1
        for dpid in dcs_dpids:
            self.dcs.append(PSIKLearningSwitch("dc" + str(i), dpid))
            i += 1

        core.openflow.addListeners(self)

    def _handle_ConnectionUp(self, event):
        log.debug("Connection %s" % (event.connection,))

        dpid = event.connection.dpid
        if dpid == self.mss.dpid:
            log.debug("Main server switch found: %s" % (event.connection,))
            self.mss.set_connection(event.connection)
        elif dpid == self.mcs.dpid:
            log.debug("Main client switch found: %s" % (event.connection,))
            self.mcs.set_connection(event.connection)
        else:
            found = False
            for switch in self.dcs:
                if switch.dpid == dpid:
                    found = True
                    log.debug("%s switch found: %s" % (switch.name, event.connection,))
                    switch.set_connection(event.connection)
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

