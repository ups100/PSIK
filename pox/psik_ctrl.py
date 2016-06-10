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
from pox.lib.packet.dns import dns
from pox.lib.packet.arp import arp
from pox.lib.packet.udp import udp
from pox.lib.packet.ipv4 import ipv4
from pox.lib.packet.ethernet import ethernet, ETHER_BROADCAST
from pox.lib.addresses import EthAddr, IPAddr # Address types
import pox.lib.util as poxutil                # Various util functions
import pox.lib.revent as revent               # Event library
import pox.lib.recoco as recoco               # Multitasking library
import random

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

class PSIKMainServerSwitch(PSIKSwitch):
    def __init__(self, sid, dpid, ip, dcs_load, connection = None):
        super(PSIKMainServerSwitch, self).__init__(sid, dpid, connection)
        self.macToPort = {}
        mac_raw = 0x0000FFFFFFFFFFFF & dpid
        self.my_mac = EthAddr(hex(mac_raw)[2:].zfill(12))
        self.my_ip = ip
        self.service_name = "service.psik.com"
        self.dcs_load = dcs_load

    def set_connection(self, connection):
        msg = of.ofp_flow_mod()
        msg.match = of.ofp_match()
        msg.match.dl_type = pkt.ethernet.IP_TYPE
        msg.match.nw_proto = pkt.ipv4.UDP_PROTOCOL
        msg.match.tp_src = 53
        msg.actions.append(of.ofp_action_output(port = of.OFPP_CONTROLLER))
        connection.send(msg)
        super(PSIKMainServerSwitch, self).set_connection(connection)

    def choose_data_center(self):
        def weighted_host_choice():
            total = sum(load for load in self.dcs_load)
            r = random.uniform(0, total)

            upto = 0
            i = 0
            for load in self.dcs_load:
                if upto + load >= r:
                    return i;
                upto += load
                i += 1
            assert False, "Shouldn't get here"

        dc = weighted_host_choice() + 1
        ip_str = "10.0." + str(dc) + ".1"
                
        return IPAddr(ip_str)

    def _handle_PacketIn(self, event):
        packet = event.parsed

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

        def handle_dns(packet):
            dnsp = packet.find('dns')

            if len(dnsp.questions) > 1:
                drop()
                return

            question = dnsp.questions[0]
            response = None
            log.debug("Question: %s" % (question.name))
            if question.qtype == dns.rr.A_TYPE and question.name == self.service_name:
                # Some one is asking about our service so let's
                # choose one of data centers and answer him
                dc_ip = self.choose_data_center()
                response = dns.rr(question.name, question.qtype, question.qclass,
                                  0, 4, dc_ip)
            elif question.qtype == dns.rr.PTR_TYPE:
                # for now we assume that only our dns is resolvable
                response = dns.rr(question.name, question.qtype, question.qclass,
                                  0, len(self.service_name), self.service_name)
            else:
                drop()
                return

            r = dns()
            r.id = dnsp.id
            r.rd = dnsp.rd
            r.ra = True
            r.aa = 1
            r.questions.append(question)
            r.answers.append(response)
            u = udp()
            u.srcport = 53
            u.dstport = packet.find('udp').srcport
            u.set_payload(r)

            ipp = ipv4()
            ipp.protocol = ipv4.UDP_PROTOCOL
            ipp.srcip = self.my_ip
            ipp.dstip = packet.find('ipv4').srcip
            ipp.set_payload(u)

            e = ethernet(type=ethernet.IP_TYPE, src=self.my_mac,
                         dst=packet.src)
            e.set_payload(ipp)
            msg = of.ofp_packet_out()
            msg.data = e.pack()
            msg.actions.append(of.ofp_action_output(port = event.port))
            msg.in_port = of.OFPP_NONE
            self.connection.send(msg)
            
        def send_arp_resonse(arpp):
            r = arp()
            r.hwtype = r.HW_TYPE_ETHERNET
            r.prototype = r.PROTO_TYPE_IP
            r.opcode = r.REPLY
            r.hwdst = arpp.hwsrc
            r.protodst = arpp.protosrc
            r.hwsrc = self.my_mac
            r.protosrc = self.my_ip
            e = ethernet(type=ethernet.ARP_TYPE, src=self.my_mac,
                         dst=arpp.hwsrc)
            e.set_payload(r)
            msg = of.ofp_packet_out()
            msg.data = e.pack()
            msg.actions.append(of.ofp_action_output(port = event.port))
            msg.in_port = of.OFPP_NONE
            self.connection.send(msg)
            
        def handle_normal(packet):
            def flood():
                msg = of.ofp_packet_out()
                msg.actions.append(of.ofp_action_output(port = of.OFPP_FLOOD))
                msg.data = event.ofp
                msg.in_port = event.port
                self.connection.send(msg)


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

        #is this to us?
        if packet.dst == self.my_mac:
            log.debug("Jest do nas")
            if packet.find('dns') is not None:
                handle_dns(packet)
        elif packet.find('arp') is not None:
            arpp = packet.find('arp')
            # someone is looking for us
            if arpp.opcode == arpp.REQUEST and arpp.protodst == self.my_ip:
                log.info("Host %s is looking for us" % (arpp.protosrc,))
                send_arp_resonse(arpp)
            else:
                handle_normal(packet)
        else:
            handle_normal(packet)
         
class PSIKComponent (object):
    def __init__(self, mss_dpid, mss_ip, mcs_dpid, dcs_dpids, decision_type, dcs_load):
        self.switches = set()
        self.mcs = PSIKLearningSwitch("mcs", mcs_dpid)
        self.mss = PSIKMainServerSwitch("mss", mss_dpid, mss_ip, dcs_load)
        self.dcs_load = dcs_load
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

def launch (mss_dpid = "00-00-00-01-00-00|1", mss_ip = None,
            mcs_dpid = "00-00-00-02-00-00|2",
            dcs_dpids = ["00-00-00-01-01-00|101", "00-00-00-01-02-00|102",
                       "00-00-00-01-03-00|103"],
            dcs_load=[1.0/3, 1.0/3, 1.0/3]):

    if mss_ip is None:
        mss_ip = IPAddr("10.254.254.254")

    mss_dpid = poxutil.str_to_dpid(mss_dpid)
    mcs_dpid = poxutil.str_to_dpid(mcs_dpid)
    for i in range(len(dcs_dpids)):
        dcs_dpids[i] = poxutil.str_to_dpid(dcs_dpids[i])

    core.registerNew(PSIKComponent, mss_dpid, mss_ip, mcs_dpid, dcs_dpids, DecisionType.DEC_STATIC, dcs_load)

