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

    def _flood(self, event):
        msg = of.ofp_packet_out()
        msg.actions.append(of.ofp_action_output(port = of.OFPP_FLOOD))
        msg.data = event.ofp
        msg.in_port = event.port
        self.connection.send(msg)

    def _drop(self, packet, buffer_id, in_port, duration = None):
        msg = None
        if duration is not None:
            if not isinstance(duration, tuple):
                duration = (duration,duration)
            msg = of.ofp_flow_mod()
            msg.match = of.ofp_match.from_packet(packet)
            msg.idle_timeout = duration[0]
            msg.hard_timeout = duration[1]
            msg.buffer_id = buffer_id
        elif buffer_id is not None:
            msg = of.ofp_packet_out()
            msg.buffer_id = buffer_id
            msg.in_port = in_port

        self.connection.send(msg)

    def _do_normal_packet(self, packet, event):
        self.macToPort[packet.src] = event.port

        if packet.dst.is_multicast:
            self._flood(event)
        elif packet.dst not in self.macToPort:
            log.debug("Route to %s not found flooding" % (packet.dst,))
            self._flood(event)
        else:
            port = self.macToPort[packet.dst]
            if port == event.port:
                log.warning("Same port for packet from %s -> %s on %s.%s.  Drop."
                            % (packet.src, packet.dst, dpid_to_str(event.dpid), port))
                self._drop(packet, event.of.buffer_id, event.port, 10)
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

    def _handle_PacketIn(self, event):
        packet = event.parsed
        self._do_normal_packet(packet, event)

class PSIKARPVisibleSwitch(PSIKLearningSwitch):
    def __init__(self, sid, dpid, ip, connection = None):
        super(PSIKARPVisibleSwitch, self).__init__(sid, dpid, connection)
        mac_raw = 0x0000FFFFFFFFFFFF & dpid
        self.my_mac = EthAddr(hex(mac_raw)[2:].zfill(12))
        self.my_ip = ip

    def _send_ethernet_packet(self, packet_type, _src, _dst, payload, out_port):
        e = ethernet(type=packet_type, src=_src, dst=_dst)
        e.set_payload(payload)
        msg = of.ofp_packet_out()
        msg.data = e.pack()
        msg.actions.append(of.ofp_action_output(port = out_port))
        msg.in_port = of.OFPP_NONE
        self.connection.send(msg)

    def _send_arp_response_packet(self, arpp, out_port):
            r = arp()
            r.hwtype = r.HW_TYPE_ETHERNET
            r.prototype = r.PROTO_TYPE_IP
            r.opcode = r.REPLY
            r.hwdst = arpp.hwsrc
            r.protodst = arpp.protosrc
            r.hwsrc = self.my_mac
            r.protosrc = self.my_ip

            self._send_ethernet_packet(ethernet.ARP_TYPE,
                                       self.my_mac, arpp.hwsrc,
                                       r, out_port)

    def _do_arp_packet(self, packet, event):
        arpp = packet.find('arp')

        if not (arpp.opcode == arpp.REQUEST and arpp.protodst == self.my_ip):
            self._do_normal_packet(packet, event)
            return

        log.info("Host %s is looking for us" % (arpp.protosrc,))
        self._send_arp_response_packet(arpp, event.port)

    def _handle_PacketIn(self, event):
        packet = event.parsed

        if packet.find('arp') is not None:
            self._do_arp_packet(packet, event)
        else:
            super(PSIKARPVisibleSwitch, self)._handle_PacketIn(event)


class PSIKMainServerSwitch(PSIKARPVisibleSwitch):
    def __init__(self, sid, dpid, ip, dcs_load, connection = None):
        super(PSIKMainServerSwitch, self).__init__(sid, dpid, ip, connection)
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

    def _choose_data_center(self):
        def weighted_host_choice():
            print "Load " + str(self.dcs_load)
            total = sum(self.dcs_load)
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

    def _send_ip_packet(self, protocol, dstip, dsthw, payload, _out_port):
        ipp = ipv4()
        ipp.protocol = protocol
        ipp.srcip = self.my_ip
        ipp.dstip = dstip
        ipp.set_payload(payload)
        self._send_ethernet_packet(packet_type=ethernet.IP_TYPE,
                                   _src=self.my_mac, _dst=dsthw,
                                   payload=ipp, out_port=_out_port)

    def _send_udp_packet(self, srcport, dstport, _dstip, _dsthw, _payload, out_port):
        u = udp()
        u.srcport = srcport
        u.dstport = dstport
        u.set_payload(_payload)
        self._send_ip_packet(protocol=ipv4.UDP_PROTOCOL,
                             dstip=_dstip, dsthw=_dsthw, payload=u, _out_port=out_port)

    def _send_dns_response_packet(self, packet, dnsp, question, response, _out_port):
        r = dns()
        r.id = dnsp.id
        r.rd = dnsp.rd
        r.ra = True
        r.aa = 1
        r.questions.append(question)
        r.answers.append(response)

        self._send_udp_packet(srcport=53, dstport=packet.find('udp').srcport,
                              _dstip=packet.find('ipv4').srcip, _dsthw=packet.src,
                              _payload=r, out_port=_out_port)

    def _do_dns_packet(self, packet, event):
        dnsp = packet.find('dns')

        if len(dnsp.questions) > 1:
            self._drop(packet, event.ofp.buffer_id, event.port)
            return

        question = dnsp.questions[0]
        response = None
        log.debug("Question: %s" % (question.name))
        if question.qtype == dns.rr.A_TYPE and question.name == self.service_name:
            # Some one is asking about our service so let's
            # choose one of data centers and answer him
            dc_ip = self._choose_data_center()
            response = dns.rr(question.name, question.qtype, question.qclass,
                              0, 4, dc_ip)
        elif question.qtype == dns.rr.PTR_TYPE:
            # for now we assume that only our dns is resolvable
            response = dns.rr(question.name, question.qtype, question.qclass,
                                  0, len(self.service_name), self.service_name)
        else:
            self._drop(packet, event.ofp.buffer_id, event.port)
            return

        self._send_dns_response_packet(packet, dnsp, question, response, event.port)

    def _handle_PacketIn(self, event):
        packet = event.parsed

        #is this to us?
        if packet.dst == self.my_mac:
            log.debug("We have packet directed to us")
            if packet.find('dns') is not None:
                self._do_dns_packet(packet, event)
            else:
                self._drop(packet, event.ofp.buffer_id, event.port, 10)
        else:
            super(PSIKMainServerSwitch, self)._handle_PacketIn(event)

class PSIKComponent (object):
    def __init__(self, mss_dpid, mss_ip, mcs_dpid, dcs_dpids, decision_type, dcs_load):
        self.switches = set()
        self.mcs = PSIKLearningSwitch("mcs", mcs_dpid)
        self.dcs_load = [float(load[0]) for load in dcs_load]
        self.mss = PSIKMainServerSwitch("mss", mss_dpid, mss_ip, self.dcs_load)
        print "Data centers loads: " + str(self.dcs_load)
        self.dcs = list()
        i = 1
        for dpid in dcs_dpids:
            s_loads = dcs_load[i - 1][1]
            print s_loads
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
            dcs_load=[(1.0/3, [1.0/3, 1.0/3, 1.0/3]),
                      (1.0/3, [1.0/3, 1.0/3, 1.0/3]),
                      (1.0/3, [1.0/3, 1.0/3, 1.0/3])]):

    if mss_ip is None:
        mss_ip = IPAddr("10.254.254.254")

    mss_dpid = poxutil.str_to_dpid(mss_dpid)
    mcs_dpid = poxutil.str_to_dpid(mcs_dpid)
    for i in range(len(dcs_dpids)):
        dcs_dpids[i] = poxutil.str_to_dpid(dcs_dpids[i])

    core.registerNew(PSIKComponent, mss_dpid, mss_ip, mcs_dpid, dcs_dpids, DecisionType.DEC_STATIC, dcs_load)
