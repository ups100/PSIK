#!/usr/bin/python

from mininet.net import Mininet
from mininet.node import Controller, RemoteController
from mininet.cli import CLI
from mininet.log import setLogLevel, info

import sys, getopt
import socket

def vid_mac2dpid(vid, mac):
   try:
      mac_hex = mac.replace(":", "")
      vid_hex = hex(vid)[2:]

      if len(vid_hex) > 4:
         raise Exception('VID too long')

      dpid = '0' * (4 - len(vid_hex)) + vid_hex + '0' * (12 - len(mac_hex)) + mac_hex
      return dpid
   except IndexError:
       raise Exception( 'Unable to derive default datapath ID - '
                        'please either specify a dpid or use a '
                        'canonical switch name such as s23.' )

def add_clients(nclients, parent, parent_mac, dns_ip, net):
    for client in range(nclients):
       cli_name = 'c' + str(client + 1)
       cli_ip = '10.1.0.' + str(client + 1)
       cli_netmask = '255.0.0.0'
       cli_mac = parent_mac[:-2] + hex(client + 1)[2:].zfill(2)
       cli = net.addHost(cli_name, ip=cli_ip, netmask=cli_netmask, mac=cli_mac, dns=dns_ip)
       net.addLink(parent, cli)

def add_data_center(nservers, data_center, parent, parent_mac, dns_ip, net):
    for server in range(nservers):
       srv_name = 'dc' + str(data_center) + 'h' + str(server + 1)
       srv_ip = '10.0.' + str(data_center) + '.' + str(server + 1)
       srv_netmask = '255.0.0.0'
       srv_mac = parent_mac[:-2] + hex(server + 1)[2:].zfill(2)
       srv = net.addHost(srv_name, ip=srv_ip, netmask=srv_netmask, mac=srv_mac, dns=dns_ip);
       net.addLink(parent, srv);
      
def add_data_centers(data_centers, parent, parent_mac, dns, net):
    for data_center in range(len(data_centers)):
       # data center switch
       switch_name = 'dcs' + str(data_center + 1)
       mac = parent_mac[:-5] +  hex(data_center + 1)[2:].zfill(2) + ":00"
       vid = 100 + data_center + 1
       dc_dpid = vid_mac2dpid(vid, mac) 
       print "Data center " + str(data_center) + " dpid: " + dc_dpid
       dcs = net.addSwitch(switch_name, dpid=dc_dpid);
       net.addLink(parent, dcs)
       add_data_center(data_centers[data_center], data_center + 1, dcs, mac, dns, net)

def create_network(nclients, data_centers):
    net = Mininet()

    # add main servers switch
    mss_mac = "00:00:00:01:00:00"
    vid = 1
    switch_dpid = vid_mac2dpid(vid, mss_mac)
    print "MSS dpid: " + switch_dpid
    mss = net.addSwitch('mss', dpid=switch_dpid)
    # add main client Switch
    mcs_mac = "00:00:00:02:00:00"
    vid = 2
    switch_dpid = vid_mac2dpid(vid, mcs_mac)
    print "MCS dpid: " + switch_dpid
    mcs = net.addSwitch('mcs', dpid=switch_dpid)
    # and link between them
    net.addLink(mcs, mss)

    dns = "10.254.254.254"
    add_data_centers(data_centers, mss, mss_mac, dns, net)
    add_clients(nclients, mcs, mcs_mac, dns, net)

    return net

def run_network(net):
    info('*** Starting Network ***\n')
    net.start()
    info('*** Running CLI ***\n')
    CLI(net)
    info('*** Stopping network ***\n')
    net.stop()

def main(argv):
    n_clients=3
    data_centers=(3, 3, 3)
    help_str='topo.py --nclients=<number of clients> --data-centers=(<n servers in 1 st data center>, <second>, <third> ...)'

    try:
        opts, args = getopt.getopt(argv, "hc:d:", ["nclients=", "data-centers="])
    except getopt.GetOptError:
        print help_str
        sys.exit(2)

    for opt, arg in opts:
        if opt =='-h':
            print help_str
            sys.exit();
        elif opt in ("-c", "--nclients"):
            try:
                n_clients=int(arg)
            except ValueError:
                print "Number of clients should be integer"
        elif opt in ("-d", "--data-centers"):
            dc_list=arg.split()
            for dc in dc_list:
                try:
                    int(dc)
                except ValueError:
                    print "Data center value is not int"
                    sys.exit(2)
            data_centers=dc_list;

    print "N clients: ", n_clients
    print "Data centers: ", data_centers

    network = create_network(n_clients, data_centers)

    ctrl_host = "pox-machine"
    ctrl_ip = socket.gethostbyname(ctrl_host)
    ctrl = network.addController('c0', controller=RemoteController, ip=ctrl_ip, port=6633)

    run_network(network)

if __name__ == '__main__':
    setLogLevel('info');
    main(sys.argv[1:])
