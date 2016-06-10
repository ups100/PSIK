#!/usr/bin/python

from mininet.net import Mininet
from mininet.node import Controller
from mininet.cli import CLI
from mininet.log import setLogLevel, info

import sys, getopt

def int2dpid( dpid ):
   try:
      dpid = hex( dpid )[ 2: ]
      dpid = '0' * ( 16 - len( dpid ) ) + dpid
      return dpid
   except IndexError:
      raise Exception( 'Unable to derive default datapath ID - '
                       'please either specify a dpid or use a '
               'canonical switch name such as s23.' )

def add_clients(nclients, parent, net):
    for client in range(nclients):
        cli_name = 'c' + str(client + 1)
        cli_ip = '20.0.0.' + str(client + 1)
        cli = net.addHost(cli_name, ip=cli_ip)
        net.addLink(parent, cli)

def add_data_center(nservers, data_center, parent, net):
    for server in range(nservers):
        srv_name = 'dc' + str(data_center) + 'h' + str(server + 1)
        srv_ip = '10.0.' + str(data_center) + '.' + str(server + 1)
        srv = net.addHost(srv_name, ip=srv_ip);
        net.addLink(parent, srv);
        
def add_data_centers(data_centers, parent, net):
    for data_center in range(len(data_centers)):
        # data center switch
        switch_name = 'dcs' + str(data_center + 1)
        dcs = net.addSwitch(switch_name, dpid=int2dpid(100 + data_center + 1));
        net.addLink(parent, dcs)
        add_data_center(data_centers[data_center], data_center + 1, dcs, net)

def create_network(nclients, data_centers):
    net = Mininet()

    # add main servers switch
    mss = net.addSwitch('mss', dpid=int2dpid(1))
    # add main client Switch
    mcs = net.addSwitch('mcs', dpid=int2dpid(2))
    # and link between them
    net.addLink(mcs, mss)

    add_data_centers(data_centers, mss, net)
    add_clients(nclients, mcs, net)

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

    run_network(network)

if __name__ == '__main__':
    setLogLevel('info');
    main(sys.argv[1:])
