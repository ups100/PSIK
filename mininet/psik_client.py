#!/usr/bin/python
from socket import *
import random

hostname = "service.psik.com"
PORT = 9999

MIN_CPU_BLOCKS = 100
MAX_CPU_BLOCKS = 100000

MIN_RECV_DATA = 1024
MAX_RECV_DATA = 1024*1024

if __name__=='__main__':
    sock = socket(AF_INET, SOCK_STREAM)
    server_address = (hostname, PORT)
    print  'connecting to %s port %s' % server_address
    sock.connect(server_address)

    try:
        cpu_blocks = random.randint(MIN_CPU_BLOCKS, MAX_CPU_BLOCKS)
        data_amount = random.randint(MIN_RECV_DATA, MAX_RECV_DATA)

        message = str(cpu_blocks) + " " + str(data_amount)
        sock.sendall(message)

        received = 0
        while received < data_amount:
            data = sock.recv(MIN_RECV_DATA)
            received += len(data)
        print received
    finally:
        sock.close()
