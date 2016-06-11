#!/usr/bin/python

from socket import *
import thread

BUFF = 4096
HOST = '0.0.0.0'
PORT = 9999
BLOCK_SIZE = 4096

def handler(clientsock,addr):
    while 1:
        data = clientsock.recv(BUFF)
        if not data:
            break

        input_data = str(data).split(" ")
        try:
            nblocks_to_read = int(input_data[0])
            ndata_to_send = int(input_data[1])

            # let's stress our server a little bit
            f = open('/dev/urandom', 'r')
            for i in range(nblocks_to_read):
                block = f.read(BLOCK_SIZE)

            # let's stress our link a little bit
            output = 'a' * ndata_to_send
            clientsock.send(output)
        except TypeError:
            break
        except IndexError:
            break

    clientsock.close()
    print addr, "- closed connection" #log on console

if __name__=='__main__':
    ADDR = (HOST, PORT)
    serversock = socket(AF_INET, SOCK_STREAM)
    serversock.setsockopt(SOL_SOCKET, SO_REUSEADDR, 1)
    serversock.bind(ADDR)
    serversock.listen(5)
    while 1:
        print 'waiting for connection... listening on port', PORT
        clientsock, addr = serversock.accept()
        print '...connected from:', addr
        thread.start_new_thread(handler, (clientsock, addr))
