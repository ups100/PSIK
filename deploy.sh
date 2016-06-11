#!/bin/bash

set -e

# Top level of repo should contain symlinks to sshfs mounted roots of both vms
GIT_TOP=`git rev-parse --show-toplevel`
MININET_VM="./mininet-vm"
POX_VM="./pox-vm"

cd $GIT_TOP

cp "./mininet/topo.py" "$MININET_VM/home/mininet/"
cp "./mininet/psik_server.py" "$MININET_VM/home/mininet/"
cp "./mininet/psik_client.py" "$MININET_VM/home/mininet/"

find pox -name *.py | xargs -L1 -I'{}' cp '{}' "$POX_VM/home/mininet/pox/ext/"
