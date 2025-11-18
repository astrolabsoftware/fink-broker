#!/bin/bash

# Master
rm -rf /tmp/ramdisk/spins
mkdir -p /tmp/ramdisk/spins
chmod 777 /tmp/ramdisk/spins

# Intallation on the slaves
# FIXME: use hostfile
nslaves=12
for i in $(seq 1 $nslaves); do
  echo vdslave $i ...
  ssh vdslave$i "rm -f /tmp/*.tmp"
  ssh vdslave$i rm -rf /tmp/ramdisk/spins
  ssh vdslave$i mkdir -p /tmp/ramdisk/spins
  ssh vdslave$i chmod 777 /tmp/ramdisk/spins
done
