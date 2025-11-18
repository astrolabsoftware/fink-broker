#!/bin/bash

nslaves=12
FOLDER=fink_fat

rm -rf /tmp/ramdisk/${FOLDER}
mkdir -p /tmp/ramdisk/${FOLDER}
chmod 777 /tmp/ramdisk/${FOLDER}

# Intallation on the slaves
# FIXME: use hostfile
for i in $(seq 1 $nslaves); do
  echo vdslave $i ...
  ssh vdslave$i rm -rf /tmp/ramdisk/${FOLDER}
  ssh vdslave$i mkdir -p /tmp/ramdisk/${FOLDER}
  ssh vdslave$i chmod 777 /tmp/ramdisk/${FOLDER}
done
