#!/bin/bash

ROOTPATH=${FINK_HOME}/conf/sso/cron

SCRIPT=fink_run_ephemcc4*.sh
USERCONF=.eproc-4.*
IOFILE=default-ephemcc-observation.xml
IOFILE2=default-ephemcc-observation_radec.xml

cp $ROOTPATH/$SCRIPT $ROOTPATH/$USERCONF $ROOTPATH/$IOFILE $ROOTPATH/$IOFILE2 /tmp/
chmod 777 /tmp/$SCRIPT /tmp/$USERCONF /tmp/$IOFILE /tmp/$IOFILE2

# Assuming 12 slaves
# FIXME: use hostfile
for i in {1..12}; do
    scp /tmp/$SCRIPT /tmp/$USERCONF /tmp/$IOFILE /tmp/$IOFILE2 vdslave${i}:/tmp/
    ssh vdslave$i chmod 777 /tmp/$SCRIPT /tmp/$USERCONF /tmp/$IOFILE /tmp/$IOFILE2
done
