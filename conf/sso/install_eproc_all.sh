#!/bin/bash

NOW=$(date +%Y-%m-%d-%H%M%S)
TARNAME=miriade-${NOW}.tar.gz

FOLDER=$(readlink -f /opt/miriade)

echo "Compress /opt/miriade"
tar -czf $TARNAME $FOLDER

echo "Send archive to all executors"
pscp.pssh -p 12 -h ../ztf/spark_ips_nomaster $TARNAME /opt
pscp.pssh -p 12 -h ../ztf/spark_ips_nomaster ./install_eproc.sh /tmp/install_eproc.sh
 
echo "Install eproc on all executors"
pssh -p 12 -t 100000000 -h ../ztf/spark_ips_nomaster -o /tmp/eproc_out/ -e /tmp/eproc_err/ -i "/tmp/install_eproc.sh $TARNAME $FOLDER"
