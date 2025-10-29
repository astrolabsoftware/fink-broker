#!/bin/bash

NOW=$(date +%Y-%m-%d-%H%M%S)
TARNAME=miriade-${NOW}.tar.gz

echo "Compress /opt/miriade"
tar -czf $TARNAME /opt/miriade

echo "Send archive to all executors"
pscp.pssh -p 12 -h ../ztf/spark_ips $TARNAME /opt

echo "Install eproc on all executors"
pssh -p 12 -t 100000000 -h ../ztf/spark_ips -I < ./install_eproc.sh
