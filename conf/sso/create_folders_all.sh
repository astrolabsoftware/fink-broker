#!/bin/bash

pssh -e . -p 12 -t 100000000 -h ../ztf/spark_ips_nomaster -I < ./create_folders.sh
