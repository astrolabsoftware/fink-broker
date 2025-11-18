#!/bin/bash

pssh -p 12 -t 100000000 -h ../ztf/spark_ips_nomaster dnf install -y libgfortran parallel
