#!/bin/bash

pssh -p 12 -t 100000000 -h ../ztf/spark_ips dnf install -y libgfortran
