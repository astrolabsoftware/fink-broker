#!/bin/bash

echo "Send astrodata to all executors"
pscp.pssh -p 12 -h ../ztf/spark_ips -r /spark_mongo_tmp/julien.peloton/astrodata /spark-dir
