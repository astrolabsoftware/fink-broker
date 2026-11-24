#!/bin/bash

pssh -p 12 -t 100000000 \
    -o /tmp/python_dustmap_out/ \
    -e /tmp/python_dustmap_err/ \
    -h /localhome/julien.peloton/fink-broker/conf/ztf/spark_ips "python -c \"import requests.utils;requests.utils.default_user_agent = lambda: 'Mozilla/5.0';from dustmaps.config import config;import dustmaps.sfd;dustmaps.sfd.fetch()\""
