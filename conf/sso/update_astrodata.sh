#!/bin/bash

DAY=`date +"%Y%m%d"`

cd /spark-dir

# Add a condition of the folder does not exist
mv astrodata astrodata_$DAY
tar xzvf astrodata_new.tar.gz
mv astrodata_new astrodata
