#!/bin/bash

ROOT=$PWD
ASTRODATA=/spark_mongo_tmp/julien.peloton/astrodata

echo "Update on the master"
cd $ASTRODATA/Catalog/ASTORB
./get_ASTORB.sh

cd $ASTRODATA/Catalog/IAUOBS
./get_IAUOBS.sh

cd $ASTRODATA/Theory/EOP
./get_EOP.sh

echo "Compress astrodata"
TARNAME=astrodata_new.tar.gz
cd /spark_mongo_tmp/julien.peloton
mv astrodata astrodata_new
tar -czf $TARNAME astrodata_new
mv astrodata_new astrodata

echo "Send archive to all executors"
pscp.pssh -p 12 -h ../ztf/spark_ips_nomaster /spark_mongo_tmp/julien.peloton/$TARNAME /spark-dir

echo "Migrate"
pssh -p 12 -t 100000000 -h ../ztf/spark_ips_nomaster -I < ./$ROOT/update_astrodata.sh
