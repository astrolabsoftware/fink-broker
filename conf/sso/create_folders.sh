#!/bin/bash

# Define the target directories
ASTRODATA_DIR="/spark_mongo_tmp/julien.peloton/astrodata"
SSOCARDS_DIR="/spark_mongo_tmp/julien.peloton/ssocards"

# /spark-dir is where data is stored remotely
# symbolic link with /spark_mongo_tmp which does not
# exist remotely.
REMOTE_DIR="/spark-dir"

# Check and create astrodata directory
if [ ! -d "$ASTRODATA_DIR" ] && [ ! -L "$ASTRODATA_DIR" ]; then
    ln -s ${REMOTE_DIR}/astrodata "$ASTRODATA_DIR"
fi

# Check and create ssocards directory
if [ ! -d "$SSOCARDS_DIR" ] && [ ! -L "$SSOCARDS_DIR" ]; then
    ln -s ${REMOTE_DIR}/ssocards "$SSOCARDS_DIR"
fi
