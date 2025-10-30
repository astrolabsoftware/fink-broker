#!/bin/bash

# Folder to compress on Master
FOLDER_TO_COMPRESS="/spark_mongo_tmp/julien.peloton/astrodata" 

# File containing the list of hosts
HOSTFILE="../ztf/spark_ips_nomaster"

# Folder on remotes where to decompress
REMOTE_FOLDER="/spark-dir/astrodata"

# Backup name with date
CURRENT_DATE=$(date +"%Y%m%d")
BACKUP_FOLDER="${REMOTE_FOLDER}_${CURRENT_DATE}_backup"

# Step 1: Check if the folder exists on each machine and back it up
pssh -p 12 -t 100000000 -h "${HOSTFILE}" "if [ -d '${REMOTE_FOLDER}' ]; then
  mv '${REMOTE_FOLDER}' '${BACKUP_FOLDER}' && echo 'Backup created at ${BACKUP_FOLDER} on $(hostname)';
else
  echo 'No existing folder to backup on $(hostname).';
fi"

# Step 2: Compress the folder on Machine 1
tar -czf astrodata_compressed.tar.gz -C "$(dirname "${FOLDER_TO_COMPRESS}")" "$(basename "${FOLDER_TO_COMPRESS}")"

# Step 3: Transfer the compressed folder to all machines
pssh.pscp astrodata_compressed.tar.gz -h "${HOSTFILE}:${REMOTE_FOLDER}/"

# Step 4: Decompress the folder on all machines
pssh -h "${HOSTFILE}" "tar -xzf '${REMOTE_FOLDER}/astrodata_compressed.tar.gz' -C '${REMOTE_FOLDER}' && rm '${REMOTE_FOLDER}/astrodata_compressed.tar.gz'"

