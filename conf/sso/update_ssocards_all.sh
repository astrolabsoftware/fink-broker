#!/bin/bash
set -e

WORKDIR=$PWD

# Folder to compress on Master, resolving symlink
FOLDER_TO_COMPRESS="/spark_mongo_tmp/julien.peloton/ssocards/ssoCard-latest"
RESOLVED_FOLDER=$(readlink -f "$FOLDER_TO_COMPRESS")

# File containing the list of hosts
HOSTFILE="../ztf/spark_ips_nomaster"

# Folder on remotes where to decompress
REMOTE_FOLDER_BASE="/spark-dir"
REMOTE_FOLDER="${REMOTE_FOLDER_BASE}/ssocards/ssoCard-latest"

ARCHIVE_NAME="ssocards_compressed.tar.gz"

# Backup name with date
CURRENT_DATE=$(date +%Y-%m-%d-%H%M%S)
BACKUP_FOLDER="${REMOTE_FOLDER}_${CURRENT_DATE}_backup"

echo "Step 1: Check if the folder exists on each machine and back it up"
pssh -p 12 -t 100000000 -h "${HOSTFILE}" "if [ -d '${REMOTE_FOLDER}' ]; then
  mv '${REMOTE_FOLDER}' '${BACKUP_FOLDER}' && echo 'Backup created at ${BACKUP_FOLDER} on $(hostname)';
else
  echo 'No existing folder to backup on $(hostname).';
fi"

echo "Step 2: Compress the folder on master"
tar -cf - -C "$(dirname "${RESOLVED_FOLDER}")" "$(basename "${RESOLVED_FOLDER}")" | pigz > ${ARCHIVE_NAME}

echo "Step 3: Transfer the compressed folder to all machines"
pscp.pssh -p 12 -h "${HOSTFILE}" ${ARCHIVE_NAME} ${REMOTE_FOLDER_BASE}


echo "Step 4: Decompress the folder on all machines using pigz"
pssh -p 12 -t 100000000 -h "${HOSTFILE}" "tar -xOf ${REMOTE_FOLDER_BASE}/${ARCHIVE_NAME} | pigz -d | tar -xf - -C ${REMOTE_FOLDER_BASE} && rm ${REMOTE_FOLDER_BASE}/${ARCHIVE_NAME}"
