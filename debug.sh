#!/bin/bash

# Allow to manually set-up and debug spark-submit option

# @author  Fabrice Jammes

set -euo pipefail

DIR=$(cd "$(dirname "$0")"; pwd -P)
. "$DIR/conf.sh"

# See update 4 https://stackoverflow.com/questions/65353164/how-to-read-files-uploaded-by-spark-submit-on-kubernetes

# See https://jaceklaskowski.github.io/spark-kubernetes-book/demo/spark-and-local-filesystem-in-minikube/#hostpath
spark-submit --master "k8s://https://127.0.0.1:46469" \
    --deploy-mode cluster \
    --conf spark.executor.instances=1 \
    --conf spark.kubernetes.authenticate.driver.serviceAccountName=spark \
    --conf spark.kubernetes.container.image="gitlab-registry.in2p3.fr/astrolabsoftware/fink/fink-broker:2.7.1-98-g9f81556" \
    --conf spark.driver.extraJavaOptions="-Divy.cache.dir=/tmp -Divy.home=/tmp" \
    --conf spark.hadoop.fs.s3a.endpoint=http://minio.minio-dev:9000 \
    --conf spark.hadoop.fs.s3a.access.key="minioadmin" \
    --conf spark.hadoop.fs.s3a.secret.key="minioadmin" \
    --conf spark.hadoop.mapreduce.fileoutputcommitter.algorithm.version=2 \
    --conf spark.hadoop.fs.s3a.connection.ssl.enabled=false \
    --conf spark.hadoop.fs.s3a.fast.upload=true \
    --conf spark.hadoop.fs.s3a.path.style.access=true \
    --conf spark.hadoop.fs.s3a.aws.credentials.provider=org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider \
    --conf spark.hadoop.fs.s3a.impl="org.apache.hadoop.fs.s3a.S3AFileSystem" \
    --files /tmp/fink_local_files1925895313/kafka_producer_jaas.conf \
    --conf spark.kubernetes.file.upload.path=/tmp/jaas \
    --conf spark.kubernetes.driver.volumes.emptyDir.jaas.mount.path=/tmp/jaas \
    --conf spark.kubernetes.driver.volumes.emptyDir.jaas.options.path=/tmp/jaas \
    --conf spark.kubernetes.executor.volumes.emptyDir.jaas.mount.path=/tmp/jaas \
    --conf spark.kubernetes.executor.volumes.emptyDir.jaas.options.path=/tmp/jaas \
    local:///home/fink/fink-broker/bin/raw2science.py \
    -log_level "INFO" \
    -online_data_prefix "s3a://fink-broker-online" \
    -producer "sims" \
    -tinterval "2" \
    -night "20190903"


# spark-submit \
#   --master k8s://https://127.0.0.1:46469 \
#   --deploy-mode cluster \
#   --files test.me \
#   --conf spark.kubernetes.file.upload.path=file:///$SOURCE_DIR \
#   --conf spark.kubernetes.driver.volumes.$VOLUME_TYPE.$VOLUME_NAME.mount.path=$MOUNT_PATH \
#   --conf spark.kubernetes.driver.volumes.$VOLUME_TYPE.$VOLUME_NAME.options.path=$MOUNT_PATH \
#   --conf spark.kubernetes.executor.volumes.$VOLUME_TYPE.$VOLUME_NAME.mount.path=$MOUNT_PATH \
#   --conf spark.kubernetes.executor.volumes.$VOLUME_TYPE.$VOLUME_NAME.options.path=$MOUNT_PATH \
#   --name $POD_NAME \
#   --class meetup.SparkApp \
#   --conf spark.kubernetes.container.image="gitlab-registry.in2p3.fr/astrolabsoftware/fink/fink-broker:2.7.1-98-g9f81556" \
#   --conf spark.kubernetes.driver.pod.name=$POD_NAME \
#   --conf spark.kubernetes.authenticate.driver.serviceAccountName=spark \
#   --verbose \
#   local:///home/fink/fink-broker/bin/raw2science.py \
#   -log_level "INFO" \
#   -online_data_prefix "s3a://fink-broker-online" \
#   -producer "sims" \
#   -tinterval "2" \
#   -night "20190903"
