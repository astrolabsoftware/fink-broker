# Troubleshooting guide

## Run s5cmd (s3 client)

From inside the k8s cluster:

```shell
kubectl run -it --rm s5cmd --image=peakcom/s5cmd --env AWS_ACCESS_KEY_ID=minio --env  AWS_SECRET_ACCESS_KEY=minio123 --env S3_ENDPOINT_URL=https://minio.minio:443 -- --log debug --no-verify-ssl ls
```

Interactive access:
```shell
kubectl run -it --rm s5cmd --image=peakcom/s5cmd --env AWS_ACCESS_KEY_ID=minio --env  AWS_SECRET_ACCESS_KEY=minio123 --env S3_ENDPOINT_URL=https://minio.minio:443 --command -- sh
/s5cmd --log debug --no-verify-ssl ls -H  "s3://fink-broker-online/raw/20200101/"
/s5cmd --log debug --no-verify-ssl ls "s3://fink-broker-online/*"
```

## Run HDFS client

TODO see github.com/k8s-school/demo-hdfs for advanced troubleshooting.


From inside the k8s cluster:

```shell
kubectl run \
    --env HDFS_URL="hdfs://simple-hdfs-namenode-default-0.simple-hdfs-namenode-default.hdfs.svc.cluster.local:8020" \
    --env HADOOP_USER_NAME=stackable \
    --image apache/hadoop:3.4.0 hdfs-client -- sleep infinity
```

Interactive access:
```shell
kubectl exec -it hdfs-client -- bash
    hdfs dfs -fs $HDFS_URL -df -h
    # check received data
    hdfs dfs -fs $HDFS_URL -du -h /user/185
    # other useful commands
    hdfs dfs -fs $HDFS_URL -ls
    hdfs dfs -fs $HDFS_URL -touch /newfile
    hdfs dfs -fs $HDFS_URL -ls /user/185

```