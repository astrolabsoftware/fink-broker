# Minio should be disabled in fink-cd!

kubectl run --image apache/hadoop:3.4.0 hdfs-client -- sleep infinity

kubectl exec -it hdfs-client bash
    hdfs dfs -fs hdfs://simple-hdfs-namenode-default-0.simple-hdfs-namenode-default.default.svc.cluster.local:8020 -df
    hdfs dfs -fs hdfs://simple-hdfs-namenode-default-0.simple-hdfs-namenode-default.default.svc.cluster.local:8020 -ls
    export HADOOP_USER_NAME=stackable
    hdfs dfs -fs hdfs://simple-hdfs-namenode-default-0.simple-hdfs-namenode-default.default.svc.cluster.local:8020 -touch /toto
    hdfs dfs -fs hdfs://simple-hdfs-namenode-default-0.simple-hdfs-namenode-default.default.svc.cluster.local:8020 -ls /

