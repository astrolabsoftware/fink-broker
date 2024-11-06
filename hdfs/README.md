kubectl run -it --image apache/hadoop:3.4.0 hdfs-client
kubectl exec -it hdfs-client bash
 hdfs dfs -fs hdfs://simple-hdfs-namenode-default-0.simple-hdfs-namenode-default.default.svc.cluster.local:8020 -df
 hdfs dfs -fs hdfs://simple-hdfs-namenode-default-0.simple-hdfs-namenode-default.default.svc.cluster.local:8020 -ls
