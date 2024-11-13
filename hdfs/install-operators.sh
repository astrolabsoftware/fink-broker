curl -L -o /tmp/stackablectl https://github.com/stackabletech/stackable-cockpit/releases/download/stackablectl-24.7.0/stackablectl-x86_64-unknown-linux-gnu
chmod +x /tmp/stackable
/tmp/stackablectl operator install commons=24.7.0   secret=24.7.0 listener=24.7.0  zookeeper=24.7.0  hdfs=24.7.0
