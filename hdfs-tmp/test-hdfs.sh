#!/bin/bash

set -euo pipefail

DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" >/dev/null 2>&1 && pwd )"

# Step 1: Create the webhdfs.yaml file
cat <<EOF > $DIR.webhdfs.yaml
apiVersion: apps/v1
kind: StatefulSet
metadata:
  name: webhdfs
  labels:
    app: webhdfs
spec:
  replicas: 1
  serviceName: webhdfs-svc
  selector:
    matchLabels:
      app: webhdfs
  template:
    metadata:
      labels:
        app: webhdfs
    spec:
      containers:
        - name: webhdfs
          image: docker.stackable.tech/stackable/testing-tools:0.2.0-stackable0.0.0-dev
          stdin: true
          tty: true
EOF

# Step 2: Apply the StatefulSet and monitor progress
echo "Applying webhdfs StatefulSet..."
kubectl apply -f $DIR/webhdfs.yaml
echo "Waiting for webhdfs pod to be ready..."
kubectl rollout status --watch --timeout=5m statefulset/webhdfs

# Step 3: Check the root directory status in HDFS (should be empty)
echo "Checking root directory in HDFS..."
kubectl exec -n default webhdfs-0 -- curl -s -XGET "http://simple-hdfs-namenode-default-0.simple-hdfs-namenode-default.default.svc.cluster.local:9870/webhdfs/v1/?op=LISTSTATUS"

# Step 4: Create a sample file for uploading
echo "Creating sample file testdata.txt..."
echo "This is a test file for HDFS upload." > $DIR/testdata.txt

# Step 5: Copy the file to the helper pod
echo "Copying testdata.txt to webhdfs pod..."
kubectl cp -n default $DIR/testdata.txt webhdfs-0:/tmp

# Step 6: Initiate a two-step PUT request to create the file in HDFS
echo "Initiating file creation in HDFS (first step)..."
create_response=$(kubectl exec -n default webhdfs-0 -- \
curl -s -XPUT -T /tmp/testdata.txt "http://simple-hdfs-namenode-default-0.simple-hdfs-namenode-default.default.svc.cluster.local:9870/webhdfs/v1/testdata.txt?user.name=stackable&op=CREATE&noredirect=true")

# Extract the location for the second PUT request
location=$(echo "$create_response" | grep -o 'http://[^"]*')
echo "Location for second PUT request: $location"

# Step 7: Complete the file upload
echo "Completing file creation in HDFS (second step)..."
kubectl exec -n default webhdfs-0 -- curl -s -XPUT -T /tmp/testdata.txt "$location"

# Step 8: Verify that the file has been created in HDFS
echo "Re-checking root directory in HDFS to verify file creation..."
kubectl exec -n default webhdfs-0 -- curl -s -XGET "http://simple-hdfs-namenode-default-0.simple-hdfs-namenode-default.default.svc.cluster.local:9870/webhdfs/v1/?op=LISTSTATUS"

# Step 9: Delete the file from HDFS to clean up
echo "Deleting testdata.txt from HDFS..."
kubectl exec -n default webhdfs-0 -- curl -s -XDELETE "http://simple-hdfs-namenode-default-0.simple-hdfs-namenode-default.default.svc.cluster.local:9870/webhdfs/v1/testdata.txt?user.name=stackable&op=DELETE"

# Clean up local files
rm $DIR/webhdfs.yaml $DIR/testdata.txt
echo "Cleanup completed. HDFS testing script finished."
