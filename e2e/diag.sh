#!/bin/bash

# Licensed to the Apache Software Foundation (ASF) under one or more
# contributor license agreements.  See the NOTICE file distributed with
# this work for additional information regarding copyright ownership.
# The ASF licenses this file to You under the Apache License, Version 2.0
# (the "License"); you may not use this file except in compliance with
# the License.  You may obtain a copy of the License at
#
#    http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

# Collect Kubernetes diagnostics for CI failure analysis

# @author  Fabrice Jammes

set -uo pipefail

DIAG_DIR="${1:-/tmp/k8s-diag}"
mkdir -p "$DIAG_DIR"

echo "=== kubectl get pods -A ==="
kubectl get pods -A --no-headers | tee "$DIAG_DIR/pods-all.txt"

# Pods not in a healthy terminal or running state
abnormal_pods=$(kubectl get pods -A --no-headers | \
    grep -v -E '\s+(Running|Completed|Succeeded|Terminating)\s+' | \
    awk '{print $1"/"$2}') || true

if [ -z "$abnormal_pods" ]; then
    echo "No abnormal pods found"
    exit 0
fi

echo "=== Abnormal pods ==="
echo "$abnormal_pods"

for ns_pod in $abnormal_pods; do
    ns=$(echo "$ns_pod" | cut -d'/' -f1)
    pod=$(echo "$ns_pod" | cut -d'/' -f2)
    safe_name="${ns}-${pod}"

    echo "--- kubectl describe pod $pod -n $ns ---"
    kubectl describe pod "$pod" -n "$ns" > "$DIAG_DIR/describe-${safe_name}.txt" 2>&1 || true

    echo "--- kubectl logs $pod -n $ns ---"
    kubectl logs "$pod" -n "$ns" --all-containers=true \
        > "$DIAG_DIR/logs-${safe_name}.txt" 2>&1 || true

    kubectl logs "$pod" -n "$ns" --all-containers=true --previous \
        > "$DIAG_DIR/logs-prev-${safe_name}.txt" 2>&1 || true
done
