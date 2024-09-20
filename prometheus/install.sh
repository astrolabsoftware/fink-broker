#!/bin/bash

DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"

helm delete -n monitoring prometheus-stack || echo "prometheus-stack not found"

helm install --version "61.3.1" prometheus-stack prometheus-community/kube-prometheus-stack -n monitoring -f $DIR/values.yaml

kubectl label namespace spark prometheus=true --overwrite
kubectl apply -f $DIR/podmon.yaml