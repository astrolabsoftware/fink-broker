

## Install prometheus

./install.sh
kubectl port-forward -n monitoring prometheus-prometheus-stack-kube-prom-prometheus-0 9090

## Check prom-exporter for fink-broker

kubectl exec -it -n spark fink-broker-stream2raw-driver -- curl http://localhost:8090

## Configure prometheus

kubectl label namespaces spark prometheus=true
kubectl apply -f $DIR/podmon.yaml


### Web UI access

# Prometheus access:
kubectl port-forward -n monitoring prometheus-prometheus-stack-kube-prom-prometheus-0 9090

# Grafana access:
# login as admin with password prom-operator
kubectl port-forward $(kubectl get  pods --selector=app.kubernetes.io/name=grafana -n  monitoring --output=jsonpath="{.items..metadata.name}") -n monitoring  3000 &
