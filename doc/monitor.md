# Monitor fink-broker platform

## Quick start

```bash
# Run the fink-ci script to install fink-broker from scratch
fink-ci.sh -m -s -b <branch-name>

# Port forward to grafana
# login as admin with password prom-operator
kubectl port-forward $(kubectl get  pods --selector=app.kubernetes.io/name=grafana -n  monitoring --output=jsonpath="{.items..metadata.name}") -n monitoring  3001:3000

# Run ssh tunnel on workstation for remote access
ssh fink_lpc -L 3001:localhost:3001 -N
# Open URL below in a browser
# login: admin
# password: prom-operator
curl http://localhost:3001
```

## Troubleshoot

```bash
# Check exporter is enabled:
kubectl exec -it -n spark fink-broker-stream2raw-driver -- curl http://localhost:8090/metrics

# Watch JMX exporter configuration
kubectl exec -it -n spark fink-broker-stream2raw-driver -- cat /etc/metrics/conf/prometheus.yaml

# Check spark metrics availability in prometheus database:
kubectl run -i --rm --tty shell --image=curlimages/curl -- sh
METRIC_NAME="spark_driver_livelistenerbus_queue_streams_size_type_gauges"
curl "http://prometheus-stack-kube-prom-prometheus.monitoring:9090/api/v1/query?query=$METRIC_NAME"

# Prometheus access:
kubectl port-forward -n monitoring prometheus-prometheus-stack-kube-prom-prometheus-0 9090

# Grafana access:
# login as admin with password prom-operator
kubectl port-forward $(kubectl get  pods --selector=app.kubernetes.io/name=grafana -n  monitoring --output=jsonpath="{.items..metadata.name}") -n monitoring  3000 &
```
