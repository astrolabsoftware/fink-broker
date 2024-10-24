# Monitor fink-broker platform

```bash
# Run the fink-ci script to install fink-broker from scratch
fink-ci.sh -m -s -b <branch-name>

# Port forward to grafana
kubectl port-forward $(kubectl get  pods --selector=app.kubernetes.io/name=grafana -n  monitoring --output=jsonpath="{.items..metadata.name}") -n monitoring  3001:3000

# Run ssh tunnel on workstation for remote access
ssh fink_lpc -L 3001:localhost:3001 -N
# Open URL below in a browser
# login: admin
# password: prom-operator
curl http://localhost:3001
```
