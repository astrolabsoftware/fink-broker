# Troubleshooting guide

## List kafka topics

```shell
kubectl exec -it kafka-cluster-dual-role-0 -n kafka -- bin/kafka-topics.sh --bootstrap-server kafka-cluster-kafka-bootstrap.kafka:9092 --list
```

## Restart fink-broker

```shell
## Use --all if needed
kubectl delete -n spark sparkapplication fink-broker-distribution
argocd app sync fink-broker
```

## Debug fink-broker helm chart

```shell
cd fink-broker
helm install --debug fink ./chart -f ./chart/values-ci-noscience.yaml --dry-run
# Or
helm template --debug spark -f ./chart/values-ci-noscience.yaml --dry-run=client  -n spark ./chart
```

## ArgoCD

### Access argoCD web UI

```bash
kubectl port-forward -n argocd $(kubectl get  pods --selector=app.kubernetes.io/name=argocd-server -n argocd --output=jsonpath="{.items..metadata.name}") 8080
# Login is "admin, Password is set to "password", fix this in production
kubectl -n argocd patch secret argocd-secret  -p '{"stringData": {"admin.password": "$2a$10$rRyBsGSHK6.uc8fntPwVIuLVHgsAhAX7TcdrqW/RADU0uh7CaChLa", "admin.passwordMtime": "'$(date +%FT%T%Z)'"  }}'
```

### Re install fink stack

```bash
./e2e/argocd.sh
```



### Fine-tune "ignoreDifferences" field of an ArgoCD Application

```bash
# Install yq
sudo wget https://github.com/mikefarah/yq/releases/latest/download/yq_linux_amd64 -O /usr/bin/yq &&\\n    sudo chmod +x /usr/bin/yq
# Retrieve failedsyncmanifest.yaml file in ArgoCD web UI
yq failedsyncmanifest.yaml -o json > failedsyncmanifest.json
# Fine-tune 'jqPathExpressions'
cat failedsyncmanifest.json | jq '.spec.versions[].additionalPrinterColumns | select(. == [])'
```