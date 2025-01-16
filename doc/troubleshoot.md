# Troubleshooting guide

## Run s5cmd (s3 client)

From inside the k8s cluster:

```shell
kubectl run -it --rm s5cmd --image=peakcom/s5cmd --env AWS_ACCESS_KEY_ID=minio --env  AWS_SECRET_ACCESS_KEY=minio123 --env S3_ENDPOINT_URL=https://minio.minio:443 -- --log debug --no-verify-ssl ls
```

Interactive access:
```shell
kubectl run -it --rm s5cmd --image=peakcom/s5cmd --env AWS_ACCESS_KEY_ID=minio --env  AWS_SECRET_ACCESS_KEY=minio123 --env S3_ENDPOINT_URL=https://minio.minio:443 --command -- sh
/s5cmd --log debug --no-verify-ssl ls -H  "s3://fink-broker-online/raw/20200101/"
/s5cmd --log debug --no-verify-ssl ls "s3://fink-broker-online/*"
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
```

## ArgoCD

### Access argoCD web UI

```bash
kubectl port-forward -n argocd $(kubectl get  pods --selector=app.kubernetes.io/name=argocd-server -n argocd --output=jsonpath="{.items..metadata.name}") 8080
# Login is "admin, Password is set to "password", fix this in production
kubectl -n argocd patch secret argocd-secret  -p '{"stringData": {"admin.password": "$2a$10$rRyBsGSHK6.uc8fntPwVIuLVHgsAhAX7TcdrqW/RADU0uh7CaChLa", "admin.passwordMtime": "'$(date +%FT%T%Z)'"  }}'
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