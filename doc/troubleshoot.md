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
