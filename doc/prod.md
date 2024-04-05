# Install fink-broker platform in production

## Pre-requisites

Access to a bootstrap server which as access to the production Kubernetes server.
All below command are on launched on the bootstrap server, which is based on a Fedora distribution.

## Install golang
```shell
GO_VERSION="1.21.5"
sudo rm -rf /usr/local/go
curl -sSL "https://dl.google.com/go/go$GO_VERSION.linux-amd64.tar.gz" | sudo tar -C /usr/local -xz
echo 'export PATH=/usr/local/go/bin:$PATH' >> ~/.bashrc
echo 'export GOPATH="$HOME/go"' >> ~/.bashrc
echo 'export PATH=$PATH:/usr/local/go/bin:$GOPATH/bin' >> ~/.bashrc
```

## Install ciux

`go install github.com/k8s-school/ciux@v0.0.3-rc2`

## Clone source code
```shell
mkdir -p src/astrolabsoftware
cd src/astrolabsoftware/
git clone https://github.com/astrolabsoftware/fink-broker.git
cd src/astrolabsoftware/fink-broker
```

## Prepare run

```shell
ciux ignite $PWD
ktbx install kubectl
# Add kubeconfig for production cluster
mkdir .kube
cat > ~/.kube/config
```

### Create configuration files

Configuration file is in `fink-broker/deploy/finkctl.yaml`


```shell
ln -s fink-broker/deploy/finkctl.yaml finkctl.yaml
cat finkctl.secret.yaml
s3:
  id: "<S3_USER_ID>"
  secret: "<S3_SECRET>"
distribution:
  kafka:
    username: "<SPARK_USER>"
    # If empty, password is set to "kubectl get -n kafka secrets/fink-producer --template={{.data.password}} | base64 --decode"
    # this is used for integration tests and CI which use a local kafka cluster
    password: "<SPARK_SECRET>"
```

### PodSecurityPolicies support

This part apply only for Kubernetes versions 1.24-.

Check PSP is enabled, but not for  `spark` serviceaccount:
```shell
kubectl get psp
Warning: policy/v1beta1 PodSecurityPolicy is deprecated in v1.21+, unavailable in v1.25+
NAME                PRIV   CAPS   SELINUX    RUNASUSER   FSGROUP    SUPGROUP   READONLYROOTFS   VOLUMES
magnum.privileged   true   *      RunAsAny   RunAsAny    RunAsAny   RunAsAny   false            *
kubectl --as=system:serviceaccount:spark:spark -n spark  auth can-i use podsecuritypolicy/magnum.privileged
Warning: resource 'podsecuritypolicies' is not namespace scoped in group 'policy'
no
```

Grant PSP usage to `spark` serviceaccount:

```shell
kubectl create ns spark
kubectl create sa -n spark spark
kubectl create -n spark role psp:magnum.privileged --verb=use --resource=podsecuritypolicy --resource-name=magnum.privileged
kubectl create -n spark rolebinding -n spark psp:magnum.privileged --role=psp:magnum.privileged --serviceaccount=spark:spark
```

Check PSP is enabled for  `spark` serviceaccount:

```shell
[fedora@fink-int fink-broker]$ kubectl --as=system:serviceaccount:spark:spark -n spark  auth can-i use podsecuritypolicy/magnum.privileged
Warning: resource 'podsecuritypolicies' is not namespace scoped in group 'policy'

yes
```

### Run fink-broker

```shell
# -t parameter: fink will process alert for the current night
cd $HOME/src/astrolabsoftware/fink-broker
./e2e/fink-start.sh -f $PWD -t
```

### Install S3

```shell
sudo dnf install docker
sudo systemctl enable docker
sudo systemctl start docker
sudo usermod -a -G docker $USER
newgrp docker
docker run --rm -v ~/.aws:/root/.aws -e AWS_ACCESS_KEY_ID="<S3_USER_ID>" -e AWS_SECRET_ACCESS_KEY="<S3_SECRET>" -e S3_ENDPOINT_URL=http://ijc-rgw-cloud-3.ijclab.in2p3.fr:8081 peakcom/s5cmd ls s3://finkk8s
```

### Web UI access

```shell
kubectl port-forward -n spark $(kubectl get  pods --selector=spark-app-name=stream2raw-py,spark-role=driver -n spark --output=jsonpath="{.items..metadata.name}") 4040
curl http://localhost:4040
# Eventually set up a tunnel to workstation
ssh fedora@<bootstrap-server_ip> -L 4040:localhost:4040 -N
```

## Install kafka external services

This is useful if output kafka servers are not declared inside DNS.

```shell
kubectl apply -f ./deploy/fink_int/manifests/
```