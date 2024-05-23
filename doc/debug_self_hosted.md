# Debug self-hosted runner

```shell
# Connect to k8s cluster
export KUBECONFIG="$HOME/.kube/self-hosted-ci_v4"

# Open a shell on the runner, the runner name is available on top of github action log for the action you want to debug
kubectl exec -it -n arc-runners arc-runners-25rrh-runner-kb87p bash

# Set environment in the pod
export CIUXCONFIG=/tmp/ciux.sh
# SUFFIX is equal to empty or noscience
export SUFFIX=""
export PATH=/home/runner/go/bin/:$PATH
sudo apt-get update -y
sudo apt-get install -y bash-completion
echo 'source <(kubectl completion bash)' >>~/.bashrc
export SRC=/home/runner/_work/fink-broker/fink-broker

# Access source code:
ls /home/runner/_work/fink-broker/fink-broker

# Access fink pod
kubectl get pods
```
