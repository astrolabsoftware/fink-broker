kubectl get -n kafka secrets/fink-producer --template={{.data.password}} | base64 --decode
