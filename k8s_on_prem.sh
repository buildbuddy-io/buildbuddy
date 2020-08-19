#!/bin/bash
set -e

function die() {
    msg=$1
    echo "$msg"
    exit 1
}

# args: $1 == message
function confirmOrExit() {
    msg=$1
    if [ -z "$msg" ]; then
	msg="Are you sure?"
    fi

    read -p "$msg " -n 1 -r
    echo
    if [[ ! $REPLY =~ ^[Yy]$ ]]
    then
	exit 1
    fi
}

configMapFile="config/buildbuddy.release.yaml"  # Default config file.
useEnterprise="false"
useOutFile="false"
restart="true"
outFile=".buildbuddy.deploy.yaml"
replicas="1"
while test $# -gt 0; do
    case "$1" in
	-config)
	    shift
	    if test $# -gt 0; then
		configMapFile=$1
	    else
		die "no config file specified"
	    fi
	    shift
	    ;;
	-out)
	    shift
	    if test $# -gt 0; then
		outFile=$1
	    useOutFile="true"
	    else
		die "no out file specified"
	    fi
	    shift
	    ;;
	-replicas)
	    shift
	    if test $# -gt 0; then
		replicas=$1
	    else
		die "no replicas"
	    fi
	    shift
	    ;;
	-norestart)
	    restart="false"
	    shift
	    ;;
	-enterprise)
	    useEnterprise="true"
	    shift
	    ;;
	*)
	    die "$1 is not a recognized flag!"
	    ;;
    esac
done


deploymentFile="deployment/buildbuddy-app.onprem.yaml"
if [ "$useEnterprise" = "true" ]; then
    echo "You've selected the enterprise version of BuildBuddy."
    echo "Please confirm you've agreed to the BuildBuddy Enterprise Terms at: https://buildbuddy.io/enterprise-terms"
    confirmOrExit "Y/n:"
    deploymentFile=deployment/buildbuddy-app.enterprise.yaml
fi

[ -f $outFile ] && rm $outFile
touch $outFile

if [ ! -z "$configMapFile" ]; then
    kubectl create namespace buildbuddy -o yaml --dry-run=client >> $outFile
    echo "---" >> $outFile
    kubectl --namespace=buildbuddy create configmap config --save-config --from-file "config.yaml=$configMapFile" -o yaml --dry-run=client >> $outFile
    echo "---" >> $outFile
fi

cat $deploymentFile >> $outFile

if [ "$replicas" -gt "1" ]; then
    sed -i.bak "s/replicas: 1/replicas: $replicas/g" "$outFile"
    rm "$outFile.bak"
fi

if [ "$useOutFile" = "true" ]; then
    echo "Configuration written to yaml file: $outFile"
else
    kubectl apply -f "$outFile"
    rm $outFile
fi

if [ "$restart" = "true" ] && [ "$useOutFile" = "false" ]; then
    kubectl rollout restart statefulset buildbuddy-app -n buildbuddy
fi