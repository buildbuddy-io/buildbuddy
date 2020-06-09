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
   confirmOrExit "You've selected the enterprise version. Please confirm you've agreed to the BuildBuddy Enterprise Terms: Y/n"
   deploymentFile=deployment/buildbuddy-app.enterprise.yaml
fi


if [ ! -z "$configMapFile" ]; then
    cp "$configMapFile" /tmp/config.yaml
    kubectl create namespace buildbuddy
    kubectl --namespace=buildbuddy create configmap config --from-file "config.yaml=/tmp/config.yaml" --dry-run=client -o yaml | kubectl apply -f -
fi

kubectl apply -f "$deploymentFile"
