#!/bin/bash

# Usage ./commands/destroy.sh [--all|--cluster|--app]

# Execute from the /terraform/eks-cluster directory.
WORKING_DIRECTORY="$(cd $(dirname "$0");pwd)/.."
cd $WORKING_DIRECTORY

# Parse arguments
while [[ "$#" -gt 0 ]]; do
    case $1 in
        -c|--cluster) DESTROY_CLUSTER=1 ;; # -c or --cluster, optional - destroy the entire kubernetes cluster
        -a|--app) DESTROY_APP=1 ;; # -a or --app, optional - uninstall the buildbuddy app and delete persistent volume claims
        --all) ALL=1 ;; # all, optional - delete both the app and the cluster
    esac
    shift
done

# Check if we should destroy the app
if [[ $DESTROY_APP == 1 ]] || [[ $ALL ]]; then

  # Print commands as we run them
  set -x

  # Uninstall helm charts
  helm uninstall buildbuddy-enterprise

  # Delete all persistent volume claims
  kubectl delete pvc --all

  # Delete all persistent volumes
  kubectl delete pv --all

  # Stop printing
  set +x

fi

# Check if we should destroy the app
if [[ $DESTROY_CLUSTER ]] || [[ $ALL ]]; then

  # Print commands as we run them
  set -x

  # Destroy the cluster
  terraform destroy -auto-approve

  # Stop printing
  set +x

fi
