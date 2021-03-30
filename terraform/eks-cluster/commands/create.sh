#!/bin/bash
set -e

# Usage
# ./command/create.sh [--all|--cluster|--app|--run] [--values /path/to/custom-values.yaml] [--helm /path/to/local/helm/chart]
#
# Examples:
#
# Create cluster, app and run bazel
# ./command/create.sh --all
#
# Create just the cluster
# ./command/create.sh --cluster
#
# Create the app and run bazel
# ./command/create.sh --app --run
#
# Create cluster, app with custom values.yaml and local helm chart, and run bazel
# ./commands/create.sh --all --values ../buildbuddy-enterprise/values/demo.yaml --helm ~/Code/buildbuddy-helm/charts/buildbuddy-enterprise/

# Execute from the /terraform/eks-cluster directory.
WORKING_DIRECTORY="$(cd $(dirname "$0");pwd)/.."
cd $WORKING_DIRECTORY

# Defaults
VALUES_PATH=../buildbuddy-enterprise/values/rbe-minimal.yaml
HELM_CHART=buildbuddy/buildbuddy-enterprise

# Check if there are arguments
if [[ $# -eq 0 ]]; then
    echo "Missing arguments, try --all, --cluster, --app, --run, or some combination of those"
fi

# Parse arguments
while [[ "$#" -gt 0 ]]; do
    case $1 in
        -v|--values) VALUES_PATH="$2"; shift ;; # -v or --values, optional - path to a custom helm values.yaml file
        -h|--helm) HELM_CHART="$2"; shift ;; # -h or --helm, optional - a custom helm chart to use, can be a directory
        -c|--cluster) CREATE_CLUSTER=1 ;; # -c or --cluster, optional - create an eks kubernetes cluster
        -a|--app) CREATE_APP=1 ;; # -a or --app, optional - deploy the buildbuddy app
        -r|--run) SHOULD_RUN=1 ;; # -r or --run, optional - run a test bazel build against the cluster
        --all) ALL=1 ;; # -all, optional - create cluster, deploy app, and run test bazel build
        *) echo "Unknown parameter passed: $1"; exit 1 ;;
    esac
    shift
done

# Check if we should create a cluster
if [[ $CREATE_CLUSTER == 1 ]] || [[ $ALL == 1 ]]; then

  # Print commands as we run them
  set -x

  # Create the cluster
  terraform init
  terraform apply -auto-approve

  # Update kubectl
  aws eks --region $(terraform output -raw region) update-kubeconfig --name $(terraform output -raw cluster_name)

  # Deploy the CRDs for cert manager
  kubectl apply --validate=false -f https://github.com/jetstack/cert-manager/releases/download/v0.16.1/cert-manager.crds.yaml

  # Stop printing
  set +x 

fi

# Check if we should create an app
if [[ $CREATE_APP == 1 ]] || [[ $ALL == 1 ]]; then

  echo "=============================================================="
  echo "Deploying BuildBuddy - you can monitor the progress with:"
  echo "kubectl -n default get events --sort-by='{.lastTimestamp}' -w"
  echo "=============================================================="

  # Print commands as we run them
  set -x

  # Add the buildbuddy helm repo
  helm repo add buildbuddy https://helm.buildbuddy.io

  # Install helm chart
  helm upgrade --install buildbuddy-enterprise --atomic --timeout 10m0s -f $VALUES_PATH $HELM_CHART

  # Stop printing
  set +x

fi

# Check if we should should run a bazel build
if [[ $SHOULD_RUN == 1 ]] || [[ $ALL == 1 ]]; then

  # Run a bazel build
  $WORKING_DIRECTORY/commands/run.sh

fi
