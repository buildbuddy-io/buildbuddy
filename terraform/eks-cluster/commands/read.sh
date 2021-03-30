#!/bin/bash
set -e

# Execute from the /terraform/eks-cluster directory.
cd "$(cd $(dirname "$0");pwd)/.."

export CLUSTER_NAME=$(terraform output -raw cluster_name)
export CLUSTER_REGION=$(terraform output -raw region)
export BUILDBUDDY_HOST=$(kubectl get --namespace default service buildbuddy-enterprise -o jsonpath='{.status.loadBalancer.ingress[0].*}')

echo "Cluster name: $CLUSTER_NAME"
echo "Cluster region: $CLUSTER_REGION"
echo "BuildBuddy host: $BUILDBUDDY_HOST"

echo "Want to see live cluster status? Try:"
echo "kubectl -n default get events --sort-by='{.lastTimestamp}' -w"
