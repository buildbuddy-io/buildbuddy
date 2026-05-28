---
id: enterprise-proxy
title: Enterprise Cache Proxy
sidebar_label: Enterprise Cache Proxy
---

The BuildBuddy Cache Proxy is a self-hosted gRPC cache server that sits in front of the BuildBuddy Remote Cache, reading and writing through to the BuildBuddy Remote Cache. Bazel clients and self-hosted Executors can communicate with a proxy running on (or near) their host machines to reduce latency and traffic to the backing cache. The proxy will serve these requests locally when possible, fetching missing artifacts from the BuildBuddy cache.

Self-hosted Cache Proxies can be useful when roundtrip latency to the BuildBuddy Remote Cache affects Bazel or remote execution performance, or when network bandwidth between clusters is a concern.

To deploy BuildBuddy Cache Proxies on-prem, we recommend using the [BuildBuddy Enterprise Cache Proxy Helm chart](https://github.com/buildbuddy-io/buildbuddy-helm/tree/master/charts/buildbuddy-enterprise-cache-proxy).

## Installing the chart

First add the BuildBuddy Helm repository:

```bash
helm repo add buildbuddy https://helm.buildbuddy.io
```

Then you'll need to make sure kubectl is configured with access to your Kubernetes cluster. Here are instructions for [Google Cloud](https://cloud.google.com/kubernetes-engine/docs/how-to/cluster-access-for-kubectl), [AWS](https://docs.aws.amazon.com/eks/latest/userguide/create-kubeconfig.html), and [Azure](https://docs.microsoft.com/en-us/azure/aks/kubernetes-walkthrough#connect-to-the-cluster).

Finally install BuildBuddy Enterprise Cache Proxy in your Kubernetes cluster:

```bash
helm install buildbuddy buildbuddy/buildbuddy-enterprise-cache-proxy
```

This will deploy a minimal BuildBuddy Enterprise Cache Proxy installation to your Kubernetes cluster.

You can verify your installation by waiting a minute or two for your deployment to complete, then running:

```bash
echo `kubectl get --namespace default service buildbuddy-enterprise-cache-proxy -o jsonpath='{.status.loadBalancer.ingress[0].*}'`
```

This will return an IP address that you can ping to verify that your installation was successful.

## Configuration

You may want to configure your deployment's size by editing the number of replicas and the CPU and memory allocation for your pods:

```yaml title="values.yaml"
replicas: 6
resources:
  limits:
    cpu: "8"
    memory: "32Gi"
  requests:
    cpu: "7"
    memory: "30Gi"
```

## Usage

Now you can point Bazel hosts and remote executors in the same cluster at your cache proxy deployment, using its cluster IP. The relevant flags are `--remote_cache` for Bazel and `--executor.cache_target` for the BuildBuddy remote executor. If you need to access the proxy from outside of the cluster, you can create and use an ingress.
