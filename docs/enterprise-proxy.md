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

### Deployment topology and storage

The Helm chart runs Cache Proxy replicas as a distributed cache. Consistent hashing assigns each cached artifact to an owning proxy pod. With the default replication factor of one, that pod stores the artifact's local copy and other proxies route reads and writes to it. The upstream BuildBuddy Remote Cache remains the authoritative copy.

Use local SSD-backed storage for Cache Proxy data when possible. Proxy storage performance is generally less sensitive than executor scratch storage, but disk latency and throughput can still become bottlenecks under heavy cache traffic. Prefer fewer, larger proxies to reduce coordination and per-pod overhead, while retaining enough replicas for maintenance and failure tolerance.

### Initial sizing

As a starting point, size the proxy deployment relative to the executor capacity in the same cluster:

| Resource | Executor-to-proxy ratio |
| -------- | ----------------------- |
| CPU      | 20:1 to 30:1            |
| Memory   | 4:1 to 5:1              |

For example, a cluster with 1,000 executor CPUs and 2 TB (approximately 1.82 TiB) of executor memory should start with approximately 33 to 50 proxy CPUs and 400 to 500 GB (approximately 373 to 466 GiB) of proxy memory, divided across the proxy replicas.

One possible starting configuration for that example is:

```yaml title="values.yaml"
replicas: 3
resources:
  limits:
    cpu: "16"
    memory: "140Gi"
  requests:
    cpu: "16"
    memory: "140Gi"
```

This allocates 48 CPUs and 420 GiB of memory across three Cache Proxy replicas.

The example sets requests equal to limits so scheduler reservations match the sizing calculation. Clusters that intentionally overcommit CPU can lower the CPU requests separately.

These ratios are rules of thumb rather than fixed requirements. Monitor proxy CPU, memory, disk utilization, cache hit rate, request latency, evictions, and upstream traffic, then tune the replica count and per-pod resources for your cache traffic and enabled features.

### Scaling executors

When Cache Proxies and executors run in the same cluster, the executor fleet can autoscale independently while the proxy fleet retains its local cache. Newly started executors can read cached artifacts over the cluster network instead of fetching those artifacts across clusters during scale-up.

## Usage

Now you can point Bazel hosts and remote executors in the same cluster at your cache proxy deployment, using its cluster IP. The relevant flags are `--remote_cache` for Bazel and `--executor.cache_target` for the BuildBuddy remote executor. If you need to access the proxy from outside of the cluster, you can create and use an ingress.

When Bazel uses a cache proxy as its `--remote_cache`, it also uses that `--remote_cache` target when writing `bytestream://` artifact URIs into the build event stream. Set `--remote_bytestream_uri_prefix` to the canonical BuildBuddy cache hostname so BuildBuddy can fetch build event artifacts such as Bazel timing profiles:

```bash title=".bazelrc"
build:buildbuddy_proxy --remote_cache=grpc://cache-proxy.example.com:1985
build:buildbuddy_proxy --remote_bytestream_uri_prefix=<org-slug>.buildbuddy.io
```

You can use your org-specific cache hostname, such as `<org-slug>.buildbuddy.io`, or `remote.buildbuddy.io`. Do not include a `grpc://`, `grpcs://`, or `bytestream://` scheme in `--remote_bytestream_uri_prefix`.
