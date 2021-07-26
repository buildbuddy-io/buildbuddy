---
id: enterprise-rbe
title: Enterprise RBE Setup
sidebar_label: Enterprise RBE Setup
---

To deploy BuildBuddy Remote Build Execution on-prem, we recommend using the [BuildBuddy Enterprise Helm charts](https://github.com/buildbuddy-io/buildbuddy-helm/tree/master/charts/buildbuddy-enterprise).

## Installing the chart

First add the BuildBuddy Helm repository:

```
helm repo add buildbuddy https://helm.buildbuddy.io
```

Then you'll need to make sure kubectl is configured with access to your Kubernetes cluster. Here are instructions for [Google Cloud](https://cloud.google.com/kubernetes-engine/docs/how-to/cluster-access-for-kubectl), [AWS](https://docs.aws.amazon.com/eks/latest/userguide/create-kubeconfig.html), and [Azure](https://docs.microsoft.com/en-us/azure/aks/kubernetes-walkthrough#connect-to-the-cluster).

Finally install BuildBuddy enterprise to your Kubernetes cluster:

```
helm install buildbuddy buildbuddy/buildbuddy-enterprise
```

This will deploy a minimal BuildBuddy enterprise install to your Kubernetes cluster.

You can verify your install by waiting a minute or two for your deployment to complete, then running:

```
echo `kubectl get --namespace default service buildbuddy-enterprise -o jsonpath='{.status.loadBalancer.ingress[0].*}'`
```

This will return an IP address that you can visit in a browser and verify that you install was successful. The basic deployment doesn't configure RBE, so you'll only see options for BES and Cache endpoints.

## Configuring your install

Now that you have a basic BuildBuddy Enterprise install deployed. Let's configure it to enable Remote Build Execution.

You can do so this by passing a `values.yaml` file to Helm that enables RBE. Here's a simple `values.yaml` file with RBE enabled. This will deploy RBE with 3 executors and Redis enabled:

```
executor:
  enabled: true
  replicas: 3
redis:
  enabled: true
config:
  remote_execution:
    enable_remote_exec: true
```

This configuration with 1 app instance, 3 executors, and a Redis instance will fit on a machine/cluster with at least 5 vCPUs and 24 GB of RAM.

GCP's [n2-standard-8 machines](https://cloud.google.com/compute/docs/machine-types#n2_standard_machine_types) or similar are a good place to start. For information on scaling up and down your deployments, see the **Configuring resource** section.

You can now upgrade your install with the following command:

```
helm upgrade buildbuddy buildbuddy/buildbuddy-enterprise --values values.yaml
```

Once your upgrade has completed (and the rollout has finished), you can reload the IP address you obtained from the kubectl command above.

```
echo `kubectl get --namespace default service buildbuddy-enterprise -o jsonpath='{.status.loadBalancer.ingress[0].*}'`
```

You should now see a remote build execution checkbox and can try your first remote build execution. Make sure your RBE [toolchains and platforms](rbe-setup.md) are correctly configured.

## Configuring resources

Now that you've got a working RBE deployment, you can configure resources requested by BuildBuddy app instances and executors to scale up and down your cluster.

By default BuildBuddy app instances request 1 CPU and 4 GB of RAM, executors request 1 CPU and 5 GB of RAM per instance, and Redis requests 1 CPU and 5GB of RAM.

Here's a values.yaml file that specifies the replica and resource settings you can use to scale your cluster up and down:

```
replicas: 1
resources:
  limits:
    cpu: "2"
    memory: "8Gi"
  requests:
    cpu: "1"
    memory: "4Gi"
executor:
  enabled: true
  replicas: 3
  resources:
    limits:
      cpu: "2"
      memory: "10Gi"
    requests:
      cpu: "1"
      memory: "5Gi"
redis:
  enabled: true
  resources:
    limits:
      cpu: "1"
      memory: "6Gi"
    requests:
      cpu: "1"
      memory: "5Gi"
config:
  remote_execution:
    enable_remote_exec: true
```

Note: if you scale beyond one app replica, make sure you're using MySQL instead of SQLite, and GCS/S3 instead of disk storage. For information on how to configure these, see the [enterprise configuration guide](enterprise-config.md). Scaling the number of executor replicas does not have these requirements.

The default values support both memory hungry Java tests and CPU-intensive Tensorflow builds. Fine tuning these parameters for peak performance depends a lot on your workload, and we're happy to help.

For a full overview of what can be configured via our enterprise Helm charts, see the [buildbuddy-enterprise values.yaml file](https://github.com/buildbuddy-io/buildbuddy-helm/blob/master/charts/buildbuddy-enterprise/values.yaml), and the [buildbuddy-executor values.yaml file](https://github.com/buildbuddy-io/buildbuddy-helm/blob/master/charts/buildbuddy-executor/values.yaml). Values for the executor deployment are nested in the `executor:` block of the buildbuddy-enterprise yaml file.

## More configuration

For more configuration options beyond RBE, like authentication and storage options, see our [configuration docs](config.md) and our [enterprise configuration guide](enterprise-config.md).

## Writing deployment to a file

If you don't want to use Helm, you can write your Kubernetes deployment configuration to a file:

```
$ helm template buildbuddy buildbuddy/buildbuddy-enterprise > buildbuddy-deploy.yaml
```

You can then check this configuration in to your source repository, or manually apply it to your cluster with:

```
$ kubectl apply -f buildbuddy-deploy.yaml
```
