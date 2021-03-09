---
id: on-prem
title: On-prem Quickstart
sidebar_label: On-prem Quickstart
---

BuildBuddy is designed to be easy to run on-premise for those use cases where data absolutely must not leave a company's servers. It can be run your own servers, or in your own cloud environment. It supports major cloud providers like GCP, AWS, and Azure.

The software itself is open-source and easy to audit.

For companies, we offer an [Enterprise](enterprise.md) version of BuildBuddy that contains advanced features like OIDC Auth, API access, and more.

## Getting started

There are four ways to run BuildBuddy on-prem:

- [Bazel Run](#bazel-run): get the source and run a simple `bazel run` command.
- [Docker Image](#docker-image): pre-built Docker images running the latest version of BuildBuddy.
- [Kubernetes](#kubernetes): deploy BuildBuddy to your Kubernetes cluster with a one-line deploy script.
- [Helm](#helm): deploy BuildBuddy to your Kubernetes cluster with the official BuildBuddy helm charts.

## Bazel Run

The simplest method of running BuildBuddy on your own computer is to download and run it with "bazel run". Doing that is simple:

1. Get the source

```bash
git clone "https://github.com/buildbuddy-io/buildbuddy"
```

2. Navigate into the BuildBuddy directory

```bash
cd buildbuddy
```

3. Build and run using bazel

```bash
bazel run -c opt server:buildbuddy
```

We recommend using a tool like [Bazelisk](https://github.com/bazelbuild/bazelisk) that respects the repo's [.bazelversion](https://github.com/buildbuddy-io/buildbuddy/blob/master/.bazelversion) file.

## Docker Image

We publish a [Docker](https://www.docker.com/) image with every release that contains a pre-configured BuildBuddy.

To run it, use the following command:

```bash
docker pull gcr.io/flame-public/buildbuddy-app-onprem:latest && docker run -p 1985:1985 -p 8080:8080 gcr.io/flame-public/buildbuddy-app-onprem:latest
```

If you'd like to pass a custom configuration file to BuildBuddy running in a Docker image - see the [configuration docs](config.md) on using Docker's [-v flag](https://docs.docker.com/storage/volumes/).

Note: If you're using BuildBuddy's Docker image locally and a third party gRPC cache, you'll likely need to add the `--network=host` [flag](https://docs.docker.com/network/host/) to your `docker run` command in order for BuildBuddy to be able to pull test logs and timing information from the external cache.

## Kubernetes

If you run or have access to a Kubernetes cluster, and you have the "kubectl" command configured, we provide a shell script that will deploy BuildBuddy to your cluster, namespaced under the "buildbuddy" namespace.

This script uses [this deployment file](https://github.com/buildbuddy-io/buildbuddy/blob/master/deployment/buildbuddy-app.onprem.yaml), if you want to see the details of what is being configured.

To kick of the Kubernetes deploy, use the following command:

```bash
bash k8s_on_prem.sh
```

### Custom configuration

Note: the `k8s_on_prem.sh` script requires **[kubectl](https://kubernetes.io/docs/tasks/tools/install-kubectl/) version 1.15** or higher to be installed.

To pass in a custom [config file](config.md), you can use the `-config` flag:

```
bash k8s_on_prem.sh -config my-config.yaml
```

### Output to yaml file

By default the `k8s_on_prem.sh` script will use `kubectl apply` to deploy BuildBuddy to your current Kubernetes cluster. If you'd like to output the Kubernetes deployment to a yaml file instead that can be checked in, you can use the `-out` flag:

```
bash k8s_on_prem.sh -out my-buildbuddy-deployment.yaml
```

### Number of replicas

By default the `k8s_on_prem.sh` script will deploy a single replica of BuildBuddy. If you've configured a MySQL database, storage, and other options necessary to support multiple replicas, you can increase the number of BuildBuddy replicas to deploy with the `-replicas` flag.

```
bash k8s_on_prem.sh -replicas 3
```

### Restart behavior

By default the `k8s_on_prem.sh` will restart your BuildBuddy deployment to pick up any changes in your configuration file. This can lead to brief downtime if only one replica is deployed. You can disable this behavior with the `-norestart` flag.

```
bash k8s_on_prem.sh -norestart
```

### Enterprise deployment

If you've obtained a BuildBuddy enterprise license, you deploy enterprise BuildBuddy by specifying the `-enterprise` flag.

```
bash k8s_on_prem.sh -enterprise
```

## Helm

If you run or have access to a Kubernetes cluster and are comfortable with [Helm](https://helm.sh/), we maintain official BuildBuddy Helm charts that are easy to configure and deploy.

They have options to deploy everything necessary to use all of BuildBuddy's bells and whistles - including MySQL, nginx, and more.

The official BuildBuddy charts live in our [buildbuddy-helm repo](https://github.com/buildbuddy-io/buildbuddy-helm) and can be added to helm with the following command:

```
helm repo add buildbuddy https://helm.buildbuddy.io
```

You can the deploy BuildBuddy Open Source with the following command:

```
helm install buildbuddy buildbuddy/buildbuddy \
  --set mysql.mysqlUser=sampleUser \
  --set mysql.mysqlPassword=samplePassword
```

For more information on configuring your BuildBuddy Helm deploy, check out the charts themselves:

- [BuildBuddy Open Source](https://github.com/buildbuddy-io/buildbuddy-helm/tree/master/charts/buildbuddy)
- [BuildBuddy Enterprise](https://github.com/buildbuddy-io/buildbuddy-helm/tree/master/charts/buildbuddy-enterprise)

## Configuring BuildBuddy

For documentation on all BuildBuddy configuration options, check out our [configuration documentation](config.md).
