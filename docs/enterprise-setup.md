---
id: enterprise-setup
title: Enterprise Setup
sidebar_label: Enterprise On-prem Setup
---

There are three ways to run BuildBuddy Enterprise On-prem:

- [Helm](#helm): deploy BuildBuddy to your Kubernetes cluster with the official BuildBuddy helm charts.
- [Docker Image](#docker-image): pre-built Docker images running the latest version of BuildBuddy.
- [Kubernetes](#kubernetes): deploy BuildBuddy to your Kubernetes cluster with a one-line deploy script.

We recommend using Helm as it includes all of the bells and whistles like nginx, remote build executors, etc. If you're not a fan of using Helm for deployment - we recommend using Helm to generate your Kubernetes deployment yaml file with `helm template`, and then running `kubectl apply` with that file.

For more instructions on deploying RBE, see our [enterprise on-prem RBE docs](enterprise-rbe.md).

## Helm

If you run or have access to a Kubernetes cluster and are comfortable with [Helm](https://helm.sh/), we maintain official BuildBuddy Helm charts that are easy to configure and deploy.

They have options to deploy everything necessary to use all of BuildBuddy's bells and whistles - including MySQL, nginx, and more.

The official BuildBuddy charts live in our [buildbuddy-helm repo](https://github.com/buildbuddy-io/buildbuddy-helm) and can be added to helm with the following command:

```
helm repo add buildbuddy https://helm.buildbuddy.io
```

You can the deploy BuildBuddy Enterprise with the following command:

```
helm install buildbuddy buildbuddy/buildbuddy-enterprise \
  --set mysql.mysqlUser=sampleUser \
  --set mysql.mysqlPassword=samplePassword
```

For more information on configuring your BuildBuddy Helm deploy, check out the chart itself:

- [BuildBuddy Enterprise](https://github.com/buildbuddy-io/buildbuddy-helm/tree/master/charts/buildbuddy-enterprise)

## Docker Image

We publish a [Docker](https://www.docker.com/) image with every release that contains a pre-configured BuildBuddy Enterprise.

To run it, use the following command:

```
docker pull gcr.io/flame-public/buildbuddy-app-enterprise:latest && docker run -p 1985:1985 -p 8080:8080 gcr.io/flame-public/buildbuddy-app-enterprise:latest
```

If you'd like to pass a custom configuration file to BuildBuddy running in a Docker image - see the [configuration docs](config.md) on using Docker's [-v flag](https://docs.docker.com/storage/volumes/).

Note: If you're using BuildBuddy's Docker image locally and a third party gRPC cache, you'll likely need to add the `--network=host` [flag](https://docs.docker.com/network/host/) to your `docker run` command in order for BuildBuddy to be able to pull test logs and timing information from the external cache.

We also publish a docker image containing our RBE executor:

```
docker pull gcr.io/flame-public/buildbuddy-executor-enterprise:latest && docker run -p 1987:1987 gcr.io/flame-public/buildbuddy-executor-enterprise:latest
```

For configuration options, see [RBE config documentation](config-rbe.md).

## Kubernetes

If you run or have access to a Kubernetes cluster, and you have the "kubectl" command configured, we provide a shell script that will deploy BuildBuddy to your cluster, namespaced under the "buildbuddy" namespace.

This script uses [this deployment file](https://github.com/buildbuddy-io/buildbuddy/blob/master/deployment/buildbuddy-app.enterprise.yaml), if you want to see the details of what is being configured.

To kick of the Kubernetes deploy, use the following command:

```
bash k8s_on_prem.sh -enterprise
```

To make this easier, the `k8s_on_prem.sh` script can optionally push a config file to your cluster in a Kubernetes ConfigMap that contains the contents of a custom config file. To do this, just specify the -config flag with an argument that is the path to your custom configuration file. For example:

```
./k8s_on_prem.sh -enterprise -config foo/bar/buildbuddy.custom.yaml
```

For more details on using the `k8s_on_prem.sh` script, see the [Kubernetes section](on-prem.md#kubernetes) of the on-prem deployment documentation.

## Configuring BuildBuddy

For documentation on BuildBuddy enterprise configuration options, check out our [enterprise configuration documentation](enterprise-config.md).
