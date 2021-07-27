---
id: enterprise-helm
title: Enterprise Helm Charts
sidebar_label: Enterprise Helm Charts
---

If you run or have access to a Kubernetes cluster and are comfortable with [Helm](https://helm.sh/), we maintain official BuildBuddy Helm charts that are easy to configure and deploy.

They have options to deploy everything necessary to use all of BuildBuddy's bells and whistles - including MySQL, nginx, remote build execution and more.

The official BuildBuddy charts live in our [buildbuddy-helm repo](https://github.com/buildbuddy-io/buildbuddy-helm).

## TL;DR

```
helm repo add buildbuddy https://helm.buildbuddy.io
helm install buildbuddy buildbuddy/buildbuddy-enterprise \
  --set mysql.mysqlUser=sampleUser \
  --set mysql.mysqlPassword=samplePassword
```

## Prerequisites

- Kubernetes 1.15+ with Beta APIs enabled
- Helm v2/v3
- Tiller (the Helm v2 server-side component) installed on the cluster

## Installing the repo

To install the BuildBuddy Helm repo:

```
helm repo add buildbuddy https://helm.buildbuddy.io
```

## Installing the Chart

To install the chart with the release name `my-release`:

```
$ helm install my-release buildbuddy/buildbuddy-enterprise
```

**Helm v2 command**

```
$ helm install --name my-release buildbuddy/buildbuddy-enterprise
```

The command deploys BuildBuddy on the Kubernetes cluster in the default configuration. The [configuration](#configuration)
section lists the parameters that can be configured during installation.

## Uninstalling the Chart

To uninstall/delete the `my-release` deployment:

```
$ helm delete my-release
```

The command removes all the Kubernetes components associated with the chart and deletes the release.

## Updating your release

If you change configuration, you can update your deployment:

```
$ helm upgrade my-release -f my-values.yaml buildbuddy/buildbuddy-enterprise
```

## Writing deployment to a file

You can write your Kubernetes deployment configuration to a file with release name `my-release`:

```
$ helm template my-release buildbuddy/buildbuddy-enterprise > buildbuddy-deploy.yaml
```

You can then check this configuration in to your source repository, or manually apply it to your cluster with:

```
$ kubectl apply -f buildbuddy-deploy.yaml
```

### Example configurations

Below are some examples of `.yaml` files with values that could be passed to the `helm`
command with the `-f` or `--values` flag to get started.

### Example MySQL configuration

```
mysql:
  enabled: true
  mysqlUser: "sampleUser"
  mysqlPassword: "samplePassword"
```

### Example external database configuration

```
mysql:
  enabled: false

config:
  database:
    ## mysql:     "mysql://<USERNAME>:<PASSWORD>@tcp(<HOST>:3306)/<DATABASE_NAME>"
    ## sqlite:    "sqlite3:///tmp/buildbuddy-enterprise.db"
    data_source: "" # Either set this or mysql.enabled, not both!
```

### Example ingress and certs configuration

Note: make sure to run `kubectl apply --validate=false -f https://github.com/jetstack/cert-manager/releases/download/v0.16.1/cert-manager.crds.yaml` to install CRDs before deploying this configuration.

```
ingress:
  enabled: true
  sslEnabled: true
  httpHost: buildbuddy.example.com
  grpcHost: buildbuddy-grpc.example.com

mysql:
  enabled: true
  mysqlUser: "sampleUser"
  mysqlPassword: "samplePassword"

certmanager:
  enabled: true
  emailAddress: your-email@gmail.com

config:
  app:
    build_buddy_url: "https://buildbuddy.example.com"
    events_api_url: "grpcs://buildbuddy-grpc.example.com"
    cache_api_url: "grpcs://buildbuddy-grpc.example.com"
  ssl:
    enable_ssl: true
```

## Example with auth (required for enterprise features)

Auth can be configured with any provider that supports OpenID Connect (OIDC) including Google GSuite, Okta, Auth0 and others.

```
ingress:
  enabled: true
  sslEnabled: true
  httpHost: buildbuddy.example.com
  grpcHost: buildbuddy-grpc.example.com

mysql:
  enabled: true
  mysqlUser: "sampleUser"
  mysqlPassword: "samplePassword"

certmanager:
  enabled: true
  emailAddress: your-email@gmail.com

config:
  app:
    build_buddy_url: "https://buildbuddy.example.com"
    events_api_url: "grpcs://buildbuddy-grpc.example.com"
    cache_api_url: "grpcs://buildbuddy-grpc.example.com"
  auth:
    ## To use Google auth, get client_id and client_secret here:
    ## https://console.developers.google.com/apis/credentials
    oauth_providers:
      - issuer_url: "https://accounts.google.com" # OpenID Connect Discovery URL
        client_id: "MY_CLIENT_ID"
        client_secret: "MY_CLIENT_SECRET"
  ssl:
    enable_ssl: true
```

## Example with remote build execution

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

## More examples

For more example `config:` blocks, see our [configuration docs](https://www.buildbuddy.io/docs/config#configuration-options).

### Local development

For local testing use [minikube](https://github.com/kubernetes/minikube)

Create local cluster using with specified Kubernetes version (e.g. `1.15.6`)

```
$ minikube start --kubernetes-version v1.15.6
```

Initialize helm

```
$ helm init
```

Above command is not required for Helm v3

Get dependencies

```
$ helm dependency update
```

Perform local installation

```
$ helm install . \
    --set image.tag=5.12.4 \
    --set mysql.mysqlUser=sampleUser \
    --set mysql.mysqlPassword=samplePassword
```

**Helm v3 command**

```
$ helm install . \
    --generate-name \
    --set image.tag=5.12.4 \
    --set mysql.mysqlUser=sampleUser \
    --set mysql.mysqlPassword=samplePassword
```

## Learn more

For more information on configuring your BuildBuddy Enterprise Helm deploy, check out the chart:

- [BuildBuddy Enterprise](https://github.com/buildbuddy-io/buildbuddy-helm/tree/master/charts/buildbuddy-enterprise)

For more information on configuring BuildBuddy, see our [Configuration docs](config.md). If you have questions please don’t hesitate to email us at [setup@buildbuddy.io](mailto:setup@buildbuddy.io) or ping us on our [Slack channel](https://slack.buildbuddy.io).
