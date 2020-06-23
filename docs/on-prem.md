<!--
{
  "name": "On-prem",
  "category": "5eeba6a6c5230e48eea60f18",
  "priority": 800
}
-->

# BuildBuddy On-prem

BuildBuddy is designed to be easy to run on-premise for those use cases where data absolutely must not leave a company's servers. It can be run your own servers, or in your own cloud environment. It supports major cloud providers like GCP, AWS, and Azure.

The software itself is open-source and easy to audit.

For companies, we offer an [Enterprise](enterprise.md) version of BuildBuddy that contains advanced features like OIDC Auth, API access, and more.

## Getting started

There are three ways to run BuildBuddy on-prem:

- [Bazel Run](#bazel-run): get the source and run a simple `bazel run` command.
- [Docker Image](#docker-image): pre-built Docker images running the latest version of BuildBuddy.
- [Kubernetes](#kubernetes): deploy BuildBuddy to your Kubernetes cluster with a one-line deploy script.

## Bazel Run

The simplest method of running BuildBuddy on your own computer is to download and run it with "bazel run". Doing that is simple:

1. Get the source
 
  ```
  git clone "https://github.com/buildbuddy-io/buildbuddy"
  ```

2. Navigate into the BuildBuddy directory

  ```
  cd buildbuddy
  ```

3. Build and run using bazel

  ```
  bazel run -c opt server:buildbuddy
  ```
We recommend using a tool like [Bazelisk](https://github.com/bazelbuild/bazelisk) that respects the repo's [.bazelversion](https://github.com/buildbuddy-io/buildbuddy/blob/master/.bazelversion) file.

## Docker Image

We publish a [Docker](https://www.docker.com/) image with every release that contains a pre-configured BuildBuddy. 

To run it, use the following command:

```
docker pull gcr.io/flame-public/buildbuddy-app-onprem:latest && docker run -p 1985:1985 -p 8080:8080 gcr.io/flame-public/buildbuddy-app-onprem:latest
```

If you'd like to pass a custom configuration file to BuildBuddy running in a Docker image - see the [configuration docs](configuration.md) on using Docker's [-v flag](https://docs.docker.com/storage/volumes/).

## Kubernetes

If you run or have access to a Kubernetes cluster, and you have the "kubectl" command configured, we provide a shell script that will deploy BuildBuddy to your cluster, namespaced under the "buildbuddy" namespace.

This script uses [this deployment file](https://github.com/buildbuddy-io/buildbuddy/blob/master/deployment/buildbuddy-app.onprem.yaml), if you want to see the details of what is being configured.

To kick of the Kubernetes deploy, use the following command:
```
bash k8s_on_prem.sh
```

## Configuring BuildBuddy

For documentation on all BuildBuddy configuration options, check out our [configuration documentation](configurationG\.md).
