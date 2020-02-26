<p align="center">
  <h1>Installing and Configuring Buildbuddy</h1>
</p>

# Intro
Setting up BuildBuddy to view your own logs is easy! The two primary ways are to use the cloud BuildBuddy instance, or to run it your own instance on-premise. We support both!

# Cloud BuildBuddy

[Cloud BuildBuddy](https://app.buildbuddy.io/) is run as a service in multiple datacenters around the world.

To upload your own logs, you just need to configure bazel to point to a BuildBuddy instance. The easiest way to do this is with a .bazelrc file in the root of your project.

You can view a [sample .bazelrc](https://app.buildbuddy.io/) here. For basic, unauthenticated uploads, create a .bazelrc file in your project root (or edit the existing one) and add the following lines:

```
build --bes_results_url=https://app.buildbuddy.io/invocation/
build --bes_backend=grpc://events.buildbuddy.io:1985
```

Now, when you run a build or test with bazel, it will print a url where you can view your build or test results. For example:

```
tylerw@lunchbox:~/tryflame/buildbuddy$ bazel build --config=prod server:all
INFO: Streaming build results to: https://app.buildbuddy.io/invocation/24a37b8f-4cf2-4909-9522-3cc91d2ebfc4
INFO: Analyzed 13 targets (0 packages loaded, 0 targets configured).
INFO: Found 13 targets...
INFO: Elapsed time: 0.212s, Critical Path: 0.01s
INFO: 0 processes.
INFO: Streaming build results to: https://app.buildbuddy.io/invocation/24a37b8f-4cf2-4909-9522-3cc91d2ebfc4
INFO: Build completed successfully, 1 total action
tylerw@lunchbox:~/tryflame/buildbuddy$
```

# On Premise

We've designed BuildBuddy to be easy to run on-premise for those use cases where data absolutely must not leave a company's servers. The software itself is open-core and easy to audit. We can also help you to run BuildBuddy in your own custom environment. [Tell us about your setup](mailto:support@tryflame.com?subject=[Enterprise]%20BuildBuddy%20Setup)

## Bazel Run

The simplest method of running BuildBuddy on your own computer is to download and run it with bazel run. Doing that is simple :

* ### Get the source
  ```
  git clone "git@github.com:tryflame/buildbuddy.git"
  ```

* ### Build and run with bazel
  ```
  cd buildbuddy && bazel run -c opt server:buildbuddy
  ```

## Docker Image

We publish a docker image with every release that contains a pre-configured BuildBuddy. To run it:

```
# Download the buildbuddy_docker.tar image from GitHub.
docker run buildbuddy_docker.tar
```

## Kubernetes

If you run or have access to a kubernetes cluster, and you have the "kubectl" command configured, we provide a shell script that will deploy BuildBuddy to your cluster, namespaced under the "buildbuddy" namespace. This script uses [this deployment file](github.com/tryflame), if you want to see the details of what is being configured.
```
bash k8s_on_prem.sh
```

