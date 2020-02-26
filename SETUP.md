<p align="center">
  <h1>Installing and Configuring Buildbuddy</h1>
</p>

# Intro
BuildBuddy is an open source Bazel build event viewer. The two primary ways to use BuildBuddy are to use the cloud instance, or to run it yourself on premise. We support both!

# Cloud BuildBuddy

Cloud BuildBuddy is run as a service in multiple datacenters around the world. To upload your own logs, you need to configure bazel to point to a BuildBuddy instance. The easiest way to do this is with a .bazelrc file in the root of your project.

You can view a [sample .bazelrc](https://app.buildbuddy.io/) here. For basic, unauthenticated uploads, create a .bazelrc file in your project root (or edit the existing one) and add the following lines:

* build --bes_results_url=https://app.buildbuddy.io/invocation/
* build --bes_backend=grpc://events.buildbuddy.io:1985

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

We've designed BuildBuddy to be easy to run on-premise for those use cases where data absolutely must not leave a company's servers. The software itself is open-core and easy to audit. We can help configure BuildBuddy to run in your custom environment -- just [ask!](mailto:support@tryflame.com?subject=[Enterprise]%20BuildBuddy%20Setup)

## Bazel Run

The simplest method of running BuildBuddy on your own computers is to download it and use bazel run. Doing that is as simple as:

### Get the source
```
git clone "git@github.com:tryflame/buildbuddy.git"
```

### Build and run it with bazel
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

Assuming you have kubectl setup and configured, we provide a shell script that will deploy the necessary services to your cluster, all namespaced under the "buildbuddy" namespace. This script uses [this deployment file](github.com/tryflame), if you want to see the details of what is being configured.
```
bash k8s_on_prem.sh
```

