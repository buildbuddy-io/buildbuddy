---
id: troubleshooting-rbe
title: Troubleshooting RBE Failures
sidebar_label: RBE Failures
---

## Remote connection/protocol failed with: execution failed

This error is often a sign that a cache write is timing out. By default, bazel's `remote_timeout` [flag](https://docs.bazel.build/versions/master/command-line-reference.html#flag--remote_timeout) limits all remote execution calls to 60 seconds.

We recommend using the following flag to increase this remote timeout:

```bash
--remote_timeout=600
```

These expensive writes should only happen once when artifacts are initially written to the cache, and shouldn't happen on subsequent builds.

## Remote connection/protocol failed with: execution failed DEADLINE_EXCEEDED: deadline exceeded after 59999899500ns

This error is a sign that a cache write is timing out. By default, bazel's `remote_timeout` [flag](https://docs.bazel.build/versions/master/command-line-reference.html#flag--remote_timeout) limits all remote execution calls to 60 seconds.

We recommend using the following flag to increase this remote timeout:

```bash
--remote_timeout=600
```

## exec user process caused "exec format error"

This error occurs when your build is configured for darwin (Mac OSX) CPUs, but attempting to run on Linux executors. Mac executors are not included in BuildBuddy Cloud's free-tier offering.

If you'd like to add Mac executors to your BuildBuddy Cloud account, please [contact our sales team](/request-demo/).

## rpc error: code = Unavailable desc = No registered executors.

This error occurs when your build is configured for darwin (Mac OSX) CPUs, but attempting to run on Linux executors. Mac executors are not included in BuildBuddy Cloud's free-tier offering.

If you'd like to add Mac executors to your BuildBuddy Cloud account, please [contact our sales team](/request-demo/).

## WARNING: Remote Cache: UNAVAILABLE: io exception

This error may occur when Bazel fails to properly maintain a long-running TCP connection to BuildBuddy.

To check whether this is the case, try running Bazel with `--remote_grpc_log=grpc.log` to capture the gRPC traffic
between Bazel and BuildBuddy. The log file will be in protobuf format. To convert it to JSON format, download the [BuildBuddy CLI](/docs/cli) and run `bb print --grpc_log=<path-to-file>/grpc.log`.

In the log, you may see network errors such as the following:

```js
  "status":  {
    "code":  14,
    "message":  "io.netty.channel.unix.Errors$NativeIoException: readAddress(..) failed: Connection reset by peer"
  },
```

This typically happens when there is a proxy or gateway (e.g. AWS NAT Gateway) in between Bazel and BuildBuddy that is terminating idle connections too quickly.

When this happens, try the following Linux network settings:

```bash
# Lowered from default value: 7200
sudo sysctl -w net.ipv4.tcp_keepalive_time=180
# Lowered from default value: 75
sudo sysctl -w net.ipv4.tcp_keepalive_intvl=60
# Lowered from default value: 9
sudo sysctl -w net.ipv4.tcp_keepalive_probes=5
```

This will cause the Linux kernel to send keepalive probes earlier and more frequently, before the proxy/gateway in the middle detects and drops the idle connection.

The optimal values may depend on specific network conditions, but try these values as a starting point. Please [contact us](/contact/) if you have any questions / concerns.
