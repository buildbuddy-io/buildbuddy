---
id: troubleshooting-rbe
title: Troubleshooting RBE Failures
sidebar_label: RBE Failures
---

## Remote connection/protocol failed with: execution failed

This error is often a sign that a cache write is timing out. By default, bazel's `remote_timeout` [flag](https://docs.bazel.build/versions/master/command-line-reference.html#flag--remote_timeout) limits all remote execution calls to 60 seconds.

We recommend using the following flag to increase this remote timeout:

```
--remote_timeout=600
```

These expensive writes should only happen once when artifacts are initially written to the cache, and shouldn't happen on subsequent builds.

## Remote connection/protocol failed with: execution failed DEADLINE_EXCEEDED: deadline exceeded after 59999899500ns

This error is a sign that a cache write is timing out. By default, bazel's `remote_timeout` [flag](https://docs.bazel.build/versions/master/command-line-reference.html#flag--remote_timeout) limits all remote execution calls to 60 seconds.

We recommend using the following flag to increase this remote timeout:

```
--remote_timeout=600
```

## exec user process caused "exec format error"

This error occurs when your build is configured for darwin (Mac OSX) CPUs, but attempting to run on Linux executors. Mac executors are not included in BuildBuddy Cloud's free-tier offering.

If you'd like to add Mac executors to your BuildBuddy Cloud account, please [contact our sales team](/request-demo/).

## rpc error: code = Unavailable desc = No registered executors.

This error occurs when your build is configured for darwin (Mac OSX) CPUs, but attempting to run on Linux executors. Mac executors are not included in BuildBuddy Cloud's free-tier offering.

If you'd like to add Mac executors to your BuildBuddy Cloud account, please [contact our sales team](/request-demo/).

## WARNING: Remote Cache: UNAVAILABLE: io exception

This error occurs when Bazel JVM have a hard time maintaining the TCP connection with BuildBuddy remote cache.

To validate this error, user could use Bazel's flag `--remote_grpc_log=grpc.log` to capture the grpc traffic
from Bazel JVM and BuildBuddy. The log file will be in protobuf format. to convert it to JSON format, user
could use our [BB CLI](/docs/cli) and run `bb print --grpc_log=<path-to-file>/grpc.log`. In side the log, we
expect to see network errors such as

```json
  "status":  {
    "code":  14,
    "message":  "io.netty.channel.unix.Errors$NativeIoException: readAddress(..) failed: Connection reset by peer"
  },
```

This is usually encountered by customers who run operate some proxy or gateway (i.e. AWS NAT Gateway) in between
Bazel JVM and BuildBuddy server.

When this happen, user should consider tunning their Linux network settings like this:

```bash
sudo sysctl -w net.ipv4.tcp_keepalive_time=180
sudo sysctl -w net.ipv4.tcp_keepalive_intvl=60
sudo sysctl -w net.ipv4.tcp_keepalive_probes=25
```

so the keepalive routines in Linux kernel would send keepalive probe more frequently.

The exact values could be different, subjected to user's specific network condition.
Please [contact us](/contact/) if you have any questions / concerns.
