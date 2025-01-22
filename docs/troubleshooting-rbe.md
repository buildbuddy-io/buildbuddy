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

## CacheNotFoundException: Missing digest

During remote build execution, Bazel may encounter a `CacheNotFoundException` error with the message `Missing digest`.

```bash
com.google.devtools.build.lib.remote.common.BulkTransferException: 3 errors during bulk transfer:
    com.google.devtools.build.lib.remote.common.CacheNotFoundException: Missing digest: d0387e622e30ab61e39b1b91e54ea50f9915789dde7b950fafb0863db4a32ef8/17096
    com.google.devtools.build.lib.remote.common.CacheNotFoundException: Missing digest: 9718647251c8d479142d459416079ff5cd9f45031a47aa346d8a6e719e374ffa/28630
    com.google.devtools.build.lib.remote.common.CacheNotFoundException: Missing digest: 785e0ead607a37bd9a12179051e6efe53d7fb3eb05cc291e49ad6965ee2b613d/11504
```

This error indicates that Bazel cannot find file(s) in the BuildBuddy Remote Cache that it expects to be present.

The first step in verifying this issue is to copy the hash of the missing blob.
Then, navigate to the Invocation URL -> Cache -> "Cache requests" and paste the hash into the Filter input.
This will show whether Bazel has attempted to upload the blob to the BuildBuddy Remote Cache.

If Bazel attempted to upload the blob but failed, you should see multiple retry attempts for the same blob.
The number of retry attempts and the delay between retries can be configured using the `--remote_retries` (default 5) and `--remote_retry_max_delay` (default 5s) flags.
Additionally, `--experimental_collect_system_network_usage` (default true since Bazel 8) can be used to collect network usage data on the Bazel host machine.
This network data will be displayed as a graph in the "Timing" tab of the Invocation page.

If there was no attempt by Bazel to upload the missing blob, this signifies a mismatch between Bazel's local state and the BuildBuddy Remote Cache.
In a previous invocation (often with Build without the Bytes enabled), Bazel's local state might have been updated to assume that the blob already exists in the Remote Cache.
However, over time, the blob may have been evicted from the BuildBuddy Remote Cache without Bazel's knowledge.

The ideal solution is for Bazel to either re-upload the missing blob or re-execute the action that created it.
This process is also known as "Action Rewinding" in Bazel.
However, this feature is not yet available as of Bazel 8.0

The current workaround involves two parts:

    a. Using the `--experimental_remote_cache_lease_extension` and `--experimental_remote_cache_ttl` flags, Bazel will maintain a record of all blobs involved in the latest invocation in a separate thread.
    This thread will periodically "ping" the BuildBuddy Remote Cache, informing the server that these blobs are still in use by Bazel.
    The remote cache server will then update the last-used timestamps of these blobs accordingly.

    b. With the `--experimental_remote_cache_eviction_retries` (default 5) flag, Bazel will detect this specific error code and attempt to reset the local state and retry the build.
    This will clear Bazel's local state and re-analyze the repository to determine which blobs are missing and which actions need to be re-executed.

If neither of these flags work, try running `bazel clean --noasync` to manually clear the local state.
Ensure that the Bazel JVM is shut down after the clean process completes (you can verify this through your process monitor).
Afterward, rerun the build with the same flags as before.

We also recommend disabling the local Disk Cache with `--disk_cache=''` when troubleshooting this issue, as well as avoiding any remote cache proxy solutions.
This will help isolate the root cause by eliminating potential interference from multiple cache sources.
