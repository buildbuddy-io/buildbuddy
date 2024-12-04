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

This error occurs when Bazel is unable to find file(s) in the BuildBuddy Remote Cache that it expects to exist.

The first step to verify this issue would be to copy the hash of the missing blob.
Then navigate to the Invocation URL -> Cache -> "Cache requests" and paste the hash into the Filter input.
This will let you know if Bazel has tried to upload the blob to BuildBuddy Remote Cache or not.

If Bazel attempted to upload the blob and failed, there should be multiple retries attempted for the same blob.
The retry attempts can be configured with the `--remote_retries` (default 5) and `--remote_retry_max_delay` (default 5s) flags.
Additionally, `--experimental_collect_system_network_usage` (default true since Bazel 8) can be used to collect network usage data on Bazel's host machine.
This network data will be displayed as a graph in the "Timing" tab of the Invocation page.

If there was no attempt from Bazel to upload the missing blob, this is caused by a mismatch of expectation between Bazel's local state and the BuildBuddy Remote Cache.
In a previous invocation (usually with Build without the Bytes turned on), Bazel local state was taught to assume that the blob is already in the Remote Cache.
However, as time passed, the blob was evicted from BuildBuddy Remote Cache without Bazel's knowledge.

The best solution in this scenario is for Bazel to either re-upload the missing blob, or to re-execute the action that created the missing blob.
This is also known as "Action Rewinding" in Bazel terminology.
However due to the complexity of Bazel's code base, this feature is not yet fully implemented.

The existing solution includes 2 halves:

a. With `--experimental_remote_cache_lease_extension` and `--experimental_remote_cache_ttl` flags, Bazel will keep track of all the blobs involved in the latest invocation in a side-car thread.
This side-car will routinely "ping" BuildBuddy Remote Cache to let the server know that these blobs are still being used by Bazel.
Our remote cache server will update the last used timestamps of these blobs accordingly.

b. With `--experimental_remote_cache_eviction_retries` (default 5) flag, Bazel will detect this specific error code and attempt to reset the local states and re-try the build.
This will clear the local state kept by Bazel and re-analyze the repository to determine which blobs are missing and which actions need to be re-executed.

If neither of these flags work, try running `bazel clean --noasync` to clear the local state manually.
Bazel JVM should be shut down by the time the clean finished. You can check your process monitor to verify this.
Then re-run the build with the same flags as before.

We also recommend disabling the local Disk Cache with `--disk_cache=''` while troubleshooting this type of issue as well as avoid using any remote cache proxy solutions.
It will help narrowing down the root cause by not having to deal with multiple sources of remote cache.
