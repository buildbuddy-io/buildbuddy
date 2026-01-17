# buildbuddy-io/plugins:upload_run_logs

The `upload_run_logs` plugin streams logs from the `bazel run` executable to our server,
so that run logs can be viewed in our UI (in addition to build logs, which are automatically
streamed on the build event stream by Bazel).

## Installation

Install this plugin with:

```
bb install buildbuddy-io/plugins:upload_run_logs
```
