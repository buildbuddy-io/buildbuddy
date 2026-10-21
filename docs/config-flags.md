---
id: config-flags
title: BuildBuddy Flags
sidebar_label: Flags
---

There are several configuration options that are not in the BuildBuddy configuration file. These are:

- `--config_file` The path to a config.yaml file from which to read configuration options.
- `--listen` The interface that BuildBuddy will listen on. Defaults to 0.0.0.0 (all interfaces)
- `--port` The port to listen for HTTP traffic on. Defaults to 8080.
- `--grpc_port` The port to listen for gRPC traffic on. Defaults to 1985.
- `--grpc_max_connection_age` The age at which external native gRPC and gRPCS connections to app, cache, and cache-proxy servers begin draining. Defaults to `10m`, with ±10% jitter. Set to `0s` to disable aging. New RPCs use replacement connections; existing RPCs may continue during the grace period. Does not apply to internal, auxiliary, or peer servers, or to gRPC served over the HTTP port.
- `--grpc_max_connection_age_grace` Additional time after aging begins before forcibly closing a connection, including active RPCs. Defaults to `0s`, which allows existing RPCs to finish without a time limit. A positive grace requires a positive connection age. Keep the default to avoid forcing long-running BES, execution, and transfer streams to reconnect.
- `--monitoring_port` The port to listen for Prometheus metrics requests on. Defaults to 9090.

## Configuration options as flags

Additionally any [configuration option](config.md) can also be specified as a flag instead using dot notation.

For example the following configuration option:

```yaml title="config.yaml"
database:
  data_source: "mysql://user:password@tcp(12.34.56.78)/buildbuddy_db"
```

Would be specified as a flag like so:

```bash
--database.data_source="mysql://user:password@tcp(12.34.56.78)/buildbuddy_db"
```
