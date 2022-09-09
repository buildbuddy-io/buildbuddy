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
- `--monitoring_port` The port to listen for Prometheus metrics requests on. Defaults to 9090.

## Configuration options as flags

Additionally any [configuration option](config.md) can also be specified as a flag instead using dot notation.

For example the following configuration option:

```
database:
  data_source: "mysql://user:password@tcp(12.34.56.78)/buildbuddy_db"
```

Would be specified as a flag like so:

```
--database.data_source="mysql://user:password@tcp(12.34.56.78)/buildbuddy_db"
```
