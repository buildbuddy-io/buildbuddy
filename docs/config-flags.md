<!--
{
  "name": "Flags",
  "category": "5f84be4816a46711e64ca065",
  "priority": 100
}
-->

# BuildBuddy Flags

There are several configuration options that are not in the BuildBuddy configuration file. These are:

- `--config_file` The path to a config file from which to read configuration options.
- `--listen` The interface that BuildBuddy will listen on. Defaults to 0.0.0.0 (all interfaces)
- `--port` The port to listen for HTTP traffic on. Defaults to 8080.
- `--grpc_port` The port to listen for gRPC traffic on. Defaults to 1985.
- `--out_file` The path to a file to write BuildBuddy output to.

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