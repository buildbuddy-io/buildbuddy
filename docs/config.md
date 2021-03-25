---
id: config
title: Configuring BuildBuddy
sidebar_label: Overview
---

[BuildBuddy on-prem](on-prem.md) is configured using a [yaml](https://en.wikipedia.org/wiki/YAML) formatted configuration file.

## Command line flag

On startup, BuildBuddy reads this config file which is specified using the `--config_file` flag. The config file is periodically re-read, although some options like enabling or disabling a cache require a restart to take effect.

## Docker

If you're running BuildBuddy in a Docker image - you can use Docker's [-v flag](https://docs.docker.com/storage/volumes/) to map a custom local config file to `/config.yaml` in the Docker image.

Be sure to replace `PATH_TO_YOUR_LOCAL_CONFIG ` with the path to your custom config file:

```
docker pull gcr.io/flame-public/buildbuddy-app-onprem:latest && docker run -p 1985:1985 -p 8080:8080 -v /PATH_TO_YOUR_LOCAL_CONFIG/config.yaml:/config.yaml gcr.io/flame-public/buildbuddy-app-onprem:latest
```

Note: If you're using BuildBuddy's Docker image locally and a third party gRPC cache, you'll likely need to add the `--network=host` [flag](https://docs.docker.com/network/host/) to your `docker run` command in order for BuildBuddy to be able to pull test logs and timing information from the external cache.

## Option types

There are two types of config options: _Required_, and _Optional_.

- **Required** - BuildBuddy will not run without these.
- **Optional** - They configure optional functionality. BuildBuddy will happily run without them.

## Sample configuration files

We maintain a list of [sample configuration files](config-samples.md) that you can copy and paste to get up and running quickly.

- [Running locally (disk only)](config-samples.md#running-locally-disk-only)
- [Running with MySQL and in-memory cache](config-samples.md#running-with-mysql-and-in-memory-cache)

## Configuration options

Here's a full list of BuildBuddy's configuration sections:

**Required**

- [App](config-app.md) - basic app-level configuration options.
- [Storage](config-storage.md) - options that determine where BuildBuddy stores build results.
- [Database](config-database.md) - options that determine where BuildBuddy stores build metadata.

**Optional**

- [Cache](config-cache.md) - configuration options for BuildBuddy's built-in Remote Build Cache.
- [Integrations](config-integrations.md) - configure integrations with other services.
- [SSL](config-ssl.md) - configure SSL/TLS certificates and setup.
- [Github](config-github.md) - configure your Github integration.
- [Misc](config-misc.md) - miscellaneous configuration options.

**Enterprise only**

- [Auth](config-auth.md) - configure authentication providers.
- [API](config-api.md) - configure BuildBuddy API.
- [Org](config-org.md) - configure BuildBuddy Organization.

## Flags

In addition to the config file, some BuildBuddy options (like port number) can only be configured via command line flags.

More information on these flags, see our [flags documentation](config-flags.md).

## Environment variables

Environment variables in the config file are expanded at runtime.
You only need to reference your environment variables like this `${ENV_VARIABLE}`.
