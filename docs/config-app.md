---
id: config-app
title: App Configuration
sidebar_label: App
---

## Section

`app:` The app section contains app-level options. **Required**

## Options

**Required**

- `build_buddy_url` Configures the external URL where your BuildBuddy instance can be found. (Does not actually change the server address or port, see [flags docs](config-flags.md) for information on how to configure ports)

**Optional**

- `events_api_url` Overrides the default build event protocol gRPC address shown by BuildBuddy on the configuration screen. (Does not actually change the server address)

- `cache_api_url` Overrides the default remote cache protocol gRPC address shown by BuildBuddy on the configuration screen. (Does not actually change the server address)

- `remote_execution_api_url` Overrides the default remote execution protocol gRPC address shown by BuildBuddy on the configuration screen. (Does not actually change the server address)

- `default_to_dense_mode` Enables Dense UI mode by default.

## Example section

```
app:
  build_buddy_url: "http://buildbuddy.acme.corp"
```
