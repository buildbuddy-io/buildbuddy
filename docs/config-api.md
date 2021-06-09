---
id: config-api
title: API Configuration
sidebar_label: API
---

The API is only configurable in the [Enterprise version](enterprise.md) of BuildBuddy. For more information, view the [API Documentation](enterprise-api.md).

## Section

`api:` The API section enables the BuildBuddy API over both gRPC(s) and REST HTTP(s). **Optional**

## Options

**Optional**

- `enable_api:` Whether or not to enable the BuildBuddy API.

- `api_key:` The default API key to use for on-prem enterprise deploys with a single organization/group.

## Example section

```
api:
  enable_api: true
```
