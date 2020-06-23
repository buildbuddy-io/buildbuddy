<!--
{
  "name": "API",
  "category": "5eed3e2ace045b343fc0a328",
  "priority": 200
}
-->

# API Configuration
The API is only configurable in the [Enterprise version](enterprise.md) of BuildBuddy.

## Section

```api:``` The Auth section enables the BuildBuddy API over both gRPC(s) and REST HTTP(s). **Optional**

## Options

**Optional**

* ```enable_api:``` Whether or not to enable the BuildBuddy API.

* ```api_key:``` The default API key to use for on-prem enterprise deploys with a single organization/group.

## Example section

```
api:
  enable_api: true
```