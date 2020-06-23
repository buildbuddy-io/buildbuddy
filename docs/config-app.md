<!--
{
  "name": "App",
  "category": "5eed3e2ace045b343fc0a328",
  "priority": 800
}
-->
# App Configuration


## Section

```app:``` The app section contains app-level options. **Required**

## Options

**Required**

* ```build_buddy_url```  Configures the external URL where your BuildBuddy instance can be found. (Does not actually change the server address or port, see [flags docs](flags.md) for information on how to configure ports)

**Optional**

* ```events_api_url```  Overrides the default build event protocol gRPC address shown by BuildBuddy on the configuration screen. (Does not actually change the server address)

* ```cache_api_url``` Overrides the default remote cache protocol gRPC address shown by BuildBuddy on the configuration screen. (Does not actually change the server address)

* ```default_to_dense_mode``` Enables Dense UI mode by default.

## Example section

```
app:
  build_buddy_url: "http://buildbuddy.acme.corp"
```
