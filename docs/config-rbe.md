<!--
{
  "name": "RBE",
  "category": "5f84be4816a46711e64ca065",
  "priority": 300
}
-->

# RBE Configuration

Remote Build Execution is only configurable in the [Enterprise version](enterprise.md) of BuildBuddy.

RBE configuration must be enabled in your `config.yaml` file, but most configuration is done via [toolchains](rbe-setup.md), [platforms](rbe-platforms.md), or the [enterprise Helm chart](enterprise-helm).

## Section

`remote_execution:` The remote_execution section allows you to configure BuildBuddy's remote build execution. **Optional**

## Options

**Optional**

- `enabled:` True if remote execution should be enabled.

## Example section

```
remote_execution:
  enabled: true
```

## Executor config

BuildBuddy RBE executors take their own configuration file that is pulled from `/config.yaml` on the executor docker image. Using BuildBuddy's [Enterprise Helm chart](enterprise-helm.md) will take care of most of this configuration for you.

Here is a minimal example:

```
executor:
  app_target: "grpcs://your.buildbuddy.install:443"
  root_directory: "/buildbuddy/remotebuilds/"
  local_cache_directory: "/buildbuddy/filecache/"
  local_cache_size_bytes: 5000000000 # 5GB
```

And a fully loaded example:

```
executor:
  app_target: "grpcs://your.buildbuddy.install:443"
  root_directory: "/buildbuddy/remotebuilds/"
  local_cache_directory: "/buildbuddy/filecache/"
  local_cache_size_bytes: 5000000000 # 5GB
  docker_sock: /var/run/docker.sock
auth:
  enable_anonymous_usage: true
  oauth_providers:
    - issuer_url: "https://accounts.google.com"
      client_id: "myclient.apps.googleusercontent.com"
      client_secret: "mysecret"
cache:
  redis_target: "my-release-redis-master:6379"
  gcs:
    bucket: "buildbuddy_cache_bucket"
    project_id: "myprojectid"
    credentials_file: "mycredentials.json"
    ttl_days: 30
```
