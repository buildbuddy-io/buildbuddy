---
id: enterprise-windows-rbe
title: Enterprise Windows RBE Setup
sidebar_label: Enterprise Windows RBE Setup
---

Deploying Windows executors as bring-your-own runners requires a little extra setup since the executor runs directly on your Windows host.

:::note
Windows RBE is currently in Beta. The Windows executor runs actions directly on the host using the `none` workload isolation type. Container images are not yet supported on Windows executors. If you need Docker support for Windows RBE, please contact us.
:::

## Prerequisites

Before setting up a Windows executor, make sure your BuildBuddy organization is enabled for bring-your-own runners and create an executor API key for that organization.

## Windows environment setup

Run the commands in this guide from an Administrator PowerShell prompt.

### Installing build tools

Make sure your Windows executor has the tools that your remotely executed actions expect to find on the machine. For example, if you build C++ targets with MSVC, install the [Visual Studio Build Tools](https://visualstudio.microsoft.com/downloads/). If you build Java targets that rely on the system JDK, install a JDK.

For best results, install tools machine-wide and run the executor under an account that has those tools on its `PATH`.

### Recommended: Use ReFS or Dev Drive storage

We recommend using a [Windows Dev Drive](https://learn.microsoft.com/en-us/windows/dev-drive/) or another ReFS volume for the executor's disk cache and execution roots. Dev Drive uses ReFS, which supports block cloning / copy-on-write file copies. This can reduce disk I/O when the executor copies files between its local cache and action execution roots.

The examples below use `D:\bb` for the executor binary, service config, logs, disk cache, and execution roots. If possible, create `D:` as a Dev Drive or ReFS volume before installing the executor. Prefer using a non-system drive like `D:` over `C:`. If you use a different drive letter, keep the paths short and keep all executor paths on that same drive.

### Optional: Enable long paths

Some Bazel builds create deeply nested output paths. To enable long paths on Windows, run:

```text title="PowerShell"
New-ItemProperty `
  -Path "HKLM:\SYSTEM\CurrentControlSet\Control\FileSystem" `
  -Name "LongPathsEnabled" `
  -Value 1 `
  -PropertyType DWORD `
  -Force
```

### Optional: Enable symlink creation

If your builds use symlinks, enable Developer Mode or grant the executor service account the "Create symbolic links" privilege. If your Bazel clients create symlinks on Windows, you may also need to pass `--windows_enable_symlinks`.

## Installing the BuildBuddy Windows executor

Now that the environment is configured, we can download and install the BuildBuddy Windows executor.

### Create directories

Create directories to store the executor binary, logs, disk cache, and execution roots:

```text title="PowerShell"
$Dirs = @(
  "D:\bb"
  "D:\bb\logs"
  "D:\bb\remote_build"
  "D:\bb\filecache"
)
New-Item -ItemType Directory -Force -Path $Dirs
```

We recommend using short paths and avoiding the Windows temp directory since it is periodically cleaned up. Keep the executor binary, config, logs, `root_directory`, and `local_cache_directory` on the same drive. The cache and execution roots must be on the same drive so the executor can hard-link files from the cache into execution roots.

### Download the BuildBuddy executor

The BuildBuddy executor binary can be downloaded with PowerShell. Make sure to update the version number in the `$Version` variable to the [latest release](https://github.com/buildbuddy-io/buildbuddy/releases).

```text title="PowerShell"
$Version = "v2.263.0"
$BaseUrl = "https://github.com/buildbuddy-io/buildbuddy/releases/download"
$Asset = "executor-enterprise-windows-amd64-beta.exe"
$Url = "$BaseUrl/$Version/$Asset"
Invoke-WebRequest -Uri $Url -OutFile "D:\bb\buildbuddy-executor.exe"
```

### Download WinSW

The BuildBuddy executor should run as a long-running Windows service. Since the executor binary is a console application, we recommend using [WinSW](https://github.com/winsw/winsw) to run it under the Windows Service Control Manager.

This is separate from `buildbuddy-executor.exe`, which is the BuildBuddy executor binary. The `buildbuddy-executor-service.exe` file is the renamed WinSW service wrapper.

```text title="PowerShell"
$WinSWVersion = "v2.12.0"
$WinSWBaseUrl = "https://github.com/winsw/winsw/releases/download"
$WinSWUrl = "$WinSWBaseUrl/$WinSWVersion/WinSW-x64.exe"
Invoke-WebRequest -Uri $WinSWUrl -OutFile "D:\bb\buildbuddy-executor-service.exe"
```

### Create config file

You'll need to create a `config.yaml` with the following contents:

```yaml title="D:\bb\config.yaml"
executor:
  root_directory: 'D:\bb\remote_build'
  app_target: "grpcs://remote.buildbuddy.io"
  local_cache_directory: 'D:\bb\filecache'
  local_cache_size_bytes: 100000000000 # 100GB
  enable_bare_runner: true
  default_isolation_type: "none"
  api_key: "YOUR_EXECUTOR_API_KEY"
```

Set `api_key` to an executor API key for your BuildBuddy organization. Executor API keys can be created on the [organization settings page](https://app.buildbuddy.io/settings/org/api-keys) by selecting **Executor key (for self-hosted executors)**.

### Create a Windows service config

Create a `buildbuddy-executor-service.xml` file with the following contents:

```xml title="D:\bb\buildbuddy-executor-service.xml"
<service>
  <id>BuildBuddyExecutor</id>
  <name>BuildBuddy Executor</name>
  <description>BuildBuddy Windows RBE executor</description>
  <executable>D:\bb\buildbuddy-executor.exe</executable>
  <arguments>--config_file=D:\bb\config.yaml</arguments>
  <workingdirectory>D:\bb</workingdirectory>
  <startmode>Automatic</startmode>
  <onfailure action="restart" delay="60 sec" />
  <logpath>D:\bb\logs</logpath>
  <log mode="roll" />
  <env name="MY_HOSTNAME" value="%COMPUTERNAME%" />
  <env name="MY_POOL" value="" />
</service>
```

If these executors should register into a specific [executor pool](rbe-pools.md), set the `MY_POOL` environment variable to that pool name.

### Install the service

Now that everything is in place, run the WinSW wrapper's `install` command. WinSW reads the adjacent `buildbuddy-executor-service.xml` file and installs the service it describes:

```
D:\bb\buildbuddy-executor-service.exe install
```

This installs the service to run as `LocalSystem` by default. If your builds rely on tools, credentials, or environment variables installed for a specific user, configure the service to run under that user instead.

### Start the executor

You can start the service with:

```
Start-Service -Name "BuildBuddyExecutor"
```

### Verify installation

You can verify that your BuildBuddy Executor successfully connected to BuildBuddy by live tailing the log file:

```
Get-Content D:\bb\logs\*.log -Wait
```

You can also check that the executor has started by checking that its `readyz` endpoint returns the string `OK`:

```text title="PowerShell"
$ReadyzUrl = "http://localhost:8080/readyz"
$ReadyzUrl += "?server-type=prod-buildbuddy-executor"
$Response = Invoke-WebRequest `
  -UseBasicParsing `
  -Uri $ReadyzUrl
if ($Response.Content -eq "OK") {
  Write-Output "Executor is ready"
}
```

## Configuring Bazel for Windows actions

Windows actions must request Windows executors. You can do this with a Bazel execution platform:

```python title="BUILD"
platform(
    name = "windows_x86_64",
    constraint_values = [
        "@platforms//os:windows",
        "@platforms//cpu:x86_64",
    ],
    exec_properties = {
        "OSFamily": "windows",
        "Arch": "amd64",
        "container-image": "none",
        "workload-isolation-type": "none",
        "use-self-hosted-executors": "true",
    },
)
```

Then pass that platform to Bazel:

```bash
bazel test \
  --remote_executor=grpcs://remote.buildbuddy.io \
  --extra_execution_platforms=//:windows_x86_64 \
  --platforms=//:windows_x86_64 \
  //...
```

You can also set the same properties with `--remote_default_exec_properties` if you don't use a platform target:

```bash
bazel test \
  --remote_executor=grpcs://remote.buildbuddy.io \
  --remote_default_exec_properties=OSFamily=windows \
  --remote_default_exec_properties=Arch=amd64 \
  --remote_default_exec_properties=container-image=none \
  --remote_default_exec_properties=workload-isolation-type=none \
  --remote_default_exec_properties=use-self-hosted-executors=true \
  //...
```

## Updating

When updating your BuildBuddy Executors, you should restart one executor at a time, waiting for the previous executor to successfully start up before restarting the next. This will ensure that work in flight is successfully rescheduled to another executor.

Stop the service:

```
Stop-Service -Name "BuildBuddyExecutor"
```

Download the new executor binary to `D:\bb\buildbuddy-executor.exe`, then start the service again:

```
Start-Service -Name "BuildBuddyExecutor"
```

You can check that an executor has successfully started by checking the `readyz` endpoint:

```text title="PowerShell"
$ReadyzUrl = "http://localhost:8080/readyz"
$ReadyzUrl += "?server-type=prod-buildbuddy-executor"
$Response = Invoke-WebRequest `
  -UseBasicParsing `
  -Uri $ReadyzUrl
if ($Response.Content -eq "OK") {
  Write-Output "Executor is ready"
}
```
