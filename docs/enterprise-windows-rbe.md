---
id: enterprise-windows-rbe
title: Enterprise Windows RBE Setup
sidebar_label: Enterprise Windows RBE Setup
---

Deploying Windows executors requires a little extra setup since the deployment process can't easily be automated via Kubernetes.

:::note
Windows RBE is currently in Beta. The Windows executor runs actions directly on the host using the `none` workload isolation type, and container images are not supported on Windows executors.
:::

## Deploying a BuildBuddy cluster

First you'll need to deploy the BuildBuddy app which serves the BuildBuddy UI, acts as a scheduler, and handles caching - which we still recommend deploying to a Linux Kubernetes cluster.

You can follow the standard [Enterprise RBE Setup](enterprise-rbe.md) instructions to get your cluster up and running.

## Windows environment setup

Once you have a BuildBuddy cluster deployed with RBE enabled, you can start setting up your Windows executors.

Run the commands in this guide from an Administrator PowerShell prompt.

### Installing build tools

Make sure your Windows executor has the tools that your remotely executed actions expect to find on the machine. For example, if you build C++ targets with MSVC, install the [Visual Studio Build Tools](https://visualstudio.microsoft.com/downloads/). If you build Java targets that rely on the system JDK, install a JDK.

For best results, install tools machine-wide and run the executor under an account that has those tools on its `PATH`.

### Optional: Enable long paths

Some Bazel builds create deeply nested output paths. To enable long paths on Windows, run:

```
New-ItemProperty -Path "HKLM:\SYSTEM\CurrentControlSet\Control\FileSystem" -Name "LongPathsEnabled" -Value 1 -PropertyType DWORD -Force
```

### Optional: Enable symlink creation

If your builds use symlinks, enable Developer Mode or grant the executor service account the "Create symbolic links" privilege. If your Bazel clients create symlinks on Windows, you may also need to pass `--windows_enable_symlinks`.

## Installing the BuildBuddy Windows executor

Now that the environment is configured, we can download and install the BuildBuddy Windows executor.

### Create directories

Create a directory to store the executor binary, logs, disk cache, and execution roots:

```
New-Item -ItemType Directory -Force C:\BuildBuddy,C:\BuildBuddy\logs,C:\BuildBuddy\remote_build,C:\BuildBuddy\filecache
```

We recommend avoiding the Windows temp directory since it is periodically cleaned up.

### Download the BuildBuddy executor

The BuildBuddy executor binary can be downloaded with PowerShell. Make sure to update the version number in the `$Version` variable to the [latest release](https://github.com/buildbuddy-io/buildbuddy/releases).

```text title="PowerShell"
$Version = "v2.263.0"
Invoke-WebRequest `
  -Uri "https://github.com/buildbuddy-io/buildbuddy/releases/download/$Version/executor-enterprise-windows-amd64-beta.exe" `
  -OutFile "C:\BuildBuddy\buildbuddy-executor.exe"
```

### Create config file

You'll need to create a `config.yaml` with the following contents:

```yaml title="C:\BuildBuddy\config.yaml"
executor:
  root_directory: 'C:\BuildBuddy\remote_build'
  app_target: 'grpcs://YOUR_BUILDBUDDY_CLUSTER_URL:443'
  local_cache_directory: 'C:\BuildBuddy\filecache'
  local_cache_size_bytes: 100000000000 # 100GB
  enable_bare_runner: true
  default_isolation_type: 'none'
```

Make sure to replace _YOUR_BUILDBUDDY_CLUSTER_URL_ with the grpc url of the BuildBuddy cluster you deployed. If you deployed the cluster without an NGINX Ingress, you'll need to update the protocol to `grpc://` and the port to `1985`.

If your BuildBuddy app requires executor authorization, or if you're connecting a self-hosted executor to BuildBuddy Cloud, add an executor API key to the `executor` section:

```yaml title="C:\BuildBuddy\config.yaml"
executor:
  api_key: 'YOUR_EXECUTOR_API_KEY'
```

Executor API keys can be created on the [organization settings page](https://app.buildbuddy.io/settings/org/api-keys) by selecting **Executor key (for self-hosted executors)**.

### Create a startup script

Create a `run-executor.ps1` script that starts the executor and writes logs to disk:

```text title="C:\BuildBuddy\run-executor.ps1"
Set-Location C:\BuildBuddy
$env:MY_HOSTNAME = $env:COMPUTERNAME
$env:MY_POOL = ""

.\buildbuddy-executor.exe --config_file=C:\BuildBuddy\config.yaml *> C:\BuildBuddy\logs\executor.log
exit $LASTEXITCODE
```

If these executors should register into a specific [executor pool](rbe-pools.md), set `$env:MY_POOL` to that pool name.

### Create a Scheduled Task

Now that everything is in place, create a Scheduled Task that starts the executor on boot and restarts it if it exits:

```
$Action = New-ScheduledTaskAction `
  -Execute "PowerShell.exe" `
  -Argument "-NoProfile -ExecutionPolicy Bypass -File C:\BuildBuddy\run-executor.ps1" `
  -WorkingDirectory "C:\BuildBuddy"
$Trigger = New-ScheduledTaskTrigger -AtStartup
$Settings = New-ScheduledTaskSettingsSet `
  -RestartCount 999 `
  -RestartInterval (New-TimeSpan -Minutes 1) `
  -ExecutionTimeLimit ([TimeSpan]::Zero) `
  -AllowStartIfOnBatteries `
  -DontStopIfGoingOnBatteries

Register-ScheduledTask `
  -TaskName "BuildBuddyExecutor" `
  -Action $Action `
  -Trigger $Trigger `
  -Settings $Settings `
  -User "SYSTEM" `
  -RunLevel Highest
```

This example runs the task as `SYSTEM`. If your builds rely on tools, credentials, or environment variables installed for a specific user, register the task under that user instead.

### Start the executor

You can start the Scheduled Task with:

```
Start-ScheduledTask -TaskName "BuildBuddyExecutor"
```

### Verify installation

You can verify that your BuildBuddy Executor successfully connected to the cluster by live tailing the log file:

```
Get-Content C:\BuildBuddy\logs\executor.log -Wait
```

You can also check that the executor has started by checking that its `readyz` endpoint returns the string `OK`:

```
$Response = Invoke-WebRequest -UseBasicParsing -Uri "http://localhost:8080/readyz?server-type=prod-buildbuddy-executor"
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
  --remote_executor=grpcs://YOUR_BUILDBUDDY_CLUSTER_URL \
  --extra_execution_platforms=//:windows_x86_64 \
  --platforms=//:windows_x86_64 \
  //...
```

You can also set the same properties with `--remote_default_exec_properties` if you don't use a platform target:

```bash
bazel test \
  --remote_executor=grpcs://YOUR_BUILDBUDDY_CLUSTER_URL \
  --remote_default_exec_properties=OSFamily=windows \
  --remote_default_exec_properties=Arch=amd64 \
  --remote_default_exec_properties=container-image=none \
  --remote_default_exec_properties=workload-isolation-type=none \
  --remote_default_exec_properties=use-self-hosted-executors=true \
  //...
```

## Updating

When updating your BuildBuddy Executors, you should restart one executor at a time, waiting for the previous executor to successfully start up before restarting the next. This will ensure that work in flight is successfully rescheduled to another executor.

Stop the Scheduled Task:

```
Stop-ScheduledTask -TaskName "BuildBuddyExecutor"
```

Download the new executor binary to `C:\BuildBuddy\buildbuddy-executor.exe`, then start the task again:

```
Start-ScheduledTask -TaskName "BuildBuddyExecutor"
```

You can check that an executor has successfully started by checking the `readyz` endpoint:

```
$Response = Invoke-WebRequest -UseBasicParsing -Uri "http://localhost:8080/readyz?server-type=prod-buildbuddy-executor"
if ($Response.Content -eq "OK") {
  Write-Output "Executor is ready"
}
```
