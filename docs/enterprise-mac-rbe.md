---
id: enterprise-mac-rbe
title: Enterprise Mac RBE Setup
sidebar_label: Enterprise Mac RBE Setup
---

Deploying Mac executors requires a little extra love since the deployment process can't easily be automated via Kubernetes.

## Deploying a BuildBuddy cluster

First you'll need to deploy the BuildBuddy app which serves the BuildBuddy UI, acts as a scheduler, and handles caching - which we still recommend deploying to a Linux Kubernetes cluster.

You can follow the standard [Enterprise RBE Setup](enterprise-rbe.md) instructions to get your cluster up and running.

## Mac environment setup

Once you have a BuildBuddy cluster deployed with RBE enabled, you can start setting up your Mac executors.

### Downloading Xcode

When starting with a clean Mac, you'll first need to make sure Xcode is installed. You can download Xcode from [Apple's Developer Website](https://developer.apple.com/download/more/) (you'll need an Apple Developer account).

We recommend installing at least Xcode 12.2 (which is the default Xcode version used if no `--xcode_version` Bazel flag is specified).

:::note
If installing on many machines, we recommend downloading the Xcode .xip file to a location you control (like a cloud storage bucket) and downloading from there using a simple curl command. This reduces the number of times you have to login to your Apple Developer account.
:::

### Installing Xcode

Once your .xip file is downloaded, you can expand it with the following command.

```sh
xip --expand Xcode_12.2.xip
```

You can then move it to your `Applications` directory with the version number as a suffix (so multiple Xcode versions can be installed together and selected between using the `--xcode_version` Bazel flag).

```sh
mv Xcode.app /Applications/Xcode_12.2.app
```

If this is the first Xcode version you're installing, you'll want to select it as your default Xcode version with:

```sh
sudo xcode-select -s /Applications/Xcode_12.2.app
```

You can then accept the license with:

```sh
sudo xcodebuild -license accept
```

And run the "first launch" with

```sh
sudo xcodebuild -runFirstLaunch
```

### Installing Homebrew

You'll likely want to install [Homebrew](https://brew.sh/) on your fresh executor to make installing other software easier. You can install it with the following line:

```
/bin/bash -c "$(curl -fsSL https://raw.githubusercontent.com/Homebrew/install/HEAD/install.sh)"
```

## Installing the BuildBuddy Mac executor

Now that the environment is configured, we can download and install the BuildBuddy Mac executor.

### Download the BuildBuddy executor

The BuildBuddy executor binary can be downloaded with (make sure to update the version number to the [lastest release](https://github.com/buildbuddy-io/buildbuddy/releases)):

```
curl -fSL https://github.com/buildbuddy-io/buildbuddy/releases/download/v2.3.0/executor-enterprise-darwin-amd64 -o buildbuddy-executor
```

### Make the executor executable

In order to run the executor binary, we must first make it executable with:

```
chmod +x buildbuddy-executor
```

### Create directories

If you don't already have any launch agents installed, you'll need to make sure the `~/Library/LaunchAgents/` directory exits with:

```
mkdir -p ~/Library/LaunchAgents/
```

You'll also need a directory to store the executor's disk cache and execution roots. We recommend _avoiding_ using the `/tmp` directory since this is periodically cleaned up.

```
mkdir -p buildbuddy
```

### Create config file

You'll need to create a `config.yaml` with the following contents:

```
executor:
  root_directory: "/Users/YOUR_USERNAME/buildbuddy/remote_build"
  app_target: "grpcs://YOUR_BUILDBUDDY_CLUSTER_URL:443"
  local_cache_directory: "/Users/YOUR_USERNAME/buildbuddy/filecache"
  local_cache_size_bytes: 100000000000  # 100GB
```

Make sure to replace _YOUR_USERNAME_ with your Mac username and _YOUR_BUILDBUDDY_CLUSTER_URL_ with the grpc url the BuildBuddy cluster you deployed. If you deployed the cluster without an NGINX Ingress, you'll need to update the protocol to grpc:// and the port to 1985.

### Create a Launch Agent .plist file

Now that everything is in place, we can create a LaunchAgent .plist file that tells Mac OS to keep the executor binary running on launch, and restart it if ever stops.

Make sure to replace _YOUR_USERNAME_ with your Mac username and _YOUR_MACS_NETWORK_ADDRESS_ with the IP address or DNS name of the Mac.

You can place this file in `~/Library/LaunchAgents/buildbuddy-executor.plist`.

```
<?xml version=\"1.0\" encoding=\"UTF-8\"?>
<!DOCTYPE plist PUBLIC \"-//Apple//DTD PLIST 1.0//EN\" \"http://www.apple.com/DTDs/PropertyList-1.0.dtd\">
<plist version=\"1.0\">
    <dict>
        <key>Label</key>
        <string>buildbuddy-executor</string>
        <key>EnvironmentVariables</key>
        <dict>
            <key>MY_HOSTNAME</key>
            <string>YOUR_MACS_NETWORK_ADDRESS</string>
            <key>MY_POOL</key>
            <string></string>
            <key>PATH</key>
            <string>/opt/homebrew/bin:/opt/homebrew/sbin:/usr/local/bin:/usr/bin:/bin:/usr/sbin:/sbin:/Library/Apple/usr/bin</string>
        </dict>
        <key>WorkingDirectory</key>
        <string>/Users/YOUR_USERNAME</string>
        <key>ProgramArguments</key>
        <array>
            <string>./buildbuddy-executor</string>
            <string>--config_file</string>
            <string>config.yaml</string>
        </array>
        <key>KeepAlive</key>
        <true/>
        <key>RunAtLoad</key>
        <true/>
        <key>StandardErrorPath</key>
        <string>/Users/YOUR_USERNAME/buildbuddy_stderr.log</string>
        <key>StandardOutPath</key>
        <string>/Users/YOUR_USERNAME/buildbuddy_stdout.log</string>
    </dict>
</plist>
```

### Update Launch Agent plist permissions

You may need to update the file's permissions with:

```
chmod 600 ~/Library/LaunchAgents/buildbuddy-executor.plist
```

### Start the Launch Agent

You can load the Launch Agent with:

```
launchctl load ~/Library/LaunchAgents/buildbuddy-executor.plist
```

And start it with:

```
launchctl start buildbuddy-executor
```

### Verify installation

You can verify that your BuildBuddy Executor successfully connected to the cluster by live tailing the stdout file:

```
tail -f buildbuddy_stdout.log
```

## Updating

When updating your BuildBuddy Executors, you should restart one executor at a time, waiting for the previous executor to successfully start up before restarting the next. This will ensure that work in flight is successfully rescheduled to another executor.

You can check that an executor has successfully started by checking that its `readyz` endpoint returns the string `OK`:

```bash
if [ "$(curl -s -X GET http://localhost:8080/readyz?server-type=prod-buildbuddy-executor || true)" == "OK" ]; then
    echo "Executor is ready"
fi
```

## Optional setup

### Optional: Enable Autologin

If your Mac executor restarts for whatever reason, you'll likely want to enable auto login so the executor will reconnect after rebooting instead of getting stuck on a login screen.

There's a convenient `brew` package called `kcpassword` that makes this easy.

```
brew tap xfreebird/utils
brew install kcpassword

sudo enable_autologin "MY_USER" "MY_PASSWORD"
```

### Optional: Install Java

If you're doing a lot of Java builds on your Mac executors that are not fully hermetic (i.e. rely on the system installed Java rather than the remote Java SDK shipped by Bazel), you can install the JDK with:

```
brew install --cask adoptopenjdk
```

### Optional: Increase the maximum number of open files

Some builds will exceed the default maximum number of open files on the Mac executor (which is relatively low). You'll know if you're hitting this limit if you see an error message that looks like `too many open files in system`.

You can increase this limit by running the following command:

```
sudo launchctl limit maxfiles 5000000 5000000
```

### Optional: Log rotation

If you find your logs are taking up too much space on disk, you may wish to implement log rotation. For this, we recommend `multilog` from `daemontools`:

```
brew install daemontools
```

Now that `multilog` is installed, in `~/Library/LaunchAgents/buildbuddy-executor.plist` change:

```
        <key>ProgramArguments</key>
        <array>
            <string>./buildbuddy-executor</string>
            <string>--config_file</string>
            <string>config.yaml</string>
        </array>
```

to:

```
        <key>ProgramArguments</key>
        <array>
            <string>bash</string>
            <string>-c</string>
            <string><![CDATA[./buildbuddy-executor --config_file config.yaml 2>&1 | /usr/local/bin/multilog t s2147483648 n25 /Users/YOUR_USERNAME/buildbuddy.log]]></string>
        </array>
```

and remove:

```
        <key>StandardErrorPath</key>
        <string>/Users/YOUR_USERNAME/buildbuddy_stderr.log</string>
        <key>StandardOutPath</key>
        <string>/Users/YOUR_USERNAME/buildbuddy_stdout.log</string>
```

This will produce automatically rotated log files with stdout and stderr interleaved. If you wish to preserve the separation of the out and error streams, you may instead use:

```
        <key>ProgramArguments</key>
        <array>
            <string>bash</string>
            <string>-c</string>
            <string><![CDATA[{ ./buildbuddy-executor --config_file config.yaml 2>&1 1>&3 3>&- | /usr/local/bin/multilog t s2147483648 n25 /Users/YOUR_USERNAME/buildbuddy_stderr.log; } 3>&1 1>&2 | /usr/local/bin/multilog t s2147483648 n25 /Users/YOUR_USERNAME/buildbuddy_stdout.log]]></string>
        </array>
```

for the `ProgramArguments`, though generally this is not recommended, as the streams are more clear when left chronologically interleaved.

Read more about `multilog` [here](https://cr.yp.to/daemontools/multilog.html).
