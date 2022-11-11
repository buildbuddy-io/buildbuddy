---
id: cli-plugins
title: CLI Plugins
sidebar_label: CLI Plugins
---

The BuildBuddy CLI comes with a robust plugin system. Plugins are super simple to write, share, and install.

You can find a list of plugins that you can install in our [plugin library](/plugins).

## Installing a plugin

Plugins can be installed using the `bb install` command.

### Example

Here's an example of a command that installs the [open-invocation plugin](https://github.com/buildbuddy-io/plugins/tree/main/open-invocation#readme):

```bash
bb install buildbuddy-io/plugins:open-invocation
```

This is shorthand for the for:

```bash
bb install https://github.com/buildbuddy-io/plugins@main:open_invocation
```

### Syntax

The syntax for the install command is as follows:

```bash
bb install [REPO][@VERSION][:PATH]
```

Notice the 3 components that define the plugin being installed: **REPO**, **VERSION**, and **PATH** which are all optional.

Either a **REPO** or a **PATH** must be specified (otherwise, the CLI wouldn't know what plugin to install).

Below is a detailed description of each of these components:

#### REPO

The **REPO** component defines a git repository from which to install the plugin. If **REPO** is omitted, the CLI will look for a plugin in the current repository based on the **PATH** component.

This makes it easy to write new plugins directly within your existing repo, without having to create a new repo per plugin.

You can specify the **REPO** component as a fully qualified git url, like `github.com/buildbuddy-io/plugins` or using a shorthand owner/repo notation like `buildbuddy-io/plugins`, in which case `github.com/` will automatically be prepended.

#### VERSION

This allows you to lock a plugin to a specific git tag, branch, or commit SHA. If **VERSION** is omitted, the CLI will pull the plugin from head.

#### PATH

A BuildBuddy CLI plugin is simply a directory. The **PATH** component defines the directory in which the plugin is contained. If no path is specified, the BuildBuddy CLI look for a plugin in the repository's root directory.

### User specific plugins

By default plugins are installed at the repository level. They are saved in the `buildbuddy.yaml` file in the root of your repository.

Sometimes you may want to install a plugin for yourself (like a theme plugin for example), but don't want to force it on everyone on your project. In that case, you can install a plugin as a user-specific plugin.

Installing a plugin as user-specific is as simple as just tacking the `--user` argument onto your `bb install` command.

When the `--user` argument is present, the plugin will be added to a `~/buildbuddy.yaml` file located in your user directory. That means those plugins will only be applied to you.

If a plugin is present in both your user-specific and workspace at differnet versions, the one in your workspace will take precedence.

### Manual install

You can manually install plugins by editing your `buildbuddy.yaml` file with a text editor.

They live under a `plugins:` section, like so:

```yaml
plugins:
  # Local plugins
  - path: cli/example_plugins/go_highlight
  - path: cli/example_plugins/ping_remote
  # Plugins in external repos
  - path: cli/plugins/go_deps
    repo: buildbuddy-io/plugins
  - path: cli/plugins/open_invocation
    repo: buildbuddy-io/plugins
  - path: cli/plugins/notify
    repo: buildbuddy-io/plugins
  - path: cli/plugins/theme_modern
    repo: buildbuddy-io/plugins
  # Single-plugin repo
  - repo: bduffany/go-highlight
```

You can check out our `buildbuddy.yaml` file [here](https://github.com/buildbuddy-io/buildbuddy/blob/master/buildbuddy.yaml#L55).

## Creating a plugin

Creating a plugin is simple, it's just a directory. The directory can live within your repo, or in a separate repository.

There are 3 files you can place in your plugin directory, each corresponding to different a hook.

The files are simply bash scripts, which gives you the flexibility to write them in any language you want.

A single plugin can contain multiple hook scripts.

### pre_bazel.sh

The `pre_bazel.sh` script will be called before Bazel is run. It is called with a single argument, which is the path to a file containing all arguments that will be passed to Bazel (including the arguments specified in your `.bazelrc` and expanded via the `--config=` flag).

The script will be called like this:

```bash
/usr/bin/env bash /path/to/plugin/pre_bazel.sh /path/to/bazel-args
```

Here's an example of what the Bazel args file might look like:

```
--bes_results_url=https://app.buildbuddy.io/invocation/ --bes_backend=grpcs://remote.buildbuddy.io --noremote_upload_local_results --workspace_status_command=$(pwd)/workspace_status.sh --incompatible_remote_build_event_upload_respect_no_cache --experimental_remote_cache_async --incompatible_strict_action_env --enable_runfiles=1 --build_tag_filters=-docker --config=cache --bes_results_url=https://app.buildbuddy.io/invocation/ --bes_backend=grpcs://remote.buildbuddy.io --remote_cache=grpcs://remote.buildbuddy.io --remote_upload_local_results --experimental_remote_cache_compression --noremote_upload_local_results --noincompatible_remote_build_event_upload_respect_no_cache
```

Your plugin can modify this file to add, remove, or change that flags that will ultimately be passed to Bazel.

Your `pre_bazel.sh` script can also accept user input, spawn other processes, or anything else you'd like.

Here's an example of a simple pre_bazel.sh plugin that disables remote execution if it's unable to ping `remote.buildbuddy.io` within 500ms.

```bash
#!/usr/bin/env bash

if ! grep -E '^--remote_executor=.*\.buildbuddy\.io$' "$1" &>/dev/null; then
  # BB remote execution is not enabled; do nothing.
  exit 0
fi

# Make sure we can ping the remote execution service in 500ms.
if ! timeout 0.5 ping -c 1 remote.buildbuddy.io &>/dev/null; then
  # Network is spotty; disable remote execution.
  echo "--remote_executor=" >>"$1"
fi
```

Because this is just a bash script, you can write your pre Bazel logic in python, js, or any other language you'd like.

The `pre_bazel.sh` script is also a great place for more complex plugins to make sure all of their dependencies are available / installed.

Here's example of a `pre_bazel.sh` script that makes sure both `python3` and `open` are installed, and then calls into a python script called `pre_bazel.py`.

```bash
#!/usr/bin/env bash
if ! which python3 &>/dev/null; then
  echo -e "\x1b[33mWarning: open_invocation plugin is disabled: missing 'python3' in \$PATH\x1b[m" >&2
  exit 0
fi
open_command=$( (which xdg-open open | head -n1) || true)
if [[ ! "$open_command" ]]; then
  echo -e "\x1b[33mWarning: open_invocation plugin is disabled: missing 'open' or 'xdg-open' in \$PATH\x1b[m" >&2
  exit
fi

exec python3 ./pre_bazel.py "$@"
```

### post_bazel.sh

The `post_bazel.sh` script is called after Bazel completes. It is called with a single argument, which contains the console output that was generated by Bazel. This allows you to analyze Bazel's output and perform actions based on these outputs.

The script will be called like this:

```bash
/usr/bin/env bash /path/to/plugin/post_bazel.sh /path/to/bazel-outputs
```

Here's an example of what the Bazel outputs file might look like:

```
INFO: Invocation ID: cd254c65-c657-4524-b084-15a20d4485d1
INFO: Streaming build results to: https://app.buildbuddy.io/invocation/cd254c65-c657-4524-b084-15a20d4485d1
DEBUG: /root/workspace/output-base/external/io_bazel_rules_k8s/toolchains/kubectl/kubectl_toolchain.bzl:28:14: No kubectl tool was found or built, executing run for rules_k8s targets might not work.
INFO: Analyzed 1212 targets (345 packages loaded, 28870 targets configured).
INFO: Found 1042 targets and 170 test targets...
INFO: From Executing genrule //enterprise/vmsupport/bin:mkinitrd:
2022/11/10 22:50:31.562 INF Wrote /buildbuddy/remotebuilds/db2ec792-449d-4957-adbd-ce43e9afef2c/bazel-out/k8-fastbuild/bin/enterprise/vmsupport/bin/initrd.cpio (44.45 MB)
INFO: From Executing genrule //enterprise/vmsupport/bin:mkinitrd [for host]:
2022/11/10 22:50:28.188 INF Wrote /buildbuddy/remotebuilds/ff369499-df70-4cdd-8ac0-468b24f9bcd4/bazel-out/host/bin/enterprise/vmsupport/bin/initrd.cpio (55.69 MB)
INFO: Elapsed time: 67.685s, Critical Path: 10.30s
INFO: 5868 processes: 5866 remote cache hit, 1 internal, 1 remote.
//app:app_typecheck_test                                        (cached) PASSED in 0.4s
//server/util/terminal:terminal_test                            (cached) PASSED in 0.7s
//server/util/url:url_test                                      (cached) PASSED in 0.4s
//enterprise/server/remote_execution/containers/sandbox:sandbox_test    SKIPPED

Executed 0 out of 170 tests: 169 tests pass and 1 was skipped.
There were tests whose specified size is too big. Use the --test_verbose_timeout_warnings command line option to see which ones these are.
```

Your `post_bazel.sh` script can also accept user input, spawn other processes, or anything else you'd like.

Here's an example of a simple plugin that sends a desktop notification once the build completes:

```bash
#!/usr/bin/env bash
set -eu

STATUS_LINE=$(grep "Build" "$1" | grep "complete" | tail -n1 | perl -p -e 's@.*?(Build)@\1@')
ELAPSED_TIME_LINE=$(grep "Elapsed time" "$1" | tail -n1 | perl -p -e 's@.*?(Elapsed)@\1@')

TITLE="Bazel build finished"
MESSAGE="${ELAPSED_TIME_LINE}\n${STATUS_LINE}"

if [[ "$OSTYPE" == darwin* ]]; then
  SCRIPT="display notification \"$MESSAGE\" with title \"$TITLE\""

  osascript -e "$SCRIPT"
  exit 0
fi

notify-send --expire-time 3000 "$TITLE" "$MESSAGE"
```

Here's another example of a more complex plugin, that simply calls a python script name `post_bazel.py` after checking that `python3` is installed:

```bash
#!/usr/bin/env bash
if ! which python3 &>/dev/null; then
  echo -e "\x1b[33mWarning: go_deps plugin is disabled: missing 'python3' in \$PATH\x1b[m" >&2
  exit 0
fi
exec python3 ./post_bazel.py "$@"

```

### handle_bazel_output.sh

The `handle_bazel_output.sh` is piped all Bazel output during a build via stdin. This allows you to transform and modify the output that Bazel displays to users.

Here's a simple script that calls a python script `handle_bazel_output.py`:

```bash
#!/usr/bin/env bash
if ! which python3 &>/dev/null; then
  echo -e "\x1b[33mWarning: theme_modern plugin is disabled: missing 'python3' in \$PATH\x1b[m" >&2
  exec cat
fi
exec python3 ./handle_bazel_output.py "$@"
```

And the Python script `handle_bazel_output.py` that highlights go errors:

```py
import re
import sys

if __name__ == "__main__":
    for line in sys.stdin:
        m = re.search(r"^(.*?\.go:\d+:\d+:)(.*)", line)
        if m:
            print("\x1b[33m" + m.group(1) + "\x1b[0m" + m.group(2))
        else:
            print(line, end="")

```

Or another `handle_bazel_output.py` python script that changes the colors of various Bazel outputs:

```py
import re
import sys

defaultGreen = "\x1b[32m"
brightGreenBold = "\x1b[92;1m"
brightCyanBold = "\x1b[96;1m"
brightWhiteBoldUnderline = "\x1b[37;1;4m"
reset = "\x1b[0m"

if __name__ == "__main__":
    for line in sys.stdin:
        line = line.replace(defaultGreen+"INFO", brightGreenBold+"INFO")
        line = line.replace(defaultGreen+"Loading", brightCyanBold+"Loading")
        line = line.replace(defaultGreen+"Analyzing", brightCyanBold+"Analyzing")
        line = line.replace(defaultGreen+"[", brightCyanBold+"[")
        m = re.search(r"^(.*)(Streaming build results to: )(.*)$", line)
        if m:
            print(m.group(1) + m.group(2) + brightWhiteBoldUnderline + m.group(3) + reset)
        else:
            print(line, end="")
```

### Environment variables

Certain environment variables are made available to your plugins, giving you

#### $PLUGIN_TEMPDIR

This is a temporary directory that can be used by your plugin to store temporary files. These files will be cleaned up after the invocation is complete.

#### $USER_CONFIG_DIR

This is a long-lived directory you can use to store user preferences, like whether or not a user always wants to automatically apply a particular fix.

### Examples

Here are some examples of plugins that can help you get started quickly:

`pre_bazel.sh`

- ping_remote: https://github.com/buildbuddy-io/buildbuddy/tree/master/cli/example_plugins/ping_remote

`post_bazel.sh`

- open_invocation: https://github.com/buildbuddy-io/buildbuddy/tree/master/cli/plugins/open_invocation
- notify: https://github.com/buildbuddy-io/buildbuddy/tree/master/cli/plugins/notify
- go_deps: https://github.com/buildbuddy-io/buildbuddy/tree/master/cli/plugins/go_deps

`handle_bazel_output.sh`

- go-highlight: https://github.com/bduffany/go-highlight
- theme-modern: https://github.com/siggisim/theme-modern

## Sharing a plugin

Because a plugin is just a directory in a repo, sharing plugins is super easy.

You can either a single plugin in a repo, like [this](https://github.com/bduffany/go-highlight) or host multiple plugins in a repo like [this](https://github.com/buildbuddy-io/plugins).

For single plugin repos, others can install your plugin with:

```bash
bb install your-username/your-repo
```

For multi plugin repos, others can install your plugin with:

```bash
bb install your-username/your-repo:path-to-repo
```

If you want to share your plugin with the wider BuildBuddy community, you can submit it to our plugin library [here](/plugins#share).
