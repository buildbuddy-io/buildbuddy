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

This is shorthand for:

```bash
bb install https://github.com/buildbuddy-io/plugins@main:open-invocation
```

### Syntax

The syntax for the install command is as follows:

```bash
bb install [REPO][@VERSION][:PATH]
```

Notice the 3 components that define the plugin being installed: **REPO**, **VERSION**, and **PATH** which are all optional.

Either a **REPO** or a **PATH** must be specified (otherwise, the CLI wouldn't know what plugin to install).

Below is a detailed description of each of these components.

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
  - path: cli/example_plugins/go-highlight
  - path: cli/example_plugins/ping-remote
  # Plugins in external repos
  - path: cli/plugins/go-deps
    repo: buildbuddy-io/plugins
  - path: cli/plugins/open-invocation
    repo: buildbuddy-io/plugins
  - path: cli/plugins/notify
    repo: buildbuddy-io/plugins
  - path: cli/plugins/theme-modern
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

The directory layout for a plugin looks like this:

```
path/to/plugin/
├── pre_bazel.sh            # optional
├── post_bazel.sh           # optional
└── handle_bazel_output.sh  # optional
```

### `pre_bazel.sh`

The `pre_bazel.sh` script will be called before Bazel is run.

It is called with a single argument, which is the path to a file
containing all arguments that will be passed to Bazel (including the
arguments specified in your `.bazelrc` and expanded via any `--config=`
flags). **Each line in this file contains a single argument.**

The script will be called like this:

```bash
/usr/bin/env bash /path/to/plugin/pre_bazel.sh /path/to/bazel-args
```

Here's an example of what the Bazel args file might look like:

```
--ignore_all_rc_files
run
//server
--bes_results_url=https://app.buildbuddy.io/invocation/
--bes_backend=grpcs://remote.buildbuddy.io
--noremote_upload_local_results
--workspace_status_command=$(pwd)/workspace_status.sh
--incompatible_remote_build_event_upload_respect_no_cache
--experimental_remote_cache_async
--incompatible_strict_action_env
--enable_runfiles=1
--build_tag_filters=-docker
--bes_results_url=https://app.buildbuddy.io/invocation/
--bes_backend=grpcs://remote.buildbuddy.io
--remote_cache=grpcs://remote.buildbuddy.io
--remote_upload_local_results
--experimental_remote_cache_compression
--noremote_upload_local_results
--noincompatible_remote_build_event_upload_respect_no_cache
```

Your plugin can modify this file to add, remove, or change that flags that will ultimately be passed to Bazel.

Your `pre_bazel.sh` script can also accept user input, spawn other processes, or anything else you'd like.

Here's an example of a simple `pre_bazel.sh` plugin that disables remote execution if it's unable to ping `remote.buildbuddy.io` within 500ms.

```bash
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
if ! which python3 &>/dev/null; then
  echo -e "\x1b[33mWarning: open-invocation plugin is disabled: missing 'python3' in \$PATH\x1b[m" >&2
  exit 0
fi
open_command=$( (which xdg-open open | head -n1) || true)
if [[ ! "$open_command" ]]; then
  echo -e "\x1b[33mWarning: open-invocation plugin is disabled: missing 'open' or 'xdg-open' in \$PATH\x1b[m" >&2
  exit
fi

exec python3 ./pre_bazel.py "$@"
```

### `post_bazel.sh`

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
if ! which python3 &>/dev/null; then
  echo -e "\x1b[33mWarning: go-deps plugin is disabled: missing 'python3' in \$PATH\x1b[m" >&2
  exit 0
fi
exec python3 ./post_bazel.py "$@"
```

### `handle_bazel_output.sh`

The `handle_bazel_output.sh` script receives on its stdin all of Bazel's
stderr output (not stdout). This is useful because Bazel outputs warnings,
errors, and progress output on stderr, allowing you to transform and
modify the output that Bazel displays to users.

As an example, we can write a `handle_bazel_output.sh` plugin to take the
plain output from a build, and add
[ANSI colors](https://wikipedia.org/wiki/ANSI_escape_code) to Go file
names to make them easier to spot.

Our `handle_bazel_output.sh` script delegates to a python script
`handle_bazel_output.py`, gracefully falling back to running `cat` if
Python is missing:

```bash
if ! which python3 &>/dev/null; then
  echo -e "\x1b[33mWarning: go-highlight plugin is disabled: missing 'python3' in \$PATH\x1b[m" >&2
  exec cat
fi
exec python3 ./handle_bazel_output.py "$@"
```

Here is the Python script `handle_bazel_output.py` from the
`go-highlight` plugin:

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

As another example, here is a `handle_bazel_output.py` script that changes
the colors of various Bazel outputs:

```py
import re
import sys

default_green = "\x1b[32m"
bright_green_bold = "\x1b[92;1m"
bright_cyan_bold = "\x1b[96;1m"
bright_white_bold_underline = "\x1b[37;1;4m"
reset = "\x1b[0m"

if __name__ == "__main__":
    for line in sys.stdin:
        line = line.replace(default_green+"INFO", bright_green_bold+"INFO")
        line = line.replace(default_green+"Loading", bright_cyan_bold+"Loading")
        line = line.replace(default_green+"Analyzing", bright_cyan_bold+"Analyzing")
        line = line.replace(default_green+"[", bright_cyan_bold+"[")
        m = re.search(r"^(.*)(Streaming build results to: )(.*)$", line)
        if m:
            print(m.group(1) + m.group(2) + bright_white_bold_underline + m.group(3) + reset)
        else:
            print(line, end="")
```

### Environment variables

The CLI exposes certain environment variables to your plugins.

#### $BUILD_WORKSPACE_DIRECTORY

This is the path to the Bazel workspace in which the CLI is run. It is the
root path, containing the bazel `WORKSPACE` or `WORKSPACE.bazel` file.

#### $PLUGIN_TEMPDIR

This is a temporary directory that can be used by your plugin to store temporary files. These files will be cleaned up after the invocation is complete.

#### $USER_CONFIG_DIR

This is a long-lived directory you can use to store user preferences, like whether or not a user always wants to automatically apply a particular fix.

Your plugin is responsible for provisioning its own directory under this
config dir, if needed, using something like
`mkdir -p $USER_CONFIG_DIR/my-plugin`. If you store user preferences here,
you'll need to decide how to handle differences in preferences across
different versions of your plugin.

#### $EXEC_ARGS_FILE

This is the path of a file that contains the args that would be passed to an
executable built by bazel as a result of a `bazel run` command. Specifically,
these are any positional arguments remaining after canonicalization of any
options that take arguments into "--option=value" options, excepting the `run`
subcommand itself and the build target. These are generally the arguments
following a `--` in the argument list passed to bazel, if any.

This environment variable will only be set for the `pre_bazel.sh` script in the
plugin. Plugins can change this file to change the arguments
passed to Bazel.

The file in question is formatted very similarly to the bazel args file, except
that the arguments will be split across lines based solely as a result of shell
lexing, as it is not possible to parse or canonicalize options without knowing
the internals of the executable to be run. The following is an example exec args
file:

```
--bool_var
true
positional_arg
--option=value
--option2
positional_arg3
--option4
```

### Examples

Here are some examples of plugins that can help you get started quickly:

`pre_bazel.sh`

- ping-remote: https://github.com/buildbuddy-io/buildbuddy/tree/master/cli/example_plugins/ping-remote

`post_bazel.sh`

- open-invocation: https://github.com/buildbuddy-io/buildbuddy/tree/master/cli/plugins/open-invocation
- notify: https://github.com/buildbuddy-io/buildbuddy/tree/master/cli/plugins/notify
- go-deps: https://github.com/buildbuddy-io/buildbuddy/tree/master/cli/plugins/go-deps

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

## Uninstalling a plugin

You can uninstall a plugin at any time by removing it from the `plugins:` block of your `buildbuddy.yaml` file.
