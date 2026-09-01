---
title: "Configure the BuildBuddy CLI with .bbrc"
date: 2026-08-18T10:00:00
authors: maggie
tags: [bazel]
---

You can now save BuildBuddy CLI options in a `.bbrc` file. It uses the same
section and named-config syntax as a `.bazelrc`, but it configures `bb` rather
than Bazel.

For example, this `.bbrc` enables streamed logs for every `bb run` and uses
Linux runners for `bb remote`:

```bash title=".bbrc"
run --stream_run_logs
remote --os=linux
```

```bash
# Runs: bb run --stream_run_logs //app
bb run //app

# Runs: bb remote --os=linux test //...
bb remote test //...
```

You can also define named configs and select them with `--bb_config`:

```bash title=".bbrc"
remote:linux --os=linux
remote:linux --arch=amd64
```

```bash
bb remote --bb_config=linux test //...
```

The CLI automatically reads `.bbrc` from the workspace root and your home
directory.

Only BuildBuddy CLI flags are allowed in `.bbrc`; regular Bazel
options still belong in `.bazelrc`.

See the [`.bbrc` documentation](/docs/cli-config) for more details.
