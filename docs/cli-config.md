---
id: cli-config
title: Configuring the BuildBuddy CLI
sidebar_label: CLI configuration (.bbrc)
---

You can save BuildBuddy CLI options in a `.bbrc` file. It uses the same section and named-config syntax as a `.bazelrc`,
but it configures `bb` rather than Bazel.

For example, this `.bbrc` enables streamed logs for every `bb run`
and uses Linux runners for `bb remote`:

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

Only BuildBuddy CLI flags are allowed in `.bbrc`. Keep
regular Bazel options in `.bazelrc`.

The `startup` section applies before any command.
For example, this `.bbrc` enables verbose logging for all `bb` commands:

```bash title=".bbrc"
startup --verbose
```

## File locations

The CLI reads `.bbrc` files in this order:

1. `.bbrc` in the workspace root
2. `.bbrc` in your home directory (`~/.bbrc`)
3. Files passed using `--bbrc`, in command-line order

Later settings take precedence when an option can only have one value.

You can load another file using the same `import` and `try-import` directives
supported by `.bazelrc` files. For example, a checked-in workspace `.bbrc` can
optionally load a workspace-local `user.bbrc`:

```bash title=".bbrc"
try-import %workspace%/user.bbrc
```

## Named configs

Mirroring .bazelrc syntax, add `:<name>` to a section to define a named config. Select it with
`--bb_config=<name>`:

```bash title=".bbrc"
remote:linux --os=linux
remote:linux --arch=amd64
remote:ci --bb_config=linux
remote:ci --skip_auto_checkout=true
remote:custom-image --container_image=docker://ubuntu:latest
```

```bash
bb remote --bb_config=linux test //...
bb remote --bb_config=ci test //...
```

Multiple named configs are expanded in the order they appear. Mirroring bazelrc policy,
named configs are not supported for the `startup` section.

`--bb_config` is separate from Bazel's `--config`: use `--bb_config` for
`.bbrc` settings and `--config` for `.bazelrc` settings.

## Selecting rc files

Pass one or more `--bbrc` startup options to load additional files:

```bash
bb --bbrc=/path/to/team.bbrc remote test //...
```

`--ignore_all_bb_rc_files` ignores all `.bbrc` files.
