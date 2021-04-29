This page provides documentation for `buildbuddy.yaml`, which can be placed
at the root of your git repo to configure BuildBuddy [workflow](workflows) execution.

_NOTE: This file only takes effect after you
[enable workflows for the repo](workflows#enable-workflows-for-a-repo)._

## Default / example config

The following config is roughly equivalent to the one we use if you don't
have a `buildbuddy.yaml` in your repo.

You can copy this as a starting point for your own `buildbuddy.yaml`:

```yaml
actions:
  - name: "Test all targets"
    triggers:
      push:
        branches:
          - "main"
      pull_request:
        branches:
          - "main"
    bazel_commands:
      - "bazel test //... --build_metadata=ROLE=CI --bes_backend=grpcs://cloud.buildbuddy.io --bes_results_url=https://app.buildbuddy.io/invocation/"
```

A few points to note:

- This example uses `"main"` for the branch name -- if you copy this config,
  be sure to replace that with the name of your main branch. By default, we
  run the above bazel command when any branch is pushed.
- By default, we also pass `--remote_header=x-buildbuddy-api-key=<YOUR_API_KEY>`,
  so that workflow builds show up in your BuildBuddy org. For security reasons,
  we only do this if your repo is private.
- Remote cache and remote execution (RBE) require additional configuration.
  The configuration steps are the same as when running Bazel locally.
  See the **Setup** page in the BuildBuddy UI.
- Bazel commands are run directly in your workspace, which means that your
  `.bazelrc` is respected. If you have lots of flags, we recommend adding
  them to your `.bazelrc` instead of adding them in this YAML config.
