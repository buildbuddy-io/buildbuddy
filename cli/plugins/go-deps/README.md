# buildbuddy-io/plugins:go-deps

The `go-deps` plugin allows running
[gazelle](https://github.com/bazelbuild/bazel-gazelle) when a missing Go
dependency is detected in your build output.

## Installation

Install this plugin with:

```
bb install buildbuddy-io/plugins:go-deps
```

## Usage

To use this plugin, you'll need:

- `python3` installed on your system
- [`gazelle`](https://github.com/bazelbuild/bazel-gazelle) setup in your
  bazel workspace (make sure you can successfully run `bb run :gazelle`)

### Fixing go deps automatically

By default, the plugin will ask whether you want to run Gazelle each time.
You can answer yes or no on a case-by-case basis, but it's more powerful
to answer "always" and let the plugin run automatically. For a nice
development workflow, you can pair this with BuildBuddy's `--watch`
feature to automatically apply Gazelle fixes and then re-run your app
automatically once the fix is applied.

### Resetting preferences

If you answer "always" or "never" to one of the plugin prompts, it saves
your answer to a file in `~/.config/bb-go-deps-plugin` on Linux or
`~/Library/Application Support/bb-go-deps-plugin` on macOS. You can delete
this directory to make the plugin forget your response and ask again next
time.
