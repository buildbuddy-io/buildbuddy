# buildbuddy-io/plugins:open-invocation

The `open-invocation` plugin adds a flag "--open" to bazel commands which causes the
`bes_results_url` to be opened automatically in the Web browser after the
invocation is complete.

## Installation

Install this plugin with:

```
bb install buildbuddy-io/plugins:open-invocation
```

## Usage

```
bb build //... --open
```
