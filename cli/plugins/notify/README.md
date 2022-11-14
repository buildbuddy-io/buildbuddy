# buildbuddy-io/plugins:notify

The notify plugin sends you desktop notifications when your build is
complete. This is useful for long-running builds that you don't want to
continually monitor, such as release builds.

To use it, pass the `--notify` argument to the CLI or set the `BB_NOTIFY=1`
environment variable.

To enable sound (currently macOS only), set the environment variable
`BB_NOTIFY_SOUND=1`. You can change the sound by setting
`BB_NOTIFY_SOUND_NAME` to one of the sound file names from
`/System/Library/Sounds`, excluding the file extension. Example:
`BB_NOTIFY_SOUND_NAME=Funk`.

## Installation

Install this plugin with:

```
bb install buildbuddy-io/plugins:notify
```

## Usage

```
bb build //... --notify
```
