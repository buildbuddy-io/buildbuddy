# buildbuddy-io/plugins:config-per-target

The config-per-target plugin allows you to specify arguments to pass to
executables run by bazel on a per-target basis. The arguments will be prepended
to the options specified on the command line, as this will sometimes allow
options specified on the command line to override those specified as default
options by this plugin (dependent on the implementation of the application).

## Installation

Install this plugin with:

```
bb install buildbuddy-io/plugins:config-per-target
```

## Configuration

Create a `config.json` file in the plugin directory. The JSON should specify a
map of execution targets to arrays of arguments to add. Any targets not present
will leave the arguments list unmodified. Here is an example `config.json`:

```
{
	"//target1": ["--option1", "--option2=value"],
	"//target3": ["--option3"],
	"//target4": ["--option4", "positional_argument"],
}
```
