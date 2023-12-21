# BuildBuddy CLI

Install BuildBuddy CLI

```
curl -fsSL install.buildbuddy.io | bash
```

Kick off a Bazel build using BuildBuddy UI

```
bb build //...
```

## Dogfood build

To run the CLI in "dogfood" mode (build from `origin/master`), alias `bb`
to `cli/dev.sh` in your `~/.bashrc`.

Alternatively, you can create a symlink using something like the following
script, which creates the symlink `~/bin/bb` and adds it to `$PATH`:

```shell
mkdir -p ~/bin
ln -s "$(pwd)/cli/dev.sh" ~/bin/bb
which bb || echo 'PATH="$PATH:$HOME/bin"' >> ~/.bashrc
```
