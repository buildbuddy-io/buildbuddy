# Executor image

This package defines most of the container image that we ship as part of
the executor release. It includes most system dependencies needed by the
executor (such as docker and podman). The executor binary as well as some
bazel-provisioned tools (such as firecracker) are additionally added to
the image in //enterprise/server/cmd/executor:executor_image.

## Multi-platform support

Each step in the Dockerfile needs to work on both amd64 and arm64 CPU
architectures. Follow the existing steps in the Dockerfile for examples of
how to install dependencies for the correct architecture.

To do a test build, you can use a temporary ARM instance on GCP or on a
Raspberry Pi (they are cheap).

## Podman installation

We install podman from https://github.com/buildbuddy-io/podman-static
which is a fork of https://github.com/mgoltzsche/podman-static.

The main reason for this fork is to ensure the integrity of the built
binaries. Our main change is to the release workflow
(`.github/workflows/workflow.yml`), adapting it for our releases (e.g.
disabling steps that we don't need). The fork may also carry a small number
of other commits, such as fixes that aren't upstream yet.

We update the fork by merging upstream release tags into master, and never
force-push master. To see our fork-specific changes, run
`git diff vX.Y.Z origin/master` or
`git log --no-merges vX.Y.Z..origin/master`, where `vX.Y.Z` is the upstream
release most recently merged into our master (an older tag would also show
upstream changes). To see how far behind upstream we are, run
`git rev-list --count origin/master..upstream/master` or check the fork banner
on GitHub.

To build a new version of podman from the fork:

```shell
# Clone the fork and fetch upstream
git clone git@github.com:buildbuddy-io/podman-static.git
cd podman-static
git remote add upstream https://github.com/mgoltzsche/podman-static
git fetch upstream --tags
# Merge the upstream release tag into a branch off master
VERSION=vX.Y.Z
git checkout -b "merge-$VERSION" origin/master
git merge "$VERSION"
# Resolve conflicts if needed. These are usually only in
# .github/workflows/workflow.yml, where our release changes live. Also
# check whether the workflow has any new steps that need to be disabled.
git push origin "merge-$VERSION"
```

Then open a PR against master and merge it with **"Create a merge commit"**.
Don't squash or rebase: that drops upstream's history and breaks the
ahead/behind comparison with upstream.

After the PR is merged, tag master to trigger a release. The workflow builds
the signed tarballs and creates the GitHub release.

```shell
git fetch origin
git tag "buildbuddy-$VERSION" origin/master
git push origin "buildbuddy-$VERSION"
# Monitor the release at https://github.com/buildbuddy-io/podman-static/actions
```

Finally, update `PODMAN_VERSION`, `PODMAN_STATIC_SHA256_AMD64` and
`PODMAN_STATIC_SHA256_ARM64` in `deps.bzl`. To get the checksums:

```shell
for ARCH in amd64 arm64; do
  curl -fsSLo "/tmp/podman-linux-$ARCH.tar.gz" "https://github.com/buildbuddy-io/podman-static/releases/download/buildbuddy-$VERSION/podman-linux-$ARCH.tar.gz" &&
    sha256sum "/tmp/podman-linux-$ARCH.tar.gz"
done
```

## Overwriting binaries

If you add or remove binaries in the container layers in
`enterprise/server/cmd/executor/BUILD` which are overwriting binaries installed
in `dockerfiles/executor_image/Dockerfile`, make sure to appropriately add or
remove the corresponding path in the `rm` command in the Dockerfile.
