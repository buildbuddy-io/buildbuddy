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

The fork's `master` tracks upstream `master` and carries none of our changes.
Instead, each podman version has a branch `buildbuddy/vX.Y.Z`, created at
the upstream tag `vX.Y.Z`, with our changes on top. Workflow changes are
marked with a `# buildbuddy:` comment. Changes land on these branches via PRs
(merge commits are fine); never force-push them. The workflow builds and
tests on pushes to `buildbuddy/**` and on PRs, and only publishes releases
for `buildbuddy-v*` tags.

To see our changes for a version, run
`git diff vX.Y.Z origin/buildbuddy/vX.Y.Z`.

To build a new version of podman from the fork:

```shell
# Clone the fork and fetch upstream tags
git clone git@github.com:buildbuddy-io/podman-static.git
cd podman-static
git remote add upstream https://github.com/mgoltzsche/podman-static
git fetch upstream --tags
PREV=vA.B.C     # version of the latest buildbuddy/ branch
VERSION=vX.Y.Z  # new upstream version
# Create the release branch at the upstream tag
git push origin "$VERSION:refs/heads/buildbuddy/$VERSION"
git fetch origin
# Re-apply our changes on a work branch
git checkout -b "update-$VERSION" "origin/buildbuddy/$VERSION"
git cherry-pick $(git rev-list --reverse --no-merges "$PREV..origin/buildbuddy/$PREV")
# Resolve conflicts if needed, and check whether the upstream workflow has
# any new steps that need to be disabled.
git push origin "update-$VERSION"
```

Then open a PR against `buildbuddy/$VERSION` and merge it. After that, tag
the branch to trigger a release. The workflow builds the signed tarballs and
creates the GitHub release.

```shell
git fetch origin
git tag "buildbuddy-$VERSION" "origin/buildbuddy/$VERSION"
git push origin "buildbuddy-$VERSION"
# Monitor the release at https://github.com/buildbuddy-io/podman-static/actions
```

If a fix lands on the branch after a release, cut a new tag with a suffix
(e.g. `buildbuddy-v5.1.1-1`) and use that in place of `buildbuddy-$VERSION`
below.

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
