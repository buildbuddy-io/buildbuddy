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

The only reason for this fork is to ensure the integrity of the built
binaries. The fork includes a single patch that we maintain, which just
disables some steps in the release workflow that we don't need.

To build a new version of podman from the fork:

```shell
# Clone the fork and ensure you have the master branch checked out
git clone https://github.com/buildbuddy-io/podman-static
cd podman-static
git checkout master
# Add upstream as a remote and fetch
git remote add upstream https://github.com/mgoltzsche/podman-static
git fetch upstream --tags
# Get the commit SHA of our custom patch
PATCH_COMMIT=$(git rev-parse HEAD)
# Temporarily undo the patch
git reset --hard HEAD~1
# Update the master branch to point to the podman version you want
VERSION=vX.Y.Z
git reset --hard "$VERSION"
# Reapply our patch, resolving conflicts if needed
git apply "$PATCH_COMMIT"
# You should also look at the release workflow and check whether
# are any new steps that need to be disabled, and modify the patch if
# needed. If you do this, then run:
git add . && git commit --amend
# Push the tag, which should trigger a release
git tag "buildbuddy-$VERSION"
git push "buildbuddy-$VERSION"
# Monitor the release at https://github.com/buildbuddy-io/podman-static/actions
# If successful, update the master branch
git push origin master --force
```
