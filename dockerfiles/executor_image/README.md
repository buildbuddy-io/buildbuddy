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

The fork exists so that we build and release the binaries ourselves (for
integrity), and so that we can carry a few patches: changes to the release
workflow, plus fixes such as toolchain or CVE bumps.

- `master` mirrors upstream `master` and contains none of our changes.
- Each release has a branch `buildbuddy/vX.Y.Z`, cut from upstream's `vX.Y.Z`
  tag (which matches the podman version). Our changes land on that branch
  through reviewed PRs, one change per PR.
- A release is the tag `buildbuddy-vX.Y.Z` on the reviewed head of that
  branch. Pushing the tag runs the release workflow. `deps.bzl` downloads
  from this tag via `PODMAN_VERSION`, so it must be exactly `buildbuddy-`
  plus the podman version. Never move or re-push an existing tag: the
  checksums in `deps.bzl` pin it.
- Never push upstream's `v*` tags to our fork. Upstream's workflow fires on
  `v*` tags and would try to publish.

To release a new version of podman from the fork:

```shell
git clone git@github.com:buildbuddy-io/podman-static.git
cd podman-static
git remote add upstream https://github.com/mgoltzsche/podman-static
git fetch upstream --tags
# Fast-forward our master to upstream master
git checkout master
git merge --ff-only upstream/master
git push origin master
# Cut the release branch from upstream's tag (push the branch, not the tag)
OLD=vA.B.C VERSION=vX.Y.Z
git switch -c "buildbuddy/$VERSION" "$VERSION"
git push origin "buildbuddy/$VERSION"
# List our commits on the previous release branch. Cherry-pick the ones that
# still make sense onto a topic branch per change and open PRs into
# buildbuddy/$VERSION; drop the obsolete ones. Also check whether upstream's
# release workflow gained steps that need disabling.
git log --oneline "$OLD..origin/buildbuddy/$OLD"
# Once the PRs are merged, tag the reviewed head and push the tag
git fetch origin
git tag "buildbuddy-$VERSION" "origin/buildbuddy/$VERSION"
git push origin "buildbuddy-$VERSION"
# Monitor the release at https://github.com/buildbuddy-io/podman-static/actions
# If successful, update PODMAN_VERSION and the checksums in deps.bzl
```

## Overwriting binaries

If you add or remove binaries in the container layers in
`enterprise/server/cmd/executor/BUILD` which are overwriting binaries installed
in `dockerfiles/executor_image/Dockerfile`, make sure to appropriately add or
remove the corresponding path in the `rm` command in the Dockerfile.
