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

The fork exists to ensure the integrity of the built binaries. It changes the
release workflow and may carry a few fixes.

Don't use or sync the fork's `master`: it's stale and has diverged from
upstream. Since v5.7.0, each podman version has a branch `buildbuddy/vX.Y.Z`,
created at the upstream tag `vX.Y.Z`, with our changes on top (workflow changes
are marked with a `# buildbuddy:` comment). Changes land on these branches via
PRs; never force-push them. The initial branch push runs no CI; the PR does.

To build a new version of podman from the fork:

```shell
git clone git@github.com:buildbuddy-io/podman-static.git
cd podman-static
git remote add upstream https://github.com/mgoltzsche/podman-static
git fetch upstream --tags
PREV=vA.B.C     # version of the latest buildbuddy/ branch
VERSION=vX.Y.Z  # new upstream version
# Create the release branch at the upstream tag
git push origin "$VERSION^{commit}:refs/heads/buildbuddy/$VERSION"
# Re-apply our changes on a work branch
git checkout -b "update-$VERSION" "$VERSION"
# This skips merge commits, so compare `git diff "$PREV" "origin/buildbuddy/$PREV"`
# with `git diff "$VERSION" HEAD` to confirm nothing was lost.
git cherry-pick $(git rev-list --reverse --topo-order --no-merges "$PREV..origin/buildbuddy/$PREV")
# Finish with `git cherry-pick --continue`, or `--skip` for picks that come out
# empty. Dockerfile commits (Go version, fuse-overlayfs, seccomp) are
# version-specific: never keep older pins. Keep the static-link checks regardless
# of what happens to the Go pin: 7175f87 bundles the `readelf` checks for pasta
# and crun with it, and upstream has no static check for those as of v6.1.4.
# Check that the vendored seccomp path in the Dockerfile still exists, and
# whether the upstream workflow has any new steps that need to be disabled.
```

Once the cherry-pick is finished, open the PR:

```shell
git push origin "update-$VERSION"
gh pr create --repo buildbuddy-io/podman-static --base "buildbuddy/$VERSION"
```

Merge the PR, then tag the branch to release; the workflow builds the tarballs
and creates the GitHub release. Releases are signed with key
`AB2B6CC5FC981BF89D35C7869668B909E1D76F80`; the fork's `verify-signature` target
and README name upstream's key, so they won't verify our releases.

```shell
git fetch origin
# For a fix after a release, add a suffix (e.g. buildbuddy-v5.1.1-1)
TAG="buildbuddy-$VERSION"
git tag "$TAG" "origin/buildbuddy/$VERSION"
git push origin "$TAG"
# Monitor the release at https://github.com/buildbuddy-io/podman-static/actions
```

Finally, switch to a buildbuddy checkout and update `PODMAN_VERSION`,
`PODMAN_STATIC_SHA256_AMD64` and `PODMAN_STATIC_SHA256_ARM64` in `deps.bzl`.
`PODMAN_VERSION` is the tag minus `buildbuddy-`. Only the public `deps.bzl` needs
editing; it syncs to buildbuddy-internal automatically. To get the checksums:

```shell
TAG=buildbuddy-vX.Y.Z  # the tag you released
for ARCH in amd64 arm64; do
  curl -fsSLo "/tmp/podman-linux-$ARCH.tar.gz" "https://github.com/buildbuddy-io/podman-static/releases/download/$TAG/podman-linux-$ARCH.tar.gz" &&
    sha256sum "/tmp/podman-linux-$ARCH.tar.gz"
done
```

When reviewing a new version, extract these tarballs and run `readelf -l -d` on
each binary to confirm it is static: the output should have no `INTERP` or
`NEEDED`. As of v5.8.8 the Dockerfile only uses `readelf` for pasta and crun, and
its `ldd` checks don't reliably catch this.

Open a PR and run `//enterprise/server/test/integration/podman/...` and
`//enterprise/server/oci/ociregistry/...` with `--config=remote` before merging
(the default config skips their `docker`-tagged tests). The genrule in
`enterprise/server/remote_execution/containers/podman/BUILD` deletes specific
files from the tarball, so a layout change breaks the build.

Once merged, the new podman ships in the next executor release: the executor
image's podman layer (`podman_static_layer` in
`enterprise/server/cmd/executor/BUILD`) comes from this pin, so nothing updates
running executors until then.

## Overwriting binaries

If you add or remove binaries in the container layers in
`enterprise/server/cmd/executor/BUILD` which are overwriting binaries installed
in `dockerfiles/executor_image/Dockerfile`, make sure to appropriately add or
remove the corresponding path in the `rm` command in the Dockerfile.
