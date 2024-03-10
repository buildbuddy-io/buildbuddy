#!/usr/bin/env bash
set -e

# Creates a static archive containing podman deb packages from the third-party
# apt repository as well as a dependency listing of standard apt packages that
# are required.

# Customize the following params as needed:

# Executor base image (first line of Dockerfile).
EXECUTOR_BASE_IMAGE="gcr.io/cloud-marketplace/google/debian11@sha256:69e2789c9f3d28c6a0f13b25062c240ee7772be1f5e6d41bb4680b63eae6b304"
# Tool used to run the executor base image container,
# where we will run apt commands to query podman deps.
BUILD_TOOL="podman"
# Podman version to be installed.
PODMAN_VERSION="4.9.1"
# Podman apt package, including version selector.
PODMAN_APT_PACKAGE="podman=100:4.9.1-1"
# Skopeo apt package, including version selector.
SKOPEO_APT_PACKAGE="skopeo=100:1.13.3-1"

WORKDIR=$(mktemp -d /tmp/podman-bundle.XXXXXX)
cd "$WORKDIR"
echo "Working directory: $WORKDIR"

"$BUILD_TOOL" run \
  --volume="$WORKDIR:/work" \
  --rm \
  "$EXECUTOR_BASE_IMAGE" bash -ec "

cd /work

apt-get update
apt-get install -y curl gnupg apt-rdepends dpkg-dev

echo 'deb https://downloadcontent.opensuse.org/repositories/home:/alvistack/Debian_11/ /' >> /etc/apt/sources.list.d/home:alvistack.list
curl -fsSL https://download.opensuse.org/repositories/home:/alvistack/Debian_11/Release.key | gpg --dearmor >> /etc/apt/trusted.gpg.d/home_alvistack_debian11.gpg
apt-get update

# Do a dry-run install of podman and skopeo.
apt-get install --dry-run $PODMAN_APT_PACKAGE $SKOPEO_APT_PACKAGE > /tmp/dry_run.txt

# See which packages would be installed from alvistack if we were to install
# podman and skopeo, then download those packages as debs.

cat /tmp/dry_run.txt |
  grep '^Inst ' |
  grep 'alvistack' |
  perl -pe 's@Inst (.*?) \((.*?) .*@\1=\2@' |
  xargs -n1 apt-get download

# Write non-alvistack package names to deps.txt - we can install those from
# standard apt repos.

cat /tmp/dry_run.txt |
  grep '^Inst ' |
  grep -v 'alvistack' |
  perl -pe 's@Inst (.*?) \((.*?) .*@\1=\2@' |
  tee deps.txt
"

# Write a convenience installer script, which installs all deb dependencies from
# deps.txt then installs all the deb archives.
echo >install.sh '#!/usr/bin/env bash
set -e
cd $(dirname "$0")
apt-get update
cat deps.txt | xargs apt-get install -y
dpkg -i *.deb
' && chmod +x install.sh

# Now package all of this up into an archive and upload it to GCS. The docker
# image will fetch this archive and run the install script.

ARCHIVE="podman-${PODMAN_VERSION}.tar.gz"
tar -czvf "$ARCHIVE" install.sh deps.txt ./*.deb

GSPATH="buildbuddy-tools/binaries/podman-debian/$ARCHIVE"

gsutil cp "$ARCHIVE" "gs://$GSPATH"

echo "Uploaded archive to https://storage.googleapis.com/$GSPATH"
sha256sum "$ARCHIVE"
