# This script installs packages necessary to build and run the BuildBuddy app and executors
# on a Linux VM. As of 2025-03-17 it works when spinning up a Google Compute Engine VM.
# Here's the command I used to create the VM, from the buildbuddy/ directory:
#
# gcloud compute instances create $YOUR_VM_NAME \
#  --zone=us-central1-a \
#  --machine-type=n2-standard-16 \
#  --enable-nested-virtualization \
#  --boot-disk-size=200GB \
#  --metadata-from-file startup-script=tools/gce-startup-script.sh
#
# The `google-startup-scripts` service executes the script.
# Logs from the script will appear under `google_metadata_script_runner` in /var/log/syslog.
# It is possible to ssh into the VM before the script finishes executing, so check logs if
# binaries are missing.

set -e

apt-get update && \
apt-get install -y --no-install-recommends \
	acl \
	build-essential \
	git \
	podman \
	redis \
	skopeo

# Install bazelisk
wget -q https://github.com/bazelbuild/bazelisk/releases/download/v1.25.0/bazelisk-linux-amd64
mv bazelisk-linux-amd64 /usr/local/bin/bazelisk
pushd /usr/local/bin
chmod ugo+x bazelisk
ln -s bazelisk bazel
popd

# Install Go
wget -q https://go.dev/dl/go1.24.1.linux-amd64.tar.gz
rm -rf /usr/local/go && tar -C /usr/local -xzf go1.24.1.linux-amd64.tar.gz
echo 'export PATH="$PATH:/usr/local/go/bin"' >> /etc/profile
