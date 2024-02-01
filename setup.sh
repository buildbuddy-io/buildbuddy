#!/bin/bash
set -e

#Bash script to set up a VM to run firecracker tests

sudo apt-get update
sudo apt-get install git

# Install gcloud
echo "deb [signed-by=/usr/share/keyrings/cloud.google.gpg] https://packages.cloud.google.com/apt cloud-sdk main" | sudo tee -a /etc/apt/sources.list.d/google-cloud-sdk.list
sudo apt-get install -y apt-transport-https ca-certificates gnupg
curl https://packages.cloud.google.com/apt/doc/apt-key.gpg | sudo gpg --dearmor -o /usr/share/keyrings/cloud.google.gpg
sudo apt-get update
sudo apt-get install -y google-cloud-sdk


# Enable virutalization
# NOTE: Run this command from VM
gcloud compute instances export $VM_NAME \
  --destination=out.yaml \
  --zone=us-central1-a
vim out.yaml
# Add
# advancedMachineFeatures:
# enableNestedVirtualization: true
gcloud compute instances update-from-file $VM_NAME \
  --source=out.yaml \
  --most-disruptive-allowed-action=RESTART \
  --zone=us-central1-a

sudo apt-get install acl
sudo setfacl -m u:${USER}:rw /dev/kvm
[ -r /dev/kvm ] && [ -w /dev/kvm ] && echo "OK" || echo "FAIL"
git clone https://github.com/buildbuddy-io/buildbuddy.git
sudo curl -Lo /usr/local/bin/bazelisk https://github.com/bazelbuild/bazelisk/releases/download/v1.15.0/bazelisk-linux-amd64
sudo chmod +x /usr/local/bin/bazelisk
sudo cp /usr/local/bin/bazelisk /usr/local/bin/bazel
cd buildbuddy
sudo apt-get install -y --no-install-recommends       build-essential

# Install latest firecracker
#release_url="https://github.com/firecracker-microvm/firecracker/releases"
#latest=$(basename $(curl -fsSLI -o /dev/null -w  %{url_effective} ${release_url}/latest))
#arch=`uname -m`
#curl -L ${release_url}/download/${latest}/firecracker-${latest}-${arch}.tgz | tar -xz
#mv release-${latest}-$(uname -m)/firecracker-${latest}-$(uname -m) firecracker
#sudo mv release-${latest}-$(uname -m)/jailer-${latest}-$(uname -m) /usr/bin/jailer
#sudo cp firecracker /usr/bin/

# Install firecracker used by our apps
wget -c https://github.com/firecracker-microvm/firecracker/releases/download/v1.1.1/firecracker-v1.1.1-x86_64.tgz
tar -xzf firecracker-v1.4.0-20230720-cf5f56f.tgz
cd release-v1.1.1-x86_64
sudo cp firecracker-v1.1.1-x86_64 /usr/bin/firecracker
sudo cp jailer-v1.1.1-x86_64 /usr/bin/jailer

# Install skopeo
echo 'deb https://downloadcontent.opensuse.org/repositories/home:/alvistack/Debian_11/ /' | sudo tee -a /etc/apt/sources.list.d/home:alvistack.list
curl -fsSL https://download.opensuse.org/repositories/home:/alvistack/Debian_11/Release.key | gpg --dearmor | sudo tee -a /etc/apt/trusted.gpg.d/home_alvistack_debian11.gpg
sudo apt-get update
sudo apt-get -y upgrade 
sudo apt-get install -y skopeo
#This alone works on debian
sudo apt install skopeo

# Install go
if ! [[ -e /usr/local/go/bin/go ]]; then
  echo "Installing go..."
  GO_VERSION=1.21.1
  sudo rm -rf /usr/local/go
  curl -fsSL https://go.dev/dl/go${GO_VERSION}.linux-amd64.tar.gz | \
    sudo tar --directory /usr/local -xzf -
fi
export PATH="$PATH:/usr/local/go/bin"
export PATH="$PATH:$HOME/go/bin"
go install github.com/bazelbuild/buildtools/buildozer@latest

sudo apt-get install umoci
sudo apt-get install net-tools
sudo ./tools/enable_local_firecracker.sh

bazel --nosystem_rc test //enterprise/server/remote_execution/containers/firecracker:firecracker_test_blockio --test_tag_filters=+bare --build_tag_filters=+bare --test_output=streamed --test_filter=TestFirecrackerSnapshotAndResume --test_arg=-executor.firecracker_debug_stream_vm_logs

wget -c https://kernel.ubuntu.com/~kernel-ppa/mainline/v5.11/amd64/linux-headers-5.11.0-051100-generic_5.11.0-051100.202102142330_amd64.deb
wget -c https://kernel.ubuntu.com/~kernel-ppa/mainline/v5.11/amd64/linux-modules-5.11.0-051100-generic_5.11.0-051100.202102142330_amd64.deb
wget -c https://kernel.ubuntu.com/~kernel-ppa/mainline/v5.11/amd64/linux-headers-5.11.0-051100_5.11.0-051100.202102142330_all.deb
wget -c https://kernel.ubuntu.com/~kernel-ppa/mainline/v5.11/amd64/linux-image-unsigned-5.11.0-051100-generic_5.11.0-051100.202102142330_amd64.deb
sudo dpkg -i *.deb
