#!/usr/bin/env bash
set -ex

sudo snap install go --classic
sudo go install github.com/mattn/goreman@latest
curl -fsSL install.buildbuddy.io | bash
sudo apt install acl
sudo snap install emacs --classic
sudo apt-get update
sudo apt install build-essential

[ $(stat -c "%G" /dev/kvm) = kvm ] && sudo usermod -aG kvm ${USER} && echo "Access granted."
sudo setfacl -m u:${USER}:rw /dev/kvm
mkdir firecracker
cd firecracker
ARCH="$(uname -m)"
wget https://s3.amazonaws.com/spec.ccfc.min/img/quickstart_guide/${ARCH}/kernels/vmlinux.bin
wget https://s3.amazonaws.com/spec.ccfc.min/ci-artifacts/disks/${ARCH}/ubuntu-18.04.ext4
wget https://s3.amazonaws.com/spec.ccfc.min/ci-artifacts/disks/${ARCH}/ubuntu-18.04.id_rsa
chmod 400 ./ubuntu-18.04.id_rsa
git clone https://github.com/firecracker-microvm/firecracker

sudo apt-get install ca-certificates curl gnupg
sudo install -m 0755 -d /etc/apt/keyrings
curl -fsSL https://download.docker.com/linux/ubuntu/gpg | sudo gpg --dearmor -o /etc/apt/keyrings/docker.gpg
sudo chmod a+r /etc/apt/keyrings/docker.gpg

echo   "deb [arch="$(dpkg --print-architecture)" signed-by=/etc/apt/keyrings/docker.gpg] https://download.docker.com/linux/ubuntu \
  "$(. /etc/os-release && echo "$VERSION_CODENAME")" stable" |   sudo tee /etc/apt/sources.list.d/docker.list > /dev/null
sudo apt-get update
sudo apt-get install docker-ce docker-ce-cli containerd.io docker-buildx-plugin docker-compose-plugin
sudo systemctl start docker
sudo ./firecracker/tools/devtool build

PATH=$PATH:~/go/bin

cd ..

sudo apt install zlib1g-dev
sudo apt install libgpgme-dev libassuan-dev libbtrfs-dev libdevmapper-dev pkg-config

GOPATH=/home/$USER/go
git clone https://github.com/containers/skopeo $GOPATH/src/github.com/containers/skopeo

cd $GOPATH/src/github.com/containers/skopeo && make bin/skopeo
cp skopeo ~/go/bin/

sudo apt-get install redis
sudo apt install acl
sudo apt install net-tools
sudo apt-get install umoci

sudo cp /home/jim/firecracker/firecracker/build/cargo_target/x86_64-unknown-linux-musl/debug/jailer /usr/sbin
sudo cp /home/jim/firecracker/firecracker/build/cargo_target/x86_64-unknown-linux-musl/debug/firecracker /usr/sbin

sudo tools/enable_local_firecracker.sh

# Update /etc/containers/policy.json
# Update path.
echo "PATH=$PATH:~/go/bin" >> ~/.bashrc 
echo 'Update /etc/containers/policy.json!'
echo 'Update Procfile.rexec to use firecracker!'
echo 'Then run goreman -f tools/goreman/procfiles/Procfile.rexec start'
