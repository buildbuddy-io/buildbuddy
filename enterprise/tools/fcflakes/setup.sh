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

sudo apt-get install ca-certificates curl gnupg
sudo install -m 0755 -d /etc/apt/keyrings
curl -fsSL https://download.docker.com/linux/ubuntu/gpg | sudo gpg --dearmor -o /etc/apt/keyrings/docker.gpg
sudo chmod a+r /etc/apt/keyrings/docker.gpg

echo   "deb [arch="$(dpkg --print-architecture)" signed-by=/etc/apt/keyrings/docker.gpg] https://download.docker.com/linux/ubuntu \
  "$(. /etc/os-release && echo "$VERSION_CODENAME")" stable" |   sudo tee /etc/apt/sources.list.d/docker.list > /dev/null
sudo apt-get update
sudo apt-get install docker-ce docker-ce-cli containerd.io docker-buildx-plugin docker-compose-plugin
sudo systemctl start docker

curl -L https://github.com/firecracker-microvm/firecracker/releases/download/v1.4.0/firecracker-v1.4.0-x86_64.tgz --output firecracker.tgz
tar xvzf firecracker.tgz
sudo cp ./release-v1.4.0-x86_64/firecracker-v1.4.0-x86_64 /usr/sbin/firecracker
sudo cp ./release-v1.4.0-x86_64/jailer-v1.4.0-x86_64 /usr/sbin/jailer

PATH=$PATH:~/go/bin

sudo apt install zlib1g-dev
sudo apt install libgpgme-dev libassuan-dev libbtrfs-dev libdevmapper-dev pkg-config

GOPATH=/home/$USER/go
git clone https://github.com/containers/skopeo $GOPATH/src/github.com/containers/skopeo

cd $GOPATH/src/github.com/containers/skopeo && make bin/skopeo
cp bin/skopeo ~/go/bin/

sudo apt-get install redis
sudo apt install acl
sudo apt install net-tools
sudo apt-get install umoci

sudo tools/enable_local_firecracker.sh

# Update /etc/containers/policy.json
# Update path.
echo "PATH=$PATH:~/go/bin" >> ~/.bashrc 
echo 'Update /etc/containers/policy.json!'
echo 'Update Procfile.rexec to use firecracker!'
echo 'Then run goreman -f tools/goreman/procfiles/Procfile.rexec start'
