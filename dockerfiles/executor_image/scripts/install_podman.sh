# Install Podman 4
# Install Podman dependencies
apt-get update && apt-get install -y \
  btrfs-progs git go-md2man iptables libassuan-dev libbtrfs-dev libc6-dev libdevmapper-dev libglib2.0-dev libgpgme-dev libgpg-error-dev libprotobuf-dev libprotobuf-c-dev libseccomp-dev libselinux1-dev libsystemd-dev pkg-config uidmap make build-essential

# Install golang-go. Requires 1.16+
curl -fsSL "https://go.dev/dl/go1.17.8.linux-amd64.tar.gz" | tar xz -C /usr/local/
OLD_PATH="${PATH}"
export PATH="/usr/local/go/bin:${PATH}"

# Install runc
curl -OL "https://github.com/opencontainers/runc/releases/download/v1.1.0/runc.amd64"  
chmod a+x runc.amd64 && mv runc.amd64 /usr/bin/runc

# Install podman
curl -fsSL "https://github.com/containers/podman/archive/refs/tags/v4.0.2.tar.gz" | tar xz 
cd podman-4.0.2 && make BUILDTAGS="selinux seccomp" && make install PREFIX=/usr

# Install conmon
curl -fsSL "https://github.com/containers/conmon/archive/refs/tags/v2.1.0.tar.gz" | tar xz
cd conmon-2.1.0 && make && make podman

# Install cni-plugins
mkdir -p /opt/cni/bin && curl -fsSL "https://github.com/containernetworking/plugins/releases/download/v1.1.1/cni-plugins-linux-amd64-v1.1.1.tgz" | tar xz -C /opt/cni/bin/

# Add configuration
mkdir -p /etc/containers 
curl -L -o /etc/containers/registries.conf "https://src.fedoraproject.org/rpms/containers-common/raw/main/f/registries.conf"  
curl -L -o /etc/containers/policy.json "https://src.fedoraproject.org/rpms/containers-common/raw/main/f/default-policy.json"

# Clean up
apt-get remove -y make btrfs-progs git go-md2man libassuan-dev libbtrfs-dev libc6-dev libdevmapper-dev libglib2.0-dev libgpgme-dev libgpg-error-dev libprotobuf-dev libprotobuf-c-dev libseccomp-dev libselinux1-dev libsystemd-dev pkg-config uidmap make build-essential
apt-get autoremove -y
apt-get install -y libgpgme11
rm -rf podman-4.0.2 && rm -rf conmon-2.1.0 && rm -rf /usr/local/go/
export PATH=${OLD_PATH}
