#!/bin/bash
set -euo pipefail

# enable_local_firecracker.sh configures a normal linux system to allow the
# user to run firecracker (via the jailer), without being root.

# this script must be run as root, so check that first.
if [ $(id -u) -ne 0 ]; then
    echo "This script must be run as root (use sudo)."
    echo "sudo $0"
    exit 1
fi

if ! command -v jailer &> /dev/null
then
    echo "jailer could not be found (install firecracker + jailer?)"
    exit 1
fi

if ! command -v ip &> /dev/null
then
    echo "ip could not be found (install iproute2?)"
    exit 1
fi

if ! command -v iptables &> /dev/null
then
    echo "iptables could not be found"
    exit 1
fi

# jailer will create stuff here; ensure the dir exists and owner is user.
mkdir -p /sys/fs/cgroup/cpuset/firecracker
chown -R $SUDO_USER:$SUDO_USER /sys/fs/cgroup/cpuset/firecracker

# enable IP forwarding.
echo 1 > /proc/sys/net/ipv4/ip_forward

PRIMARY_DEVICE=$(route | grep default | awk '{print $8}')
iptables -t nat -A POSTROUTING -o $PRIMARY_DEVICE -j MASQUERADE
iptables -A FORWARD -m conntrack --ctstate RELATED,ESTABLISHED -j ACCEPT

# allow the jailer to run without root by setting capabilities on the binary.
JAILER_PATH=$(which jailer)
JAILER_PERMS="$(getcap $JAILER_PATH | awk '{print $3}')"
if [ "$JAILER_PERMS" != "cap_net_admin,cap_sys_admin,cap_mknod+eip" ]; then
    echo "Running setcap CAP_MKNOD,CAP_SYS_ADMIN,CAP_NET_ADMIN+eip $JAILER_PATH"
    setcap CAP_MKNOD,CAP_SYS_ADMIN,CAP_NET_ADMIN+eip $JAILER_PATH
fi

# Add "ip" to the sudoers file with NOPASSWD.
IP_ENTRY="$SUDO_USER ALL = (root) NOPASSWD: /sbin/ip"
FOUND_ENTRY=$(grep "$IP_ENTRY" /etc/sudoers || true)

if [ "$FOUND_ENTRY" != "$IP_ENTRY" ]; then
    echo "Adding \"$IP_ENTRY\" to /etc/sudoers"
    echo "$IP_ENTRY" | EDITOR='tee -a' visudo >/dev/null
fi

echo "All done! You should be ready to run the executor as your user now."
echo "You will need to run this program again if you restart or update the jailer binary"
