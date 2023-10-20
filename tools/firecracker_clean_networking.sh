#!/bin/bash
set -eu

if [[ "$(id -u)" != 0 ]]; then
  echo >&2 "$0: error: must be run as root"
  exit 1
fi

echo >&2 "Cleaning up network namespaces"
ip netns list | awk '{ print $1 }' | while read -r netns; do
  [[ "$netns" == bb-executor-* ]] && (
    set -x
    ip netns delete "$netns"
  )
done

IPTABLES=$(iptables-save)
BACKUP_FILE=$(mktemp --suffix .iptables.bak)
cat <<<"$IPTABLES" >"$BACKUP_FILE"
echo >&2 "Created iptables backup at $BACKUP_FILE"
echo >&2 "Restore backup with: sudo iptables-restore < $BACKUP_FILE"

echo >&2 "Cleaning up iptables veth* rules"
grep -vP '\bveth' <<<"$IPTABLES" | uniq | iptables-restore

echo >&2 "Cleaning up any remaining firecracker processes"
killall firecracker
