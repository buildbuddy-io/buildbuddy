#!/bin/bash
# Host-side egress filter for the dev VM. Runs on the HOST as root (invoked by
# `vmctl` via sudo). The VM's only route out is the host; this:
#   * runs a dedicated nginx SNI-allowlist proxy (nginx-sni-proxy.conf),
#   * transparently REDIRECTs the VM subnet's :443 into it (PREROUTING),
#   * default-denies everything else from the VM,
# and the guest -- however privileged inside -- cannot touch any of it.
#
# SURGICAL: dedicated BBVM_* chains, never flush the host's built-in chains
# (Docker/libvirt rules must survive). Idempotent; safe to re-run.
set -euo pipefail
IFS=$'\n\t'

VM_BR="${VM_BR:-bbvmbr0}"
VM_SUBNET="${VM_SUBNET:-192.168.71.0/24}"
PROXY_PORT="${PROXY_PORT:-8443}"
SRC_CONF="$(dirname "$(readlink -f "$0")")/nginx-sni-proxy.conf"
RUN_CONF="/etc/nginx/bbvm-sni.conf"
LOG_DIR="/var/log/bbvm-sni"

[ "$(id -u)" -eq 0 ] || { echo "host-egress.sh must run as root" >&2; exit 1; }

# --teardown: remove our chains/jumps and stop the dedicated nginx. Never
# touches the host's built-in chains or other (Docker/libvirt) rules.
if [ "${1:-}" = "--teardown" ]; then
    iptables -t nat -D PREROUTING -i "$VM_BR" -s "$VM_SUBNET" -j BBVM_PRE 2>/dev/null || true
    iptables -D INPUT   -i "$VM_BR" -j BBVM_IN  2>/dev/null || true
    iptables -D FORWARD -i "$VM_BR" -j BBVM_FWD 2>/dev/null || true
    for c in BBVM_PRE; do iptables -t nat -F "$c" 2>/dev/null || true; iptables -t nat -X "$c" 2>/dev/null || true; done
    for c in BBVM_IN BBVM_FWD; do iptables -F "$c" 2>/dev/null || true; iptables -X "$c" 2>/dev/null || true; done
    [ -f /run/bbvm-sni.pid ] && kill "$(cat /run/bbvm-sni.pid)" 2>/dev/null || true
    echo "[host-egress] torn down"
    exit 0
fi

# --- 1. deps ---------------------------------------------------------------
if ! dpkg -s nginx >/dev/null 2>&1 || ! dpkg -s libnginx-mod-stream >/dev/null 2>&1; then
    echo "[host-egress] installing nginx + libnginx-mod-stream..."
    apt-get update -qq && apt-get install -y --no-install-recommends nginx libnginx-mod-stream
fi
mkdir -p "$LOG_DIR"

# --- 2. render + (re)start the dedicated nginx instance --------------------
resolver="$(awk '/^nameserver/ {print $2; exit}' /etc/resolv.conf)"
[ -z "${resolver:-}" ] && resolver="127.0.0.53"   # systemd-resolved stub
sed "s|__RESOLVER__|${resolver}|" "$SRC_CONF" > "$RUN_CONF"
nginx -t -c "$RUN_CONF"
if [ -f /run/bbvm-sni.pid ] && kill -0 "$(cat /run/bbvm-sni.pid)" 2>/dev/null; then
    nginx -c "$RUN_CONF" -s reload
else
    nginx -c "$RUN_CONF"
fi
echo "[host-egress] SNI proxy on 127.0.0.1:${PROXY_PORT} (resolver ${resolver})"

# --- 3. kernel knobs (host /proc/sys is writable, unlike the container) ----
sysctl -wq net.ipv4.ip_forward=1
sysctl -wq net.ipv4.conf.all.route_localnet=1
# PREROUTING REDIRECT to 127.0.0.1 needs route_localnet on the ingress iface.
[ -d "/proc/sys/net/ipv4/conf/${VM_BR}" ] && \
    sysctl -wq "net.ipv4.conf.${VM_BR}.route_localnet=1" || \
    echo "[host-egress] WARN: ${VM_BR} not present yet (start the libvirt net first)"

# --- 4. iptables (own chains, never flush built-ins) ----------------------
ensure_jump() {  # table chain matchspec... targetchain
    local t="$1" c="$2"; shift 2; local tgt="${*: -1}"; set -- "${@:1:$#-1}"
    iptables -t "$t" -C "$c" "$@" -j "$tgt" 2>/dev/null \
        || iptables -t "$t" -I "$c" 1 "$@" -j "$tgt"
}

# NOTE: this is PREROUTING (forwarded VM traffic), NOT OUTPUT. `REDIRECT`
# here would rewrite the dest to the inbound iface IP ($VM_BR's address),
# where the loopback-bound nginx is NOT listening. DNAT to 127.0.0.1 is the
# correct primitive; it works because route_localnet=1 is set on $VM_BR
# (set in step 3) -- the exact PREROUTING/route_localnet case.
iptables -t nat -N BBVM_PRE 2>/dev/null || iptables -t nat -F BBVM_PRE
iptables -t nat -A BBVM_PRE -s "$VM_SUBNET" -p tcp --dport 443 \
    -j DNAT --to-destination "127.0.0.1:${PROXY_PORT}"
ensure_jump nat PREROUTING -i "$VM_BR" -s "$VM_SUBNET" BBVM_PRE

iptables -N BBVM_IN 2>/dev/null || iptables -F BBVM_IN
iptables -A BBVM_IN -m state --state ESTABLISHED,RELATED -j ACCEPT
iptables -A BBVM_IN -p udp --dport 67 -j ACCEPT          # DHCP
iptables -A BBVM_IN -p udp --dport 53 -j ACCEPT          # DNS (libvirt dnsmasq)
iptables -A BBVM_IN -p tcp --dport 53 -j ACCEPT
iptables -A BBVM_IN -d 127.0.0.0/8 -j ACCEPT             # REDIRECT'd :443 -> nginx
iptables -A BBVM_IN -j DROP                              # nothing else from the VM
ensure_jump filter INPUT -i "$VM_BR" BBVM_IN

iptables -N BBVM_FWD 2>/dev/null || iptables -F BBVM_FWD
# The VM has NO legitimate forwarded path (nginx is host-local). Deny all
# VM-initiated forwarding -> no general internet, no exfil bypass.
iptables -A BBVM_FWD -j DROP
ensure_jump filter FORWARD -i "$VM_BR" BBVM_FWD

# --- 5. fail-closed self-test (host invariants) ---------------------------
fail() { echo "[host-egress] ERROR: $1" >&2; exit 1; }
pgrep -x nginx >/dev/null || fail "nginx (SNI proxy) is not running"
iptables -t nat -C PREROUTING -i "$VM_BR" -s "$VM_SUBNET" -j BBVM_PRE 2>/dev/null \
    || fail "PREROUTING redirect for ${VM_BR} not installed"
[ "$(cat /proc/sys/net/ipv4/ip_forward)" = 1 ] || fail "ip_forward not enabled"
echo "[host-egress] active: VM ${VM_SUBNET} egress = SNI allowlist only."
echo "[host-egress] (end-to-end allow/deny is validated from inside the VM)"
