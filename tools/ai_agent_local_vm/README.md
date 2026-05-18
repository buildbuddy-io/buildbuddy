# BuildBuddy dev VM (`tools/ai_agent_local_vm/`)

A libvirt/QEMU **KVM VM** used as an SSH/Remote dev host, for work that needs
real privileges the hardened container can't safely give — the BuildBuddy
executor's **OCI runtime**, nested containers, even Firecracker (host nested
virt is on).

## Why a VM

Inside the VM you can be fully privileged (root, `sudo`, `SYS_ADMIN`, nested
KVM) **safely**, because the trust boundary is the hypervisor. The egress
allowlist runs on the **host at the VM's network edge** (`tools/ai_agent_local_vm/egress/`), so
the guest — however privileged — **cannot tamper with or bypass it**. This
dissolves the container-era tension (privileged-for-OCI vs. tamper-proof
egress) that `network_mode: service:proxy` could never fully resolve.

## Layout

| Path | Role |
|------|------|
| `vmctl` | lifecycle wrapper (see below) |
| `cloud-init/` | first-boot provisioning (user `aiagent`, toolchain, OCI runtime) |
| `libvirt/network.xml` | isolated net; no NAT; host is the only way out |
| `libvirt/domain.xml.tmpl` | the VM (host-passthrough CPU, virtiofs repo share) |
| `egress/nginx-sni-proxy.conf` | host SNI-allowlist proxy (mirror of the container's) |
| `egress/host-egress.sh` | host iptables: REDIRECT VM :443 → proxy, deny the rest |

## Usage

```bash
tools/ai_agent_local_vm/vmctl setup     # define net+domain, apply egress; reuses disk+seed if
                    # present (no re-provision). --fresh forces a clean disk.
tools/ai_agent_local_vm/vmctl up        # start VM + ensure egress; waits for it to report an IP
tools/ai_agent_local_vm/vmctl ssh       # shell in as aiagent (first boot: cloud-init takes a few min)
tools/ai_agent_local_vm/vmctl status
tools/ai_agent_local_vm/vmctl down      # graceful shutdown (keeps disk/state)
tools/ai_agent_local_vm/vmctl destroy   # tear down domain + egress; KEEPS disk+seed and network.
                    #   --purge-disk : also delete disk+seed (re-provision)
                    #   --net        : also remove the libvirt network
tools/ai_agent_local_vm/vmctl egress apply|status   # re-apply / inspect the host filter
```

Tunables via env: `VCPUS`, `MEM_GIB`, `DISK_GIB`, `IMG_DIR`, `VM_NAME`.

**Change CPU/RAM without re-provisioning** (keeps the disk):

```bash
tools/ai_agent_local_vm/vmctl destroy                       # disk+seed preserved
MEM_GIB=64 VCPUS=24 tools/ai_agent_local_vm/vmctl setup     # re-defines the domain w/ new specs
tools/ai_agent_local_vm/vmctl up                            # boots the SAME disk, new specs
```

This only works because the domain uses a **fixed NIC MAC**
(`52:54:00:bb:bb:01` in `libvirt/domain.xml.tmpl`): cloud-init's netplan
pins the interface by MAC, so a re-defined domain must keep the same MAC or
the reused disk loses all networking. A disk provisioned *before* the fixed
MAC was introduced won't match it — adopt it once with a fresh re-provision:
`tools/ai_agent_local_vm/vmctl destroy --purge-disk && tools/ai_agent_local_vm/vmctl setup && tools/ai_agent_local_vm/vmctl up`.

The repo is shared into the VM at `/workspaces/buildbuddy` via virtiofs. 
Bazel cache and `~/.claude` live on the VM disk — persisted across `down`/`up`, wiped by `destroy`.

## Egress model & allowlist

Same SNI-preview design as the container: no TLS termination (HTTP/2 + gRPC
pass through), hostname allowlist, default-deny. Differences:

- Enforced on the **host** for the VM subnet (PREROUTING REDIRECT — the
  `route_localnet` case, vs. the container's OUTPUT case).
- The allowlist in `egress/nginx-sni-proxy.conf` is a **manual copy** of
  `.devcontainer/nginx-sni-proxy.conf` plus the VM's first-boot provisioning
  hosts (Ubuntu archive/security, NodeSource). **Keep the two in sync** when
  you add hosts. apt is forced to HTTPS so it traverses the proxy.

To change the allowlist: edit `egress/nginx-sni-proxy.conf`, then
`tools/ai_agent_local_vm/vmctl egress apply` (no VM teardown needed).

