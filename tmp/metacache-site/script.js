// Sidebar search filter
document.addEventListener("DOMContentLoaded", () => {
  const search = document.querySelector(".search-box");
  if (search) {
    search.addEventListener("input", (e) => {
      const q = e.target.value.toLowerCase().trim();
      document.querySelectorAll(".nav-link").forEach((el) => {
        const txt = el.textContent.toLowerCase();
        el.style.display = !q || txt.includes(q) ? "" : "none";
      });
      document.querySelectorAll(".nav-section").forEach((sec) => {
        // hide section if all its links hidden
        const next = sec.nextElementSibling;
        // do nothing; sections always visible
      });
    });
  }

  // Set active nav link from current path
  const path = location.pathname.split("/").pop() || "index.html";
  document.querySelectorAll(".nav-link").forEach((el) => {
    if (el.getAttribute("href") === path) {
      el.classList.add("active");
    }
  });

  // Tabs
  document.querySelectorAll(".tabs").forEach((tabbar) => {
    const tabs = tabbar.querySelectorAll(".tab");
    const panels = tabbar.parentElement.querySelectorAll(":scope > .tab-panel");
    tabs.forEach((tab, i) => {
      tab.addEventListener("click", () => {
        tabs.forEach((t) => t.classList.remove("active"));
        panels.forEach((p) => p.classList.remove("active"));
        tab.classList.add("active");
        if (panels[i]) panels[i].classList.add("active");
      });
    });
    if (tabs.length > 0 && !tabbar.parentElement.querySelector(".tab.active")) {
      tabs[0].classList.add("active");
      if (panels[0]) panels[0].classList.add("active");
    }
  });

  // Clickable architecture nodes — navigate to the target on click
  document.querySelectorAll(".arch-node[data-href]").forEach((n) => {
    n.addEventListener("click", () => {
      location.href = n.getAttribute("data-href");
    });
  });

  // Highlight architecture node on hover and show details panel if present
  const detailPanel = document.getElementById("arch-detail");
  if (detailPanel) {
    const archDetails = {
      "metacache": {
        title: "MetaCache",
        path: "enterprise/server/backends/metacache/metacache.go",
        text: "The cache.Cache implementation: speaks Bazel cache semantics. Splits each write into a metadata RPC (to the raft cluster) plus a blob write (inline if small, GCS if large)."
      },
      "metadata-svc": {
        title: "MetadataService (gRPC)",
        path: "enterprise/server/raft/metadata/metadata.go",
        text: "Public gRPC server exposing Get/Set/Find/Delete. Authorizes, computes pebble keys, fans out via Sender.RunMultiKey."
      },
      "sender": {
        title: "Sender",
        path: "enterprise/server/raft/sender/sender.go",
        text: "Range router. Looks up which raft group owns a key, tries replicas in preference order, retries on stale routing."
      },
      "store": {
        title: "Store",
        path: "enterprise/server/raft/store/store.go",
        text: "Per-process orchestrator: owns the dragonboat NodeHost, the local Pebble DB, replicas, leasekeeper, driver, txn coordinator, gRPC server."
      },
      "replica": {
        title: "Replica (IOnDiskStateMachine)",
        path: "enterprise/server/raft/replica/replica.go",
        text: "Dragonboat state machine for one (range, replica). Update applies committed batches to Pebble; snapshots stream KVs over the wire."
      },
      "dragonboat": {
        title: "Dragonboat NodeHost",
        path: "github.com/lni/dragonboat/v4",
        text: "External multi-Raft library: leader election, log replication, snapshot framework, membership changes, multiplexed transport."
      },
      "pebble": {
        title: "PebbleDB",
        path: "shared single instance per Store",
        text: "CockroachDB's LSM KV. One DB per node, multiplexed across all (range, replica) pairs via byte-prefix scoping (\\x01c<rid>n<rep>-)."
      },
      "gcs": {
        title: "Google Cloud Storage",
        path: "external blob backend",
        text: "Where the actual blob bytes live (when not inlined). Objects keyed by /{app}/{partition}/{group}/{cas|ac}/{prefix}/{hash}-{salt}, lifecycle-managed via DaysSinceCustomTime."
      },
      "leasekeeper": {
        title: "LeaseKeeper",
        path: "enterprise/server/raft/leasekeeper/leasekeeper.go",
        text: "Per-Store. Spawns a leaseAgent per range; reacts to raft leader-changes and node-liveness epoch changes to acquire/drop range leases."
      },
      "driver": {
        title: "Placement Driver",
        path: "enterprise/server/raft/driver/driver.go",
        text: "Priority-queued rebalancer. Detects under/over-replication, dead replicas, oversized ranges; schedules adds/removes/splits/lease transfers."
      },
      "gossip": {
        title: "Serf Gossip",
        path: "github.com/hashicorp/serf",
        text: "Used for: meta-range descriptor propagation, NHID→address discovery (Registry), per-node usage tags (StoreUsageTag), bringup coordination."
      },
      "cache-client": {
        title: "Cache client",
        path: "Bazel via grpc, or other internal consumer",
        text: "The user-facing layer that calls into interfaces.Cache (Get/Set/Reader/Writer). Sees the MetaCache as just another cache backend."
      },
      "registry": {
        title: "Node Registry",
        path: "enterprise/server/raft/registry/registry.go",
        text: "Bridges dragonboat's INodeRegistry to gossip. Maps (range_id, replica_id) → NHID, and NHID → (raft addr, grpc addr)."
      }
    };
    document.querySelectorAll(".arch-node[data-key]").forEach((n) => {
      n.addEventListener("mouseenter", () => {
        const k = n.getAttribute("data-key");
        const d = archDetails[k];
        if (d) {
          detailPanel.innerHTML = `<div class="callout-title">${d.title}</div><div class="mono" style="color:var(--text-muted);font-size:12px;margin-bottom:8px">${d.path}</div><div>${d.text}</div>`;
          detailPanel.style.display = "block";
        }
      });
    });
  }

  // Smooth-scroll for anchor links
  document.querySelectorAll('a[href^="#"]').forEach((a) => {
    a.addEventListener("click", (e) => {
      const id = a.getAttribute("href").slice(1);
      const el = document.getElementById(id);
      if (el) {
        e.preventDefault();
        el.scrollIntoView({ behavior: "smooth", block: "start" });
        history.pushState(null, "", "#" + id);
      }
    });
  });
});
