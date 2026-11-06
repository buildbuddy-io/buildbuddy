import React from "react";
import { config } from "../lib/config";
import router, { paths } from "../lib/router";

type Instance = { name: string; url: string };

/**
 * Names the cluster this atlas indexes and, when other instances are
 * configured, switches to one of them.
 */
export function ClusterPicker() {
  const current = config.clusterName;
  const instances: Instance[] = config.clusterLinks.map((c) => ({ name: c.name ?? "", url: c.url ?? "" }));
  if (current && !instances.some((i) => i.name === current)) {
    instances.unshift({ name: current, url: "" });
  }
  if (instances.length < 2) {
    return current ? <span className="atlas-cluster-name">{current}</span> : null;
  }
  return (
    <select
      className="atlas-cluster-picker"
      value={current}
      title="Switch to another cluster's atlas"
      onChange={(e) => switchTo(instances.find((i) => i.name === e.target.value))}>
      {instances.map((i) => (
        <option key={i.name} value={i.name}>
          {i.name}
        </option>
      ))}
    </select>
  );
}

/**
 * Opens the same view on another instance. An object page has no twin over
 * there, so it becomes a search for the object's name.
 */
function switchTo(target?: Instance) {
  if (!target?.url || target.name === config.clusterName) return;
  const route = router.current();
  const path =
    route.kind === "object" ? paths.search(route.ref.name) : window.location.pathname + window.location.search;
  window.location.href = target.url.replace(/\/+$/, "") + path;
}

export default ClusterPicker;
