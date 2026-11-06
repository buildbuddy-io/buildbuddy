import React from "react";
import { Tone, healthTone } from "../lib/format";
import { atlas } from "../../../../proto/atlas_ts_proto";

export type BadgeProps = {
  tone?: Tone | "cluster" | "ns";
  className?: string;
  children: React.ReactNode;
};

export function Badge({ tone = "", className = "", children }: BadgeProps) {
  return <span className={`atlas-badge ${tone} ${className}`}>{children}</span>;
}

export function ClusterBadge({ name }: { name: string }) {
  return <Badge tone="cluster">{name}</Badge>;
}

export function NamespaceBadge({ name }: { name?: string | null }) {
  return name ? <Badge tone="ns">{name}</Badge> : null;
}

export function PhaseBadge({ entry }: { entry: atlas.IEntry }) {
  return entry.phase ? <Badge tone={healthTone(entry.health)}>{entry.phase}</Badge> : null;
}

export function ReadyBadge({ entry }: { entry: atlas.IEntry }) {
  return entry.ready ? <Badge tone={healthTone(entry.health)}>{entry.ready}</Badge> : null;
}
