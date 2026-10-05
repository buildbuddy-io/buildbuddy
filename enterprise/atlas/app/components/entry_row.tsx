import React from "react";
import { entryContext } from "../lib/format";
import { paths } from "../lib/router";
import { atlas } from "../../../../proto/atlas_ts_proto";
import { ClusterBadge, NamespaceBadge, PhaseBadge, ReadyBadge } from "./badges";
import CopyButton from "./copy_button";
import Link from "./link";

export type EntryRowProps = {
  entry: atlas.Entry;
  /** Tunnel links for the entry's ports, from the search result. */
  links: atlas.IPortLink[];
  /** Highlighted by keyboard navigation; scrolls into view when it becomes selected. */
  selected?: boolean;
};

/**
 * One search result: name, where it lives, its state, some context, and a
 * line of chips beneath it, one per port a browser can open through the tunnel.
 */
export function EntryRow({ entry, links, selected }: EntryRowProps) {
  const ref = React.useRef<HTMLDivElement>(null);
  React.useEffect(() => {
    if (selected) ref.current?.scrollIntoView({ block: "nearest" });
  }, [selected]);
  const reachable = links.filter((l) => l.hostPort);
  const ports = reachable.filter((l) => !l.path);
  const pages = reachable.filter((l) => l.path);
  return (
    <div ref={ref} className={`atlas-row ${selected ? "selected" : ""}`}>
      <Link to={paths.object(entry)} className="atlas-row-main">
        <span className="atlas-row-name">{entry.name}</span>
        <ClusterBadge name={entry.cluster} />
        <NamespaceBadge name={entry.namespace} />
        <PhaseBadge entry={entry} />
        <ReadyBadge entry={entry} />
        <span className="atlas-row-context">{entryContext(entry)}</span>
      </Link>
      <PortChips links={ports} />
      <PortChips links={pages} />
    </div>
  );
}

/** One line of [name | copy] chips; ports and pages get a line each. */
function PortChips({ links }: { links: atlas.IPortLink[] }) {
  if (!links.length) return null;
  return (
    <span className="atlas-row-links">
      {links.map((l) => (
        <span key={`${l.name}:${l.port}:${l.path}`} className={`atlas-port-chip ${l.path ? "page" : ""}`}>
          {l.url ? (
            <a href={l.url ?? ""} target="_blank" rel="noopener" className="atlas-port-chip-name" title={l.url ?? ""}>
              {l.name || l.port}
            </a>
          ) : (
            <span className="atlas-port-chip-name" title={l.hostPort ?? ""}>
              {l.name || l.port}
            </span>
          )}
          <CopyButton text={(l.path ? l.url : l.hostPort) ?? ""} compact className="atlas-port-chip-copy" />
        </span>
      ))}
    </span>
  );
}

export default EntryRow;
