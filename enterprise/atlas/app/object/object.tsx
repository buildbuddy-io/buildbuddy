import React from "react";
import Spinner from "../../../../app/components/spinner/spinner";
import { BuildBuddyError } from "../../../../app/util/errors";
import { atlas } from "../../../../proto/atlas_ts_proto";
import { Badge, ClusterBadge, NamespaceBadge, PhaseBadge, ReadyBadge } from "../components/badges";
import CopyButton from "../components/copy_button";
import Link from "../components/link";
import { age, toNumber } from "../lib/format";
import { ObjectRef, paths } from "../lib/router";
import rpcService from "../lib/rpc_service";
import LogsComponent from "./logs";

interface Props {
  objectRef: ObjectRef;
}

interface State {
  response?: atlas.GetObjectResponse;
  errorMessage?: string;
}

/** The detail page for one object: live YAML plus everything the index knows around it. */
export default class ObjectComponent extends React.Component<Props, State> {
  state: State = {};

  componentDidMount() {
    this.fetch();
  }

  componentDidUpdate(prevProps: Props) {
    if (paths.object(prevProps.objectRef) !== paths.object(this.props.objectRef)) {
      this.setState({ response: undefined, errorMessage: undefined });
      this.fetch();
    }
  }

  private fetch() {
    const ref = this.props.objectRef;
    rpcService.service
      .getObject(new atlas.GetObjectRequest(ref))
      .then((response) => this.setState({ response }))
      .catch((e) => this.setState({ errorMessage: BuildBuddyError.parse(e).description }));
  }

  render() {
    if (this.state.errorMessage) {
      return <div className="atlas-error-panel">{this.state.errorMessage}</div>;
    }
    const response = this.state.response;
    if (!response) {
      return <Spinner className="atlas-spinner" />;
    }
    const ref = this.props.objectRef;
    // Objects the index does not summarize still have a page: the live YAML.
    const entry =
      response.entry ??
      new atlas.Entry({ cluster: ref.cluster, namespace: ref.namespace, name: ref.name, kind: response.kind });
    const rel = response.relations ?? new atlas.Relations();
    const restarts = toNumber(entry.restarts);
    const info: [string, string][] = Object.entries(entry.extra);
    if (entry.images.length) info.push(["images", entry.images.join("  ")]);
    if (entry.ips.length) info.push(["ip", entry.ips.join("  ")]);
    const related = [
      ...rel.owners.map((o) => ({ prefix: `${o.kind}/`, entry: o })),
      ...(rel.node ? [{ prefix: "Node/", entry: rel.node }] : []),
      ...rel.services.map((s) => ({ prefix: "Service/", entry: s })),
      ...rel.jobs.map((j) => ({ prefix: "Job/", entry: j })),
    ];

    return (
      <div className="atlas-detail">
        <div className="atlas-crumb">
          {response.kind || entry.kind} · <ClusterBadge name={entry.cluster} />{" "}
          <NamespaceBadge name={entry.namespace} />
        </div>
        <h1>
          {entry.name} <PhaseBadge entry={entry} />
          <ReadyBadge entry={entry} />
        </h1>
        <div className="atlas-meta-line">
          {entry.created && `created ${age(entry.created)} ago`}
          {restarts > 0 && ` · ${restarts} restarts`}
          {entry.node && (
            <>
              {" "}
              · on <b>{entry.node}</b>
            </>
          )}
        </div>

        <PortsSection key={paths.object(ref)} ports={response.ports} />

        {related.length > 0 && (
          <div className="atlas-section">
            <h3>Related</h3>
            <div className="atlas-chips">
              {related.map(({ prefix, entry: e }) => (
                <Link key={prefix + e.name} to={paths.object(e)} className="atlas-chip">
                  <span className="atlas-chip-key">{prefix}</span>
                  {e.name}
                </Link>
              ))}
            </div>
          </div>
        )}

        <PodsSection relations={rel} />

        {Object.keys(entry.labels).length > 0 && (
          <div className="atlas-section">
            <h3>Labels</h3>
            <div className="atlas-chips">
              {Object.entries(entry.labels)
                .sort(([a], [b]) => a.localeCompare(b))
                .map(([k, v]) => (
                  <Link key={k} to={paths.search(`label:${k}=${v}`)} className="atlas-chip">
                    <span className="atlas-chip-key">{k}=</span>
                    {v}
                  </Link>
                ))}
            </div>
          </div>
        )}

        {info.length > 0 && (
          <div className="atlas-section">
            <h3>Info</h3>
            <div className="atlas-chips">
              {info.map(([k, v]) => (
                <span key={k} className="atlas-chip">
                  <span className="atlas-chip-key">{k}: </span>
                  {v}
                </span>
              ))}
            </div>
          </div>
        )}

        {entry.kind === "Pod" && response.entry && <LogsComponent key={paths.object(entry)} pod={response.entry} />}

        <EventsSection events={response.events} error={response.eventsError} />

        <div className="atlas-section">
          <details className="atlas-yaml">
            <summary>YAML</summary>
            <pre>{response.yaml}</pre>
          </details>
        </div>
      </div>
    );
  }
}

/** A port and everything reachable on it: a link to the port itself and its known pages. */
type PortRow = { name: string; port: number; protocol: string; hostPort: string; url: string; pages: atlas.PortLink[] };

/** Ports grouped by how they are reached: the pod itself, or a service in front of it. */
type ViaGroup = { key: string; label: string; rows: PortRow[] };

function groupPorts(ports: atlas.PortLink[]): ViaGroup[] {
  const groups = new Map<string, ViaGroup>();
  for (const p of ports) {
    const viaService = p.via === atlas.PortLink.Via.SERVICE;
    const key = viaService ? `service:${p.viaName}` : "pod";
    let group = groups.get(key);
    if (!group) {
      group = { key, label: viaService ? `via ${p.viaName || "service"}` : "pod", rows: [] };
      groups.set(key, group);
    }
    let row = group.rows.find((r) => r.port === p.port);
    if (!row) {
      row = { name: p.name, port: p.port, protocol: p.protocol, hostPort: p.hostPort, url: "", pages: [] };
      group.rows.push(row);
    }
    if (p.path) {
      row.pages.push(p);
    } else {
      row.url = p.url;
    }
  }
  return [...groups.values()];
}

function PortsSection({ ports }: { ports: atlas.PortLink[] }) {
  const groups = React.useMemo(() => groupPorts(ports), [ports]);
  const [selected, setSelected] = React.useState(0);
  if (!groups.length) return null;
  const group = groups[Math.min(selected, groups.length - 1)];
  const reachable = group.rows.some((r) => r.hostPort);
  return (
    <div className="atlas-section">
      <h3>Ports</h3>
      {groups.length > 1 && (
        <div className="atlas-switch">
          {groups.map((g, i) => (
            <button
              key={g.key}
              className={`atlas-switch-option ${g === group ? "selected" : ""}`}
              onClick={() => setSelected(i)}>
              {g.label}
            </button>
          ))}
        </div>
      )}
      <div className="atlas-card">
        {group.rows.map((r) => (
          <div className="atlas-portrow" key={r.port}>
            <span className="atlas-port-name">
              {r.name && `${r.name} `}
              {r.port}
              {r.protocol && <span className="atlas-muted atlas-small"> {r.protocol}</span>}
            </span>
            <span className="atlas-port-links">
              {r.url && (
                <a href={r.url} target="_blank" rel="noopener" className="atlas-chip" title={r.url}>
                  open
                </a>
              )}
              {r.pages.map((pg) => (
                <a key={pg.path} href={pg.url} target="_blank" rel="noopener" className="atlas-chip" title={pg.url}>
                  {pg.name}
                </a>
              ))}
            </span>
            {r.hostPort && <CopyButton text={r.hostPort} />}
          </div>
        ))}
      </div>
      {!reachable && <div className="atlas-muted atlas-small">no tunnel zone configured for this cluster</div>}
    </div>
  );
}

function PodsSection({ relations }: { relations: atlas.Relations }) {
  if (!relations.pods.length) return null;
  return (
    <div className="atlas-section">
      <h3>
        Pods <span className="atlas-muted">({relations.podsTotal})</span>
      </h3>
      <div className="atlas-card">
        <table className="atlas-table">
          <thead>
            <tr>
              <th>name</th>
              <th>ready</th>
              <th>status</th>
              <th>↻</th>
              <th>node</th>
              <th>age</th>
            </tr>
          </thead>
          <tbody>
            {relations.pods.map((p) => (
              <tr key={p.name}>
                <td className="atlas-mono atlas-wrap">
                  <Link to={paths.object(p)}>{p.name}</Link>
                </td>
                <td>
                  <ReadyBadge entry={p} />
                </td>
                <td>
                  <PhaseBadge entry={p} />
                </td>
                <td>{toNumber(p.restarts) || ""}</td>
                <td className="atlas-muted atlas-small">{p.node}</td>
                <td className="atlas-muted atlas-small">{age(p.created)}</td>
              </tr>
            ))}
          </tbody>
        </table>
        {relations.podsTotal > relations.pods.length && (
          <div className="atlas-portrow atlas-muted atlas-small">
            showing {relations.pods.length} of {relations.podsTotal}
          </div>
        )}
      </div>
    </div>
  );
}

function EventsSection({ events, error }: { events: atlas.Event[]; error: string }) {
  if (error) {
    return (
      <div className="atlas-section">
        <h3>Events</h3>
        <div className="atlas-muted atlas-small">unavailable: {error}</div>
      </div>
    );
  }
  if (!events.length) return null;
  return (
    <div className="atlas-section">
      <h3>Events</h3>
      <div className="atlas-card">
        <table className="atlas-table">
          <tbody>
            {events.map((e, i) => (
              <tr key={i}>
                <td>
                  <Badge tone={e.type === "Warning" ? "warn" : ""}>{e.reason || e.type}</Badge>
                </td>
                <td className="atlas-wrap">{e.message}</td>
                <td className="atlas-muted atlas-small">{toNumber(e.count) > 1 ? `×${e.count}` : ""}</td>
                <td className="atlas-muted atlas-small atlas-nowrap">{age(e.lastSeen)}</td>
              </tr>
            ))}
          </tbody>
        </table>
      </div>
    </div>
  );
}
