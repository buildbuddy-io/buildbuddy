import { ArrowRight, Circle, CircleDot, MinusCircle, PlusCircle } from "lucide-react";
import React from "react";
import * as varint from "varint";
import { build_event_stream } from "../../proto/build_event_stream_ts_proto";
import { tools } from "../../proto/spawn_ts_proto";
import { OutlinedButton } from "../components/button/button";
import DigestComponent from "../components/digest/digest";
import Link from "../components/link/link";
import error_service from "../errors/error_service";
import InvocationModel from "../invocation/invocation_model";
import rpcService from "../service/rpc_service";
import { digestToString } from "../util/cache";

interface Props {
  modelA: InvocationModel;
  modelB: InvocationModel;
  search: URLSearchParams;
  filter: string;
}

interface State {
  loading: boolean;
  limit: Map<string, number>;
  logA: tools.protos.ExecLogEntry[] | undefined;
  logB: tools.protos.ExecLogEntry[] | undefined;

  changed: SpawnComparison[];
  added: SpawnComparison[];
  removed: SpawnComparison[];
  unchanged: SpawnComparison[];
}

interface SpawnComparison {
  a?: tools.protos.ExecLogEntry;
  b?: tools.protos.ExecLogEntry;
  status: "added" | "removed" | "changed" | "unchanged";
}

const PAGE_SIZE = 10;

export default class CompareExecutionLogSpawnsComponent extends React.Component<Props, State> {
  state: State = {
    loading: true,
    limit: new Map<string, number>(),
    logA: undefined,
    logB: undefined,
    changed: [],
    added: [],
    removed: [],
    unchanged: [],
  };

  componentDidMount() {
    this.fetchLogs();
  }

  componentDidUpdate(prevProps: Props) {
    if (this.props.modelA !== prevProps.modelA || this.props.modelB !== prevProps.modelB) {
      this.fetchLogs();
    }
  }

  async fetchLogs() {
    await Promise.all([this.fetchLog(this.props.modelA), this.fetchLog(this.props.modelB)])
      .then(([logA, logB]) => {
        this.setState({ logA, logB }, () => {
          this.compareSpawns();
        });
      })
      .catch((e) => error_service.handleError(e))
      .finally(() => this.setState({ loading: false }));
  }

  fetchLog(model?: InvocationModel) {
    if (!model) return Promise.resolve(undefined);

    if (!model.hasExecutionLog()) {
      this.setState({ loading: false });
    }

    this.setState({ loading: true });
    return model.getExecutionLog()
      .catch((e) => error_service.handleError(e))
      .finally(() => this.setState({ loading: false }));
  }

  getSpawnKey(spawn?: tools.protos.ExecLogEntry.Spawn | null): string {
    if (!spawn) return "";
    // TODO (siggisim): do something smarter here like first action output
    return `${spawn.targetLabel}:${spawn.mnemonic}:${spawn.args.join(" ")}`;
  }

  getSpawnDigest(spawn?: tools.protos.ExecLogEntry.Spawn | null): string {
    if (!spawn?.digest) return "";
    return spawn.digest.hash;
  }

  compareSpawns() {
    const spawnsA = this.state.logA?.filter((l) => l.type == "spawn" && l.spawn) || [];
    const spawnsB = this.state.logB?.filter((l) => l.type == "spawn" && l.spawn) || [];

    const changed: SpawnComparison[] = [];
    const added: SpawnComparison[] = [];
    const removed: SpawnComparison[] = [];
    const unchanged: SpawnComparison[] = [];

    const spawnMapB = new Map<string, tools.protos.ExecLogEntry>();

    for (const spawnB of spawnsB) {
      const key = this.getSpawnKey(spawnB.spawn);
      spawnMapB.set(key, spawnB);
    }

    for (const spawnA of spawnsA) {
      const key = this.getSpawnKey(spawnA.spawn);
      const spawnB = spawnMapB.get(key);

      if (!spawnB) {
        removed.push({ a: spawnA, status: "removed" });
      } else {
        spawnMapB.delete(key);
        const digestA = this.getSpawnDigest(spawnA.spawn);
        const digestB = this.getSpawnDigest(spawnB.spawn);

        if (digestA !== digestB || spawnA.spawn?.exitCode !== spawnB.spawn?.exitCode) {
          changed.push({ a: spawnA, b: spawnB, status: "changed" });
        } else {
          unchanged.push({ a: spawnA, b: spawnB, status: "unchanged" });
        }
      }
    }

    for (const [_, spawnB] of spawnMapB) {
      added.push({ b: spawnB, status: "added" });
    }

    this.setState({ changed, added, removed, unchanged });
  }

  sort(a: SpawnComparison, b: SpawnComparison): number {
    const getSpawn = (comp: SpawnComparison) => comp.a?.spawn || comp.b?.spawn;
    const firstSpawn = getSpawn(a);
    const secondSpawn = getSpawn(b);

    if (+(firstSpawn?.metrics?.startTime?.seconds || 0) == +(secondSpawn?.metrics?.startTime?.seconds || 0)) {
      return +(firstSpawn?.metrics?.startTime?.nanos || 0) - +(secondSpawn?.metrics?.startTime?.nanos || 0);
    }
    return +(firstSpawn?.metrics?.startTime?.seconds || 0) - +(secondSpawn?.metrics?.startTime?.seconds || 0);
  }

  handleMoreClicked(section: string) {
    this.setState({ limit: this.state.limit.set(section, (this.state.limit.get(section) || PAGE_SIZE) + PAGE_SIZE) });
  }

  handleAllClicked(section: string) {
    this.setState({ limit: this.state.limit.set(section, Number.MAX_SAFE_INTEGER) });
  }

  getActionPageLink(model: InvocationModel, spawn?: tools.protos.ExecLogEntry.Spawn | null) {
    if (!spawn?.digest) {
      return undefined;
    }
    const search = new URLSearchParams();
    search.set("actionDigest", digestToString(spawn.digest));
    return `/invocation/${model.getInvocationId()}?${search}#action`;
  }

  getCompareActionLink(comparison: SpawnComparison) {
    if (comparison.status !== "changed" || !comparison.a?.spawn?.digest || !comparison.b?.spawn?.digest) {
      return undefined;
    }
    const digestA = digestToString(comparison.a.spawn.digest);
    const digestB = digestToString(comparison.b.spawn.digest);
    return `/action/compare/${this.props.modelA.getInvocationId()}:${encodeURIComponent(digestA)}...${this.props.modelB.getInvocationId()}:${encodeURIComponent(digestB)}`;
  }

  renderComparisonRow(comparison: SpawnComparison, index: number) {
    const spawn = comparison.a?.spawn || comparison.b?.spawn;
    let link = this.getCompareActionLink(comparison);

    if (comparison.status === "removed") {
      link = this.getActionPageLink(this.props.modelA, comparison.a?.spawn);
    } else if (comparison.status === "added" || comparison.status === "unchanged") {
      link = this.getActionPageLink(this.props.modelB, comparison.b?.spawn);
    }

    return (
      <Link key={index} className={`invocation-execution-row spawn-comparison-row ${comparison.status}`} href={link}>
        <div>
          <div className="invocation-execution-row-header">
            <span className="invocation-execution-row-header-status">
              {spawn?.targetLabel} ({spawn?.mnemonic})
            </span>
            {comparison.status === "changed" && comparison.a?.spawn?.digest && comparison.b?.spawn?.digest && (
              <>
                <DigestComponent digest={comparison.a.spawn.digest} expanded={false} />
                <ArrowRight className="icon" />
                <DigestComponent digest={comparison.b.spawn.digest} expanded={false} />
              </>
            )}
            {comparison.status !== "changed" && spawn?.digest && (
              <DigestComponent digest={spawn.digest} expanded={false} />
            )}
          </div>
          <div className="invocation-spawn-args">{spawn?.args.join(" ").slice(0, 200)}...</div>
        </div>
      </Link>
    );
  }

  renderCard(comparisons: SpawnComparison[], section: string, icon: React.ReactNode) {
    return (
      <div className={`card expanded`}>
        <div className="content">
          <div className="invocation-content-header">
            <div className="title">
              {icon}
              {comparisons.length} {section} spawn{comparisons.length == 1 ? "" : "s"}
            </div>
            <div>
              <div className="invocation-execution-table">
                {comparisons.slice(0, this.state.limit.get(section) || PAGE_SIZE).map((comparison, index) => {
                  return this.renderComparisonRow(comparison, index);
                })}
              </div>
              {comparisons.length > (this.state.limit.get(section) || PAGE_SIZE) && (
                <div className="compare-spawns-more-buttons">
                  <OutlinedButton onClick={this.handleMoreClicked.bind(this, section)}>
                    See more {section} spawns
                  </OutlinedButton>
                  <OutlinedButton onClick={this.handleAllClicked.bind(this, section)}>
                    See all {section} spawns
                  </OutlinedButton>
                </div>
              )}
            </div>
          </div>
        </div>
      </div>
    );
  }

  render() {
    if (this.state.loading) {
      return <div className="loading" />;
    }
    if (!this.state.logA?.length && !this.state.logB?.length) {
      return <div className="invocation-execution-empty-state">No execution log spawns for these invocations.</div>;
    }

    return (
      <>
        {this.renderCard(this.state.changed, "changed", <CircleDot className="icon yellow" />)}
        {this.renderCard(this.state.added, "added", <PlusCircle className="icon green" />)}
        {this.renderCard(this.state.removed, "removed", <MinusCircle className="icon red" />)}
        {this.renderCard(this.state.unchanged, "unchanged", <Circle className="icon" />)}
      </>
    );
  }
}
