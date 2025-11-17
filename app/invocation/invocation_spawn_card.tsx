import { AlertCircle, CheckCircle, Download } from "lucide-react";
import React from "react";
import { build } from "../../proto/remote_execution_ts_proto";
import { tools } from "../../proto/spawn_ts_proto";
import { OutlinedButton } from "../components/button/button";
import DigestComponent from "../components/digest/digest";
import Link from "../components/link/link";
import Select, { Option } from "../components/select/select";
import error_service from "../errors/error_service";
import format from "../format/format";
import { digestToString } from "../util/cache";
import ActionCompareButtonComponent from "./action_compare_button";
import InvocationModel from "./invocation_model";

interface Props {
  model: InvocationModel;
  search: URLSearchParams;
  filter: string;
}

interface State {
  loading: boolean;
  sort: string;
  direction: "asc" | "desc";
  mnemonicFilter: string;
  runnerFilter: string;
  platformPropertyFilters: Map<string, string>;
  limit: number;
  log: tools.protos.ExecLogEntry[] | undefined;
}

const ExecutionStage = build.bazel.remote.execution.v2.ExecutionStage;

export default class InvocationExecLogCardComponent extends React.Component<Props, State> {
  state: State = {
    loading: true,
    sort: "total-duration",
    direction: "desc",
    mnemonicFilter: "",
    runnerFilter: "",
    platformPropertyFilters: new Map<string, string>(),
    limit: 100,
    log: undefined,
  };

  timeoutRef?: number;

  componentDidMount(): void {
    this.fetchLog();
  }

  componentDidUpdate(prevProps: Props): void {
    if (this.props.model !== prevProps.model) {
      this.fetchLog();
    }
  }

  componentWillUnmount(): void {
    clearTimeout(this.timeoutRef);
  }

  fetchLog(): void {
    if (!this.props.model.hasExecutionLog()) {
      this.setState({ loading: false });
    }

    // Already fetched
    if (this.state.log) return;

    this.setState({ loading: true });

    this.props.model
      .getExecutionLog()
      .then((log) => this.setState({ log: log }))
      .catch((e) => error_service.handleError(e))
      .finally(() => this.setState({ loading: false }));
  }

  downloadLog(): void {
    this.props.model.downloadExecutionLog();
  }

  sort(a: tools.protos.ExecLogEntry, b: tools.protos.ExecLogEntry): number {
    let first = this.state.direction == "asc" ? a : b;
    let second = this.state.direction == "asc" ? b : a;

    switch (this.state.sort) {
      case "start-time":
        if (+(first?.spawn?.metrics?.startTime?.seconds || 0) == +(second?.spawn?.metrics?.startTime?.seconds || 0)) {
          return +(first?.spawn?.metrics?.startTime?.nanos || 0) - +(second?.spawn?.metrics?.startTime?.nanos || 0);
        }
        return +(first?.spawn?.metrics?.startTime?.seconds || 0) - +(second?.spawn?.metrics?.startTime?.seconds || 0);
      case "total-duration":
        if (+(first?.spawn?.metrics?.totalTime?.seconds || 0) == +(second?.spawn?.metrics?.totalTime?.seconds || 0)) {
          return +(first?.spawn?.metrics?.totalTime?.nanos || 0) - +(second?.spawn?.metrics?.totalTime?.nanos || 0);
        }
        return +(first?.spawn?.metrics?.totalTime?.seconds || 0) - +(second?.spawn?.metrics?.totalTime?.seconds || 0);
    }
    return 0;
  }

  handleSortDirectionChange(event: React.ChangeEvent<HTMLInputElement>): void {
    this.setState({
      direction: event.target.value as "asc" | "desc",
    });
  }

  handleSortChange(event: React.ChangeEvent<HTMLSelectElement>): void {
    this.setState({
      sort: event.target.value,
    });
  }

  handleMnemonicFilterChange(event: React.ChangeEvent<HTMLSelectElement>): void {
    this.setState({
      mnemonicFilter: event.target.value,
    });
  }

  handleRunnerFilterChange(event: React.ChangeEvent<HTMLSelectElement>): void {
    this.setState({
      runnerFilter: event.target.value,
    });
  }

  handlePlatformPropertyFilterChange(propertyName: string, value: string): void {
    this.setState((prevState) => {
      const newFilters = new Map(prevState.platformPropertyFilters);
      if (value === "*") {
        newFilters.delete(propertyName);
      } else {
        newFilters.set(propertyName, value);
      }
      return { platformPropertyFilters: newFilters };
    });
  }

  handleMoreClicked(): void {
    this.setState({ limit: this.state.limit + 100 });
  }

  handleAllClicked(): void {
    this.setState({ limit: Number.MAX_SAFE_INTEGER });
  }

  getActionPageLink(entry: tools.protos.ExecLogEntry): string | undefined {
    if (!entry.spawn?.digest) {
      return undefined;
    }

    const search = new URLSearchParams();
    search.set("actionDigest", digestToString(entry.spawn.digest));
    return `/invocation/${this.props.model.getInvocationId()}?${search}#action`;
  }

  getPlatformProperties(platform?: any): Map<string, string> {
    const props = new Map<string, string>();
    if (!platform?.properties) return props;
    for (const prop of platform.properties) {
      if (prop.name && prop.value) {
        props.set(prop.name, prop.value);
      }
    }
    return props;
  }

  getPlatformPropertyValue(platform?: any, propertyName?: string): string {
    if (!platform?.properties || !propertyName) return "";
    const prop = platform.properties.find((p: any) => p.name === propertyName);
    return prop?.value || "";
  }

  render(): React.ReactNode {
    if (this.state.loading) {
      return <div className="loading" />;
    }
    if (!this.state.log?.length) {
      return (
        <div className="invocation-execution-empty-state">
          No execution log actions for this invocation{this.props.model.isInProgress() && <span> yet</span>}.
        </div>
      );
    }

    const mnemonics = new Set<string>();
    const runners = new Set<string>();
    const platformPropertyValues = new Map<string, Set<string>>();

    const spawns = this.state.log
      .filter((l) => {
        if (l.spawn?.mnemonic) {
          mnemonics.add(l.spawn.mnemonic);
        }
        if (l.spawn?.runner) {
          runners.add(l.spawn.runner);
        }
        const platformProps = this.getPlatformProperties(l.spawn?.platform);
        for (const [propName, propValue] of platformProps) {
          if (!platformPropertyValues.has(propName)) {
            platformPropertyValues.set(propName, new Set<string>());
          }
          platformPropertyValues.get(propName)!.add(propValue);
        }

        if (l.type != "spawn") {
          return false;
        }
        if (this.state.mnemonicFilter != "" && l.spawn?.mnemonic != this.state.mnemonicFilter) {
          return false;
        }
        if (this.state.runnerFilter != "" && l.spawn?.runner != this.state.runnerFilter) {
          return false;
        }
        for (const [propName, filterValue] of this.state.platformPropertyFilters) {
          const actualValue = this.getPlatformPropertyValue(l.spawn?.platform, propName);
          if (filterValue != "*" && actualValue != filterValue) {
            return false;
          }
        }
        if (
          this.props.filter != "" &&
          !l.spawn?.targetLabel.toLowerCase().includes(this.props.filter.toLowerCase()) &&
          !l.spawn?.mnemonic.toLowerCase().includes(this.props.filter.toLowerCase()) &&
          !l.spawn?.args.join(" ").toLowerCase().includes(this.props.filter.toLowerCase()) &&
          !l.spawn?.digest?.hash.toLowerCase().includes(this.props.filter.toLowerCase())
        ) {
          return false;
        }

        return true;
      })
      .sort(this.sort.bind(this));

    return (
      <div>
        <div className={`card expanded`}>
          <div className="content">
            <div className="invocation-content-header">
              <div className="title">
                Spawns ({format.formatWithCommas(spawns.length)}){" "}
                <Download className="download-exec-log-button" onClick={() => this.downloadLog()} />
              </div>
              <div className="invocation-sort-controls">
                <div className="invocation-filter-control">
                  <span className="invocation-filter-title">Mnemonic</span>
                  <Select onChange={this.handleMnemonicFilterChange.bind(this)} value={this.state.mnemonicFilter}>
                    <Option value="">All</Option>
                    {sorted(mnemonics).map((m) => (
                      <Option value={m}>{m}</Option>
                    ))}
                  </Select>
                </div>
                <div className="invocation-filter-control">
                  <span className="invocation-filter-title">Runner</span>
                  <Select onChange={this.handleRunnerFilterChange.bind(this)} value={this.state.runnerFilter}>
                    <Option value="">All</Option>
                    {sorted(runners).map((m) => (
                      <Option value={m}>{m}</Option>
                    ))}
                  </Select>
                </div>
                {Array.from(platformPropertyValues.entries())
                  .sort(([a], [b]) => a.localeCompare(b))
                  .map(([propName, values]) => (
                    <div className="invocation-filter-control" key={propName}>
                      <span className="invocation-filter-title">{propName}</span>
                      <Select
                        onChange={(e) => this.handlePlatformPropertyFilterChange(propName, e.target.value)}
                        value={
                          this.state.platformPropertyFilters.has(propName)
                            ? this.state.platformPropertyFilters.get(propName)
                            : "*"
                        }>
                        <Option value="*">All</Option>
                        {sorted(values).map((value) => (
                          <Option key={value} value={value}>
                            {value}
                          </Option>
                        ))}
                        <Option value="">Not set</Option>
                      </Select>
                    </div>
                  ))}
                <div className="invocation-filter-control">
                  <span className="invocation-filter-title">Sort by</span>
                  <Select onChange={this.handleSortChange.bind(this)} value={this.state.sort}>
                    <Option value="start-time">Start Time</Option>
                    <Option value="total-duration">Total Duration</Option>
                  </Select>
                </div>
                <span className="group-container">
                  <div>
                    <input
                      id="direction-asc"
                      checked={this.state.direction == "asc"}
                      onChange={this.handleSortDirectionChange.bind(this)}
                      value="asc"
                      name="execLogDirection"
                      type="radio"
                    />
                    <label htmlFor="direction-asc">Asc</label>
                  </div>
                  <div>
                    <input
                      id="direction-desc"
                      checked={this.state.direction == "desc"}
                      onChange={this.handleSortDirectionChange.bind(this)}
                      value="desc"
                      name="execLogDirection"
                      type="radio"
                    />
                    <label htmlFor="direction-desc">Desc</label>
                  </div>
                </span>
              </div>
            </div>
            <div>
              <div className="invocation-execution-table">
                {spawns.slice(0, this.state.limit).map((spawn) => (
                  <Link key={spawn.id} className="invocation-execution-row" href={this.getActionPageLink(spawn)}>
                    <div className="invocation-execution-row-image">
                      {spawn.spawn?.exitCode == 0 ? (
                        <CheckCircle className="icon green" />
                      ) : (
                        <AlertCircle className="icon red" />
                      )}
                    </div>
                    <div>
                      <div className="invocation-execution-row-header">
                        <span className="invocation-execution-row-header-status">{spawn.spawn?.targetLabel}</span>
                        {spawn.spawn?.digest && <DigestComponent digest={spawn.spawn.digest} expanded={true} />}
                      </div>
                      <div>{spawn.spawn?.args.join(" ").slice(0, 200)}...</div>
                      <div className="invocation-execution-row-stats">
                        <div>Mnemonic: {spawn.spawn?.mnemonic}</div>
                        <div>Runner: {spawn.spawn?.runner}</div>
                        {Array.from(this.getPlatformProperties(spawn.spawn?.platform)).map(([propName, propValue]) => (
                          <div key={propName}>
                            {propName}: {propValue}
                          </div>
                        ))}
                        <div>Remotable: {spawn.spawn?.remotable ? "true" : "false"}</div>
                        <div>Cachable: {spawn.spawn?.cacheable ? "true" : "false"}</div>
                        <div>Exit code: {spawn.spawn?.exitCode || 0}</div>
                        {spawn.spawn?.metrics?.startTime && (
                          <div>Start time: {format.formatTimestamp(spawn.spawn.metrics.startTime)}</div>
                        )}
                        {spawn.spawn?.metrics?.totalTime && (
                          <div>Duration: {format.durationProto(spawn.spawn.metrics.totalTime)}</div>
                        )}
                      </div>
                      {spawn.spawn?.digest && (
                        <ActionCompareButtonComponent
                          invocationId={this.props.model.getInvocationId()}
                          actionDigest={digestToString(spawn.spawn.digest)}
                          mini={true}
                        />
                      )}
                    </div>
                  </Link>
                ))}
              </div>
              {spawns.length > this.state.limit && (
                <div className="more-buttons">
                  <OutlinedButton onClick={this.handleMoreClicked.bind(this)}>See more executions</OutlinedButton>
                  <OutlinedButton onClick={this.handleAllClicked.bind(this)}>See all executions</OutlinedButton>
                </div>
              )}
            </div>
          </div>
        </div>
      </div>
    );
  }
}

// TODO(bduffany): figure out ES2023 polyfills and use toSorted() instead.
function sorted<T>(items: Iterable<T>): T[] {
  return [...items].sort();
}
