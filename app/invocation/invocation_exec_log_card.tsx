import React from "react";
import InvocationModel from "./invocation_model";
import Select, { Option } from "../components/select/select";
import { build } from "../../proto/remote_execution_ts_proto";
import rpcService from "../service/rpc_service";
import { OutlinedButton } from "../components/button/button";
import { build_event_stream } from "../../proto/build_event_stream_ts_proto";
import { tools } from "../../proto/spawn_ts_proto";
import format from "../format/format";
import error_service from "../errors/error_service";
import * as varint from "varint";
import { AlertCircle, CheckCircle, Download } from "lucide-react";
import DigestComponent from "../components/digest/digest";
import Link from "../components/link/link";
import { digestToString } from "../util/cache";

interface Props {
  inProgress: boolean;
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
    limit: 100,
    log: undefined,
  };

  timeoutRef?: number;

  componentDidMount() {
    this.fetchLog();
  }

  componentDidUpdate(prevProps: Props) {
    if (this.props.model !== prevProps.model) {
      this.fetchLog();
    }
  }

  componentWillUnmount() {
    clearTimeout(this.timeoutRef);
  }

  getExecutionLogFile(): build_event_stream.File | undefined {
    return this.props.model.buildToolLogs?.log.find(
      (log: build_event_stream.File) =>
        (log.name == "execution.log" || log.name == "execution_log.binpb.zstd") &&
        log.uri &&
        Boolean(log.uri.startsWith("bytestream://"))
    );
  }

  fetchLog() {
    if (!this.getExecutionLogFile()) {
      this.setState({ loading: false });
    }

    // Already fetched
    if (this.state.log) return;

    let logFile = this.getExecutionLogFile();
    if (!logFile?.uri) return;

    const init = {
      // Set the stored encoding header to prevent the server from double-compressing.
      headers: { "X-Stored-Encoding-Hint": "zstd" },
    };

    this.setState({ loading: true });
    rpcService
      .fetchBytestreamFile(logFile.uri, this.props.model.getInvocationId(), "arraybuffer", { init })
      .then(async (body) => {
        if (body === null) throw new Error("response body is null");
        let entries: tools.protos.ExecLogEntry[] = [];
        let byteArray = new Uint8Array(body);
        for (var offset = 0; offset < body.byteLength; ) {
          let length = varint.decode(byteArray, offset);
          let bytes = varint.decode.bytes || 0;
          offset += bytes;
          entries.push(tools.protos.ExecLogEntry.decode(byteArray.subarray(offset, offset + length)));
          offset += length;
        }
        console.log(entries);
        return entries;
      })
      .then((log) => this.setState({ log: log }))
      .catch((e) => error_service.handleError(e))
      .finally(() => this.setState({ loading: false }));
  }

  downloadLog() {
    let profileFile = this.getExecutionLogFile();
    if (!profileFile?.uri) {
      return;
    }

    try {
      rpcService.downloadBytestreamFile(
        "execution_log.binpb.zstd",
        profileFile.uri,
        this.props.model.getInvocationId()
      );
    } catch {
      console.error("Error downloading execution log");
    }
  }

  sort(a: tools.protos.ExecLogEntry, b: tools.protos.ExecLogEntry): number {
    let first = this.state.direction == "asc" ? a : b;
    let second = this.state.direction == "asc" ? b : a;

    switch (this.state.sort) {
      case "total-duration":
        if (+(first?.spawn?.metrics?.totalTime?.seconds || 0) == +(second?.spawn?.metrics?.totalTime?.seconds || 0)) {
          return +(first?.spawn?.metrics?.totalTime?.nanos || 0) - +(second?.spawn?.metrics?.totalTime?.nanos || 0);
        }
        return +(first?.spawn?.metrics?.totalTime?.seconds || 0) - +(second?.spawn?.metrics?.totalTime?.seconds || 0);
    }
    return 0;
  }

  handleInputChange(event: React.ChangeEvent<HTMLInputElement>) {
    const target = event.target;
    const name = target.name;
    this.setState({
      [name]: target.value,
    } as Record<keyof State, any>);
  }

  handleSortChange(event: React.ChangeEvent<HTMLSelectElement>) {
    this.setState({
      sort: event.target.value,
    });
  }

  handleMnemonicFilterChange(event: React.ChangeEvent<HTMLSelectElement>) {
    this.setState({
      mnemonicFilter: event.target.value,
    });
  }

  handleRunnerFilterChange(event: React.ChangeEvent<HTMLSelectElement>) {
    this.setState({
      runnerFilter: event.target.value,
    });
  }

  handleMoreClicked() {
    this.setState({ limit: this.state.limit + 100 });
  }

  handleAllClicked() {
    this.setState({ limit: Number.MAX_SAFE_INTEGER });
  }

  getActionPageLink(entry: tools.protos.ExecLogEntry) {
    const search = new URLSearchParams();
    if (entry.spawn?.digest) {
      search.set("actionDigest", digestToString(entry.spawn.digest));
    }

    return `/invocation/${this.props.model.getInvocationId()}?${search}#action`;
  }

  render() {
    if (this.state.loading) {
      return <div className="loading" />;
    }
    if (!this.state.log?.length) {
      return (
        <div className="invocation-execution-empty-state">
          No execution log actions for this invocation{this.props.inProgress && <span> yet</span>}.
        </div>
      );
    }

    const mnemonics = new Set<string>();
    const runners = new Set<string>();

    const spawns = this.state.log
      .filter((l) => {
        if (l.spawn?.mnemonic) {
          mnemonics.add(l.spawn.mnemonic);
        }
        if (l.spawn?.runner) {
          runners.add(l.spawn.runner);
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
                Executed actions ({spawns.length}){" "}
                <Download className="download-exec-log-button" onClick={() => this.downloadLog()} />
              </div>
              <div className="invocation-sort-controls">
                <span className="invocation-filter-title">Mnemnonic</span>
                <Select onChange={this.handleMnemonicFilterChange.bind(this)} value={this.state.mnemonicFilter}>
                  <Option value="">All</Option>
                  {[...mnemonics].map((m) => (
                    <Option value={m}>{m}</Option>
                  ))}
                </Select>
                <span className="invocation-sort-title">Runner</span>
                <Select onChange={this.handleRunnerFilterChange.bind(this)} value={this.state.runnerFilter}>
                  <Option value="">All</Option>
                  {[...runners].map((m) => (
                    <Option value={m}>{m}</Option>
                  ))}
                </Select>
                <span className="invocation-sort-title">Sort by</span>
                <Select onChange={this.handleSortChange.bind(this)} value={this.state.sort}>
                  <Option value="total-duration">Total Duration</Option>
                </Select>
                <span className="group-container">
                  <div>
                    <input
                      id="direction-asc"
                      checked={this.state.direction == "asc"}
                      onChange={this.handleInputChange.bind(this)}
                      value="asc"
                      name="direction"
                      type="radio"
                    />
                    <label htmlFor="direction-asc">Asc</label>
                  </div>
                  <div>
                    <input
                      id="direction-desc"
                      checked={this.state.direction == "desc"}
                      onChange={this.handleInputChange.bind(this)}
                      value="desc"
                      name="direction"
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
                        {spawn.spawn?.metrics?.totalTime && (
                          <div>Duration: {format.durationProto(spawn.spawn.metrics.totalTime)}</div>
                        )}
                        <div>Mnemonic: {spawn.spawn?.mnemonic}</div>
                        <div>Runner: {spawn.spawn?.runner}</div>
                        <div>Remotable: {spawn.spawn?.remotable ? "true" : "false"}</div>
                        <div>Cachable: {spawn.spawn?.cacheable ? "true" : "false"}</div>
                        <div>Exit code: {spawn.spawn?.exitCode || 0}</div>
                      </div>
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
