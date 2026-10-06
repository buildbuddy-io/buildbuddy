import Long from "long";
import { RefreshCw } from "lucide-react";
import React from "react";
import { OutlinedButton } from "../../../../app/components/button/button";
import Checkbox from "../../../../app/components/checkbox/checkbox";
import Select, { Option } from "../../../../app/components/select/select";
import { BuildBuddyError } from "../../../../app/util/errors";
import { atlas } from "../../../../proto/atlas_ts_proto";
import rpcService, { ServerStream } from "../lib/rpc_service";
import { LogLine, clockTime, parseLogLine, severityTone } from "../lib/structured_log";

interface Props {
  pod: atlas.Entry;
}

interface State {
  container: string;
  tailLines: string;
  follow: boolean;
  previous: boolean;
  /** Show lines as written instead of the readable form of structured ones. */
  raw: boolean;
  /** The newest complete lines, parsed; the tail still arriving is `partial`. */
  lines: LogLine[];
  /** How many older lines were let go to stay under MAX_LINES. */
  dropped: number;
  errorMessage?: string;
  streaming: boolean;
}

const TAIL_OPTIONS = ["200", "1000", "5000"];
// A followed pod can log thousands of lines a second. Render them in batches,
// and keep only the newest, so the page's work per second stays bounded.
const FLUSH_MS = 100;
const MAX_LINES = 5000;

/**
 * A pod's logs, streamed from the StreamLogs RPC. With follow on, the stream
 * stays open and new output is appended as it arrives.
 */
export default class LogsComponent extends React.Component<Props, State> {
  state: State = {
    container: this.props.pod.containers[0] ?? "",
    tailLines: TAIL_OPTIONS[0],
    follow: false,
    previous: false,
    raw: false,
    lines: [],
    dropped: 0,
    streaming: false,
  };
  private stream?: ServerStream<atlas.StreamLogsResponse>;
  private decoder = new TextDecoder();
  private partial = "";
  /** Parsed lines not yet rendered, and the timer that will render them. */
  private pending: LogLine[] = [];
  private flushTimer?: number;
  private pane = React.createRef<HTMLPreElement>();

  componentDidMount() {
    this.start();
  }

  componentDidUpdate(_: Props, prevState: State) {
    if (
      prevState.container !== this.state.container ||
      prevState.tailLines !== this.state.tailLines ||
      prevState.follow !== this.state.follow ||
      prevState.previous !== this.state.previous
    ) {
      this.start();
    }
  }

  componentWillUnmount() {
    this.stop();
    window.clearTimeout(this.flushTimer);
  }

  private stop() {
    this.stream?.cancel();
    this.stream = undefined;
  }

  private start() {
    this.stop();
    this.decoder = new TextDecoder();
    this.partial = "";
    this.pending = [];
    window.clearTimeout(this.flushTimer);
    this.flushTimer = undefined;
    this.setState({ lines: [], dropped: 0, errorMessage: undefined, streaming: true });
    const pod = this.props.pod;
    this.stream = rpcService.service.streamLogs(
      new atlas.StreamLogsRequest({
        cluster: pod.cluster,
        namespace: pod.namespace,
        name: pod.name,
        container: this.state.container,
        tailLines: Long.fromNumber(Number(this.state.tailLines)),
        follow: this.state.follow,
        previous: this.state.previous,
      }),
      {
        next: (chunk) => this.append(this.decoder.decode(chunk.data, { stream: true })),
        error: (e) => this.setState({ errorMessage: BuildBuddyError.parse(e).description, streaming: false }),
        complete: () => this.finish(),
      }
    );
  }

  /** The stream is done, consume partial text. */
  private finish() {
    const rest = this.partial + this.decoder.decode();
    this.partial = "";
    if (rest) {
      this.pending.push(parseLogLine(rest));
    }
    this.flush();
    this.setState({ streaming: false });
  }

  private append(text: string) {
    // Chunks end anywhere; only complete lines are parsed.
    const parts = (this.partial + text).split("\n");
    this.partial = parts.pop() ?? "";
    for (const part of parts) {
      this.pending.push(parseLogLine(part));
    }
    if (this.flushTimer === undefined) {
      this.flushTimer = window.setTimeout(this.flush, FLUSH_MS);
    }
  }

  /** Renders what arrived since the last flush, letting the oldest lines go. */
  private flush = () => {
    window.clearTimeout(this.flushTimer);
    this.flushTimer = undefined;
    const pending = this.pending;
    this.pending = [];
    // Stick to the bottom only if the reader is already there.
    const pane = this.pane.current;
    const pinned = !pane || pane.scrollHeight - pane.scrollTop - pane.clientHeight < 40;
    this.setState(
      (state) => {
        const all = state.lines.concat(pending);
        const excess = Math.max(0, all.length - MAX_LINES);
        return { lines: excess ? all.slice(excess) : all, dropped: state.dropped + excess };
      },
      () => {
        if (pinned && this.pane.current) this.pane.current.scrollTop = this.pane.current.scrollHeight;
      }
    );
  };

  private renderLines(lines: LogLine[], dropped: number) {
    return (
      <>
        {dropped > 0 && (
          <div className="atlas-logline atlas-muted">… {dropped.toLocaleString()} earlier lines dropped</div>
        )}
        {lines.map((line, i) => (
          // Keys follow the line, not its position, so dropping old lines
          // does not remount the rest.
          <LogLineView key={dropped + i} line={line} />
        ))}
        {this.partial && <div className="atlas-logline">{this.partial}</div>}
      </>
    );
  }

  render() {
    const containers = this.props.pod.containers;
    const { lines, dropped, raw, errorMessage, streaming } = this.state;
    const empty = lines.length === 0 && !this.partial;
    return (
      <div className="atlas-section">
        <h3>Logs</h3>
        <div className="atlas-card">
          <div className="atlas-logbar">
            {containers.length > 1 ? (
              <Select value={this.state.container} onChange={(e) => this.setState({ container: e.target.value })}>
                {containers.map((c) => (
                  <Option key={c} value={c}>
                    {c}
                  </Option>
                ))}
              </Select>
            ) : (
              <span className="atlas-muted atlas-small">{containers[0] ?? ""}</span>
            )}
            <Select value={this.state.tailLines} onChange={(e) => this.setState({ tailLines: e.target.value })}>
              {TAIL_OPTIONS.map((n) => (
                <Option key={n} value={n}>
                  {n} lines
                </Option>
              ))}
            </Select>
            <label>
              <Checkbox checked={this.state.follow} onChange={(e) => this.setState({ follow: e.target.checked })} />{" "}
              follow
            </label>
            <label>
              <Checkbox checked={this.state.previous} onChange={(e) => this.setState({ previous: e.target.checked })} />{" "}
              previous run
            </label>
            <label>
              <Checkbox checked={raw} onChange={(e) => this.setState({ raw: e.target.checked })} /> raw
            </label>
            <OutlinedButton className="atlas-small-button" onClick={() => this.start()}>
              <RefreshCw className="icon" /> reload
            </OutlinedButton>
          </div>
          <pre ref={this.pane} className="atlas-logpane">
            {errorMessage ??
              (empty
                ? streaming
                  ? "…"
                  : "(no output)"
                : raw
                  ? rawText(lines, this.partial)
                  : this.renderLines(lines, dropped))}
          </pre>
        </div>
      </div>
    );
  }
}

/** The kept lines as they were written. */
function rawText(lines: LogLine[], partial: string): string {
  return lines.map((l) => l.text).join("\n") + (partial ? "\n" + partial : "");
}

/**
 * One line: structured entries get a time, a severity column and dimmed
 * details. A line never changes once parsed, so memo skips it on later chunks.
 */
const LogLineView = React.memo(function LogLineView({ line }: { line: LogLine }) {
  if (line.kind === "text") {
    return <div className="atlas-logline">{line.text || " "}</div>;
  }
  return (
    <div className={`atlas-logline ${severityTone(line.severity)}`}>
      {line.time && (
        <span className="atlas-log-time" title={line.time.toISOString()}>
          {clockTime(line.time)}
        </span>
      )}
      <span className="atlas-log-severity">{line.severity.slice(0, 5)}</span>
      <span className="atlas-log-message">{line.message}</span>
      {line.source && <span className="atlas-log-source">{line.source}</span>}
      {line.fields.map(([k, v]) => (
        <span className="atlas-log-field" key={k}>
          <span className="atlas-log-key">{k}=</span>
          {v}
        </span>
      ))}
    </div>
  );
});
