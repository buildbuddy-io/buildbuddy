import Long from "long";
import { ArrowDown, ArrowUp, ChevronDown, ChevronRight, Network } from "lucide-react";
import React from "react";
import { execution_graph_analysis } from "../../proto/execution_graph_analysis_ts_proto";
import { OutlinedButton } from "../components/button/button";
import { Link } from "../components/link/link";
import Spinner from "../components/spinner/spinner";
import error_service from "../errors/error_service";
import * as format from "../format/format";
import router from "../router/router";
import rpcService from "../service/rpc_service";
import { BuildBuddyError } from "../util/errors";
import InvocationModel from "./invocation_model";

type Node = execution_graph_analysis.Node;

interface Props {
  model: InvocationModel;
}

interface Sort {
  col: string;
  desc: boolean;
}

interface State {
  loading: boolean;
  notFound?: boolean;
  analysis?: execution_graph_analysis.ExecutionGraphAnalysis;
  showAllSteps: boolean;
  // Sort state per table id; undefined means the table's natural order.
  sorts: Record<string, Sort | undefined>;
  // Key of the expanded "new critical path" row, e.g. "factor:Genrule",
  // "node:5", or "edge:6".
  expandedPath?: string;
  // Measured pixel width of the timeline container, so the SVG renders 1:1
  // instead of scaling its viewBox (which would scale fonts too).
  timelineWidth: number;
}

// Cap on the number of rows drawn in the timeline when showing all steps; the
// longest-running steps and every critical path step are always kept.
const MAX_TIMELINE_ROWS = 200;
// Cap on the number of factor rows shown.
const MAX_FACTOR_ROWS = 50;

const TIMELINE_MIN_WIDTH = 700;
const TIMELINE_ROW_HEIGHT = 30;
const TIMELINE_BAR_HEIGHT = 16;
const TIMELINE_AXIS_HEIGHT = 24;

// Timing components of a step (Node.component_millis keys), in display order.
// Rendered with the .comp-<key> CSS classes.
const COMPONENTS: { key: string; label: string; desc: string }[] = [
  { key: "queue", label: "Queue", desc: "Waiting for an available remote executor." },
  { key: "setup", label: "Setup", desc: "Setting up the action's inputs and environment on the remote executor." },
  { key: "parse", label: "Parse", desc: "Bazel preparing the action for remote execution." },
  { key: "fetch", label: "Fetch", desc: "Downloading action outputs from the remote cache." },
  { key: "network", label: "Network", desc: "Network transfer time." },
  { key: "process", label: "Process", desc: "Running the action's command." },
  { key: "upload", label: "Upload", desc: "Uploading action outputs from the remote executor to the cache." },
  { key: "process_outputs", label: "Outputs", desc: "Bazel processing and registering action outputs." },
  { key: "retry", label: "Retry", desc: "Time spent on failed attempts that were retried." },
  { key: "discover_inputs", label: "Discover inputs", desc: "Bazel discovering the action's inputs." },
  { key: "other", label: "Other", desc: "Unattributed remote execution time." },
];

// Descriptions shown under well-known factor names.
const FACTOR_DESCRIPTIONS = new Map<string, string>([
  ...COMPONENTS.map((c): [string, string] => [c.label, c.desc]),
  ["Other", "Unattributed remote execution time (action duration minus all measured components)."],
  ["Bazel overhead", "Client-side (Bazel) overhead: Parse + Outputs + Discover inputs."],
  ["Remote overhead", "Remote execution overhead: Setup + Fetch + Upload + Retry."],
  ["Bazel startup", "Bazel launch and initialization, before analysis."],
  ["Analysis phase", "Loading and analysis, before execution could start."],
  ["Finalization", "Bazel work after the last action finished (e.g. output download)."],
]);

function toNumber(value: number | Long | null | undefined): number {
  if (!value) return 0;
  return typeof value === "number" ? value : value.toNumber();
}

function nodeName(node?: Node | null): string {
  if (!node) return "";
  return node.targetLabel || node.description || node.mnemonic;
}

function nodeSubtitle(node?: Node | null): string {
  if (!node) return "";
  // The identifier is often an opaque digest, so prefer the description.
  const description = node.description !== nodeName(node) ? node.description : "";
  return [node.mnemonic, description].filter((s) => s).join(" · ");
}

function nodeRunner(node?: Node | null): string {
  if (!node?.runner) return "";
  return node.runner + (node.runnerSubtype ? "/" + node.runnerSubtype : "");
}

// Where a step's identifier (the action digest) links to: cache hits link to
// the cache tab's request search, executed steps to the executions tab.
function nodeLinkHref(node: Node): string {
  if (!node.identifier) return "";
  if (node.runner === "remote cache hit") {
    return `${window.location.pathname}?search=${encodeURIComponent(node.identifier)}#cache`;
  }
  return `${window.location.pathname}?executionFilter=${encodeURIComponent(node.identifier)}#execution`;
}

function nodeLinkHint(node: Node): string {
  if (!node.identifier) return "";
  return node.runner === "remote cache hit" ? "Click to view the cache request" : "Click to view the execution";
}

// A plain-text tooltip listing everything we know about a step: identity
// fields first, then a blank line, then the timing metrics.
function nodeTooltip(node: Node, t0: number): string {
  const format_lines = (lines: [string, string][]) =>
    lines
      .filter(([, value]) => value)
      .map(([label, value]) => `${label}: ${value}`)
      .join("\n");
  const identity = format_lines([
    ["target", node.targetLabel],
    ["description", node.description],
    ["mnemonic", node.mnemonic],
    ["runner", nodeRunner(node)],
    ["identifier", node.identifier],
    ["retry of", node.retryOfIndex !== null && node.retryOfIndex !== undefined ? `step #${node.retryOfIndex}` : ""],
  ]);
  const metrics = format_lines([
    ["start", t0 ? "+" + format.durationMillis(toNumber(node.startTimestampMillis) - t0) : ""],
    ["duration", format.durationMillis(toNumber(node.durationMillis))],
    ...componentsOf(node).map((c): [string, string] => [c.label.toLowerCase(), format.durationMillis(c.millis)]),
  ]);
  return [identity, metrics].filter((s) => s).join("\n\n");
}

function componentsOf(node?: Node | null): { key: string; label: string; desc: string; millis: number }[] {
  if (!node?.componentMillis) return [];
  return COMPONENTS.map((c) => ({ ...c, millis: toNumber(node.componentMillis[c.key]) })).filter((c) => c.millis > 0);
}

function factorDescription(fd: execution_graph_analysis.FactorDrag): string {
  const known = FACTOR_DESCRIPTIONS.get(fd.factor);
  if (known) return known;
  switch (fd.type) {
    case execution_graph_analysis.FactorType.MNEMONIC:
      return "Process time of all actions with this mnemonic.";
    case execution_graph_analysis.FactorType.RULE_CLASS:
      return "Process time of all actions with this rule class.";
    case execution_graph_analysis.FactorType.TARGET:
      return "Process time of all of this target's actions.";
    case execution_graph_analysis.FactorType.RUNNER:
      return "All time of steps executed by this runner.";
    case execution_graph_analysis.FactorType.RUNNER_SUBTYPE:
      return "All time of steps executed by this runner subtype; rolls up to the runner (drags are not additive).";
    case execution_graph_analysis.FactorType.FLAKY_TEST:
      return "Test attempts that failed and were retried — time wasted on flaky tests.";
    default:
      return "";
  }
}

function factorTypeLabel(type: execution_graph_analysis.FactorType | null | undefined): string {
  switch (type) {
    case execution_graph_analysis.FactorType.COMPONENT:
      return "component";
    case execution_graph_analysis.FactorType.OVERHEAD:
      return "overhead";
    case execution_graph_analysis.FactorType.MNEMONIC:
      return "mnemonic";
    case execution_graph_analysis.FactorType.RULE_CLASS:
      return "rule class";
    case execution_graph_analysis.FactorType.TARGET:
      return "target";
    case execution_graph_analysis.FactorType.PHASE:
      return "phase";
    case execution_graph_analysis.FactorType.RUNNER:
      return "runner";
    case execution_graph_analysis.FactorType.RUNNER_SUBTYPE:
      return "runner subtype";
    case execution_graph_analysis.FactorType.FLAKY_TEST:
      return "flaky test";
    default:
      return "";
  }
}

// A row of a critical path table.
interface PathRow {
  pathIndex: number;
  node: Node;
  dragMillis?: number;
  edgeDragMillis?: number;
  // The critical path that would result from zeroing this node / removing
  // the edge from the previous step, with titles for the expanded view.
  dragPath?: execution_graph_analysis.ICriticalPath | null;
  edgePath?: execution_graph_analysis.ICriticalPath | null;
  edgeTitle?: string;
}

export default class ExecutionGraphCardComponent extends React.Component<Props, State> {
  state: State = { loading: true, showAllSteps: false, sorts: {}, timelineWidth: 0 };

  private timelineRef = React.createRef<HTMLDivElement>();
  private onResize = () => this.measureTimeline();

  componentDidMount() {
    this.fetch();
    window.addEventListener("resize", this.onResize);
  }

  componentWillUnmount() {
    window.removeEventListener("resize", this.onResize);
  }

  componentDidUpdate(prevProps: Props) {
    if (prevProps.model !== this.props.model) {
      this.fetch();
    }
    this.measureTimeline();
  }

  private measureTimeline() {
    const width = this.timelineRef.current?.clientWidth ?? 0;
    if (width > 0 && width !== this.state.timelineWidth) {
      this.setState({ timelineWidth: width });
    }
  }


  private fetch() {
    this.setState({ loading: true, notFound: false, analysis: undefined });
    rpcService.service
      .getExecutionGraphAnalysis(
        execution_graph_analysis.GetExecutionGraphAnalysisRequest.create({
          invocationId: this.props.model.getInvocationId(),
        })
      )
      .then((response) => {
        this.setState({
          analysis: response.analysis as execution_graph_analysis.ExecutionGraphAnalysis,
        });
      })
      .catch((e) => {
        const error = BuildBuddyError.parse(e);
        if (error.code === "NotFound") {
          this.setState({ notFound: true });
        } else {
          error_service.handleError(e);
        }
      })
      .finally(() => this.setState({ loading: false }));
  }

  // The earliest step start, used as the zero point for start offsets.
  private buildStartMillis(): number {
    const starts = (this.state.analysis?.nodes ?? [])
      .map((n) => toNumber(n.startTimestampMillis))
      .filter((start) => start > 0);
    return starts.length ? Math.min(...starts) : 0;
  }

  private setSort(tableId: string, col: string) {
    const sort = this.state.sorts[tableId];
    let next: Sort | undefined;
    if (sort?.col !== col) {
      next = { col, desc: true };
    } else if (sort.desc) {
      next = { col, desc: false };
    } else {
      next = undefined; // Back to the table's natural order.
    }
    this.setState({ sorts: { ...this.state.sorts, [tableId]: next } });
  }

  private sortableHeader(tableId: string, col: string, label: string, className: string) {
    const sort = this.state.sorts[tableId];
    return (
      <div
        className={`${className} sortable-header`}
        role="button"
        title="Click to sort"
        onClick={() => this.setSort(tableId, col)}>
        {label}
        {sort?.col === col && (sort.desc ? <ArrowDown className="sort-icon" /> : <ArrowUp className="sort-icon" />)}
      </div>
    );
  }

  private sortRows<T>(tableId: string, rows: T[], values: Record<string, (row: T) => number | string>): T[] {
    const sort = this.state.sorts[tableId];
    if (!sort) return rows;
    const value = values[sort.col];
    if (!value) return rows;
    return [...rows].sort((a, b) => {
      const va = value(a);
      const vb = value(b);
      let cmp: number;
      if (typeof va === "string" || typeof vb === "string") {
        cmp = String(va).localeCompare(String(vb));
      } else {
        cmp = va - vb;
      }
      return sort.desc ? -cmp : cmp;
    });
  }

  render() {
    if (this.state.loading) {
      return (
        <div className="card execution-graph-card">
          <Spinner />
        </div>
      );
    }
    if (this.state.notFound || !this.state.analysis) {
      return (
        <div className="card execution-graph-card">
          <Network className="icon" />
          <div className="content">
            <div className="title">Execution graph analysis</div>
            <div className="details">
              No analysis is available for this invocation. Analyses are computed for completed invocations that were
              run with <code>--experimental_enable_execution_graph_log</code> (plus{" "}
              <code>--experimental_execution_graph_log_dep_type=all</code>), once the execution graph analysis worker
              has processed them.
            </div>
          </div>
        </div>
      );
    }
    const analysis = this.state.analysis;
    return (
      <div className="execution-graph-tab">
        {this.renderSummary(analysis)}
        {this.renderTimeline(analysis)}
        {this.renderCriticalPath(analysis)}
        {this.renderFactors(analysis)}
        {this.renderTargetDepDrags(analysis)}
      </div>
    );
  }

  private renderSummary(analysis: execution_graph_analysis.ExecutionGraphAnalysis) {
    const cpMillis = toNumber(analysis.criticalPath?.durationMillis);
    const invocationMillis = toNumber(analysis.invocationDurationMillis);
    return (
      <div className="card execution-graph-card">
        <Network className="icon" />
        <div className="content">
          <div className="title">Execution graph analysis</div>
          <div className="details">
            The critical path is the longest chain of dependent steps in this build; the invocation cannot be faster
            than it. Drag is how much faster the build would have been if a step, dependency edge, or factor took no
            time — it is capped by whatever else runs in parallel.
          </div>
          <div className="execution-graph-summary">
            <div className="summary-tile">
              <div className="summary-value">{format.durationMillis(cpMillis)}</div>
              <div className="summary-label">critical path</div>
            </div>
            <div className="summary-tile">
              <div className="summary-value">
                {invocationMillis > 0 ? format.percent(cpMillis / invocationMillis) + "%" : "—"}
              </div>
              <div className="summary-label">of invocation duration</div>
            </div>
            <div className="summary-tile">
              <div className="summary-value">{analysis.criticalPath?.nodeIndex.length ?? 0}</div>
              <div className="summary-label">steps on the path</div>
            </div>
            <div className="summary-tile">
              <div className="summary-value">
                {analysis.numNodes} / {toNumber(analysis.numEdges)}
              </div>
              <div className="summary-label">steps / edges analyzed</div>
            </div>
          </div>
        </div>
      </div>
    );
  }

  private renderTimeline(analysis: execution_graph_analysis.ExecutionGraphAnalysis) {
    const onPath = new Set(analysis.criticalPath?.nodeIndex ?? []);
    let nodes: Node[];
    let capped = false;
    if (this.state.showAllSteps) {
      nodes = [...analysis.nodes];
      if (nodes.length > MAX_TIMELINE_ROWS) {
        capped = true;
        const byDuration = [...nodes].sort((a, b) => toNumber(b.durationMillis) - toNumber(a.durationMillis));
        const keep = new Set<Node>(nodes.filter((n) => onPath.has(n.index)));
        for (const n of byDuration) {
          if (keep.size >= MAX_TIMELINE_ROWS) break;
          keep.add(n);
        }
        nodes = nodes.filter((n) => keep.has(n));
      }
    } else {
      nodes = analysis.nodes.filter((n) => onPath.has(n.index));
    }
    nodes.sort((a, b) => toNumber(a.startTimestampMillis) - toNumber(b.startTimestampMillis));
    const t0 = Math.min(...nodes.map((n) => toNumber(n.startTimestampMillis)));
    const t1 = Math.max(...nodes.map((n) => toNumber(n.startTimestampMillis) + toNumber(n.durationMillis)));
    const span = Math.max(t1 - t0, 1);
    // Render the SVG at the container's measured pixel width so text renders
    // at its CSS size instead of scaling with the viewBox.
    const width = Math.max(this.state.timelineWidth, TIMELINE_MIN_WIDTH);
    const x = (millis: number) => ((millis - t0) / span) * width;
    const height = nodes.length * TIMELINE_ROW_HEIGHT + TIMELINE_AXIS_HEIGHT;

    return (
      <div className="card execution-graph-card">
        <div className="content">
          <div className="execution-graph-section-header">
            <div>
              <div className="title">Timeline</div>
              <div className="details">
                Each bar is one step, placed at its actual start time. Blue bars are on the critical path
                {capped && (
                  <>
                    {" "}
                    (showing the {MAX_TIMELINE_ROWS} longest of {analysis.nodes.length} steps)
                  </>
                )}
                .
              </div>
            </div>
            <OutlinedButton
              className="execution-graph-toggle"
              onClick={() => this.setState({ showAllSteps: !this.state.showAllSteps })}>
              {this.state.showAllSteps
                ? "Show critical path only"
                : `Show all ${analysis.nodes.length} steps`}
            </OutlinedButton>
          </div>
          <div className="execution-graph-scroll" ref={this.timelineRef}>
            <svg
              viewBox={`0 0 ${width} ${height}`}
              width={width}
              height={height}
              role="img"
              aria-label="Timeline of build steps with the critical path highlighted">
              {[0.25, 0.5, 0.75].map((f) => (
                <line
                  key={f}
                  x1={f * width}
                  y1={0}
                  x2={f * width}
                  y2={height - TIMELINE_AXIS_HEIGHT + 4}
                  className="execution-graph-gridline"
                />
              ))}
              {[0, 0.25, 0.5, 0.75, 1].map((f) => (
                <text
                  key={f}
                  x={Math.min(f * width, width - 2)}
                  y={height - 6}
                  textAnchor={f === 0 ? "start" : "end"}
                  className="execution-graph-axis-label">
                  {format.durationMillis(f * span)}
                </text>
              ))}
              {nodes.map((n, row) => {
                const y = row * TIMELINE_ROW_HEIGHT + (TIMELINE_ROW_HEIGHT - TIMELINE_BAR_HEIGHT) / 2;
                const start = toNumber(n.startTimestampMillis);
                const dur = toNumber(n.durationMillis);
                const barStart = x(start);
                const barEnd = Math.max(x(start + dur), barStart + 2);
                // Draw the labels in the empty space beside the bar: to the
                // right when there's room, otherwise to the left.
                const labelOnRight = barEnd < width * 0.72;
                const labelX = labelOnRight ? barEnd + 8 : barStart - 8;
                const name = nodeName(n);
                const subtitle = nodeSubtitle(n);
                const href = nodeLinkHref(n);
                const title = nodeTooltip(n, t0) + (href ? `\n\n${nodeLinkHint(n)} (label)` : "");
                return (
                  <g key={n.index}>
                    <rect
                      x={barStart}
                      y={y}
                      width={barEnd - barStart}
                      height={TIMELINE_BAR_HEIGHT}
                      rx={3}
                      className={`execution-graph-bar ${onPath.has(n.index) ? "on-path" : ""} ${
                        n.synthetic ? "synthetic" : ""
                      }`}>
                      <title>{title}</title>
                    </rect>
                    <a
                      href={href || undefined}
                      onClick={
                        href
                          ? (e) => {
                              // Plain clicks navigate in-app; modified clicks
                              // (ctrl / middle / right) get default anchor
                              // behavior so "open in new tab" works.
                              if (e.metaKey || e.ctrlKey) return;
                              e.preventDefault();
                              router.navigateTo(href);
                            }
                          : undefined
                      }>
                      <text
                        x={labelX}
                        y={y + 7}
                        textAnchor={labelOnRight ? "start" : "end"}
                        className={`execution-graph-row-label ${onPath.has(n.index) ? "on-path" : ""} ${
                          href ? "linked" : ""
                        }`}>
                        <title>{title}</title>
                        {name}
                      </text>
                    </a>
                    {subtitle && (
                      <text
                        x={labelX}
                        y={y + 16}
                        textAnchor={labelOnRight ? "start" : "end"}
                        className="execution-graph-row-sublabel">
                        {subtitle}
                      </text>
                    )}
                  </g>
                );
              })}
            </svg>
          </div>
        </div>
      </div>
    );
  }

  private renderComponentLegend(rows: PathRow[]) {
    const present = new Set(rows.flatMap((r) => componentsOf(r.node).map((c) => c.key)));
    return (
      <div className="execution-graph-legend">
        {COMPONENTS.filter((c) => present.has(c.key)).map((c) => (
          <span className="legend-key" key={c.key} title={c.desc}>
            <span className={`legend-swatch comp-${c.key}`}></span>
            {c.label}
          </span>
        ))}
      </div>
    );
  }

  // Renders a table of path steps: name, start offset, duration with a
  // stacked per-component bar, and (for the main critical path) drag columns.
  private renderPathTable(tableId: string, rows: PathRow[], { showDrag }: { showDrag: boolean }) {
    const buildStart = this.buildStartMillis();
    const maxDur = Math.max(...rows.map((r) => toNumber(r.node.durationMillis)), 1);
    const sorted = this.sortRows(tableId, rows, {
      "#": (r) => r.pathIndex,
      step: (r) => nodeName(r.node),
      runner: (r) => nodeRunner(r.node),
      start: (r) => toNumber(r.node.startTimestampMillis),
      duration: (r) => toNumber(r.node.durationMillis),
      drag: (r) => r.dragMillis ?? 0,
      edge: (r) => r.edgeDragMillis ?? 0,
    });
    return (
      <div className="execution-graph-table">
        <div className="execution-graph-table-header">
          {this.sortableHeader(tableId, "#", "#", "col-index")}
          {this.sortableHeader(tableId, "step", "Step", "col-name")}
          {this.sortableHeader(tableId, "runner", "Runner", "col-runner")}
          {this.sortableHeader(tableId, "start", "Start", "col-num")}
          {this.sortableHeader(tableId, "duration", "Duration", "col-num")}
          <div className="col-bar">Composition</div>
          {showDrag && this.sortableHeader(tableId, "drag", "Drag", "col-num")}
          {showDrag && this.sortableHeader(tableId, "edge", "Edge drag", "col-num")}
        </div>
        {sorted.map((r) => {
          const dur = toNumber(r.node.durationMillis);
          const start = toNumber(r.node.startTimestampMillis);
          const comps = componentsOf(r.node);
          const nodePathKey = `node:${r.node.index}`;
          const edgePathKey = `edge:${r.node.index}`;
          return (
            <React.Fragment key={r.node.index}>
              <div className="execution-graph-table-row">
                <div className="col-index">{r.pathIndex + 1}</div>
                <div
                  className="col-name"
                  title={nodeTooltip(r.node, buildStart) + (r.node.identifier ? `\n\n${nodeLinkHint(r.node)}` : "")}>
                  {r.node.identifier ? (
                    <Link className="step-name linked" href={nodeLinkHref(r.node)}>
                      {nodeName(r.node)}
                    </Link>
                  ) : (
                    <div className="step-name">{nodeName(r.node)}</div>
                  )}
                  {nodeSubtitle(r.node) && <div className="step-subtitle">{nodeSubtitle(r.node)}</div>}
                </div>
                <div className="col-runner" title={nodeRunner(r.node)}>
                  {nodeRunner(r.node) || "—"}
                </div>
                <div className="col-num">{buildStart ? "+" + format.durationMillis(start - buildStart) : "—"}</div>
                <div className="col-num">{format.durationMillis(dur)}</div>
                <div className="col-bar">
                  <div className="composition-bar" style={{ width: `${(dur / maxDur) * 100}%` }}>
                    {comps.map((c) => (
                      <div
                        key={c.key}
                        className={`composition-segment comp-${c.key}`}
                        style={{ flexGrow: c.millis }}
                        title={`${c.label}: ${format.durationMillis(c.millis)} (${format.percent(c.millis / dur)}%)`}
                      />
                    ))}
                    {!comps.length && <div className="composition-segment comp-synthetic" style={{ flexGrow: 1 }} />}
                  </div>
                </div>
                {showDrag &&
                  this.pathToggleCell(
                    "col-num",
                    r.dragMillis ?? 0,
                    r.dragPath,
                    nodePathKey,
                    (r.dragMillis ?? 0) > 0,
                    `Drag of "${nodeName(r.node)}"`
                  )}
                {showDrag &&
                  this.pathToggleCell(
                    "col-num",
                    r.edgeDragMillis,
                    r.edgePath,
                    edgePathKey,
                    (r.edgeDragMillis ?? 0) > 0,
                    r.edgeTitle?.replace("Critical path without the dependency", "Drag of the dependency")
                  )}
              </div>
              {showDrag && this.state.expandedPath === nodePathKey && r.dragPath && (
                <div className="execution-graph-expanded-path">
                  <div className="expanded-path-title">
                    Critical path if "{nodeName(r.node)}" took no time (
                    {format.durationMillis(toNumber(r.dragPath.durationMillis))})
                  </div>
                  {this.renderExpandedPath(nodePathKey, r.dragPath)}
                </div>
              )}
              {showDrag && this.state.expandedPath === edgePathKey && r.edgePath && (
                <div className="execution-graph-expanded-path">
                  <div className="expanded-path-title">
                    {r.edgeTitle} ({format.durationMillis(toNumber(r.edgePath.durationMillis))})
                  </div>
                  {this.renderExpandedPath(edgePathKey, r.edgePath)}
                </div>
              )}
            </React.Fragment>
          );
        })}
      </div>
    );
  }

  // Renders a drag value cell that expands the resulting new critical path
  // when clicked. The title identifies what the drag refers to even when the
  // table is sorted out of path order.
  private pathToggleCell(
    className: string,
    millis: number | undefined,
    path: execution_graph_analysis.ICriticalPath | null | undefined,
    pathKey: string,
    expandable: boolean,
    title?: string
  ) {
    if (millis === undefined) {
      return <div className={className}>—</div>;
    }
    if (!expandable || !path) {
      return (
        <div className={className} title={title}>
          {format.durationMillis(millis)}
        </div>
      );
    }
    const expanded = this.state.expandedPath === pathKey;
    return (
      <div className={className}>
        <div
          className="newpath-toggle"
          role="button"
          title={(title ? title + "\n" : "") + "Click to show the resulting critical path"}
          onClick={() => this.setState({ expandedPath: expanded ? undefined : pathKey })}>
          {expanded ? <ChevronDown className="chevron" /> : <ChevronRight className="chevron" />}
          {format.durationMillis(millis)}
        </div>
      </div>
    );
  }

  private renderExpandedPath(pathKey: string, path: execution_graph_analysis.ICriticalPath) {
    const analysis = this.state.analysis;
    if (!analysis) return null;
    return this.renderPathTable(`${pathKey}-table`, this.pathRows(analysis, path, false), { showDrag: false });
  }

  private pathRows(
    analysis: execution_graph_analysis.ExecutionGraphAnalysis,
    path: execution_graph_analysis.ICriticalPath | null | undefined,
    withDrags: boolean
  ): PathRow[] {
    const nodesByIndex = new Map(analysis.nodes.map((n) => [n.index, n]));
    const edgeDragByNode = new Map(analysis.edgeDrags.map((e) => [e.nodeIndex, e]));
    return (path?.nodeIndex ?? [])
      .map((index, i): PathRow | undefined => {
        const node = nodesByIndex.get(index);
        if (!node) return undefined;
        const row: PathRow = { pathIndex: i, node: node as Node };
        if (withDrags) {
          row.dragMillis = toNumber(analysis.nodeDrags[i]?.dragMillis);
          row.dragPath = analysis.nodeDrags[i]?.newCriticalPath;
          const edge = edgeDragByNode.get(index);
          if (edge) {
            row.edgeDragMillis = toNumber(edge.dragMillis);
            row.edgePath = edge.newCriticalPath;
            // "A → B" reads as "A depends on B".
            row.edgeTitle = `Critical path without the dependency "${nodeName(node)}" → "${nodeName(
              nodesByIndex.get(edge.depIndex)
            )}"`;
          }
        }
        return row;
      })
      .filter((r): r is PathRow => !!r);
  }

  private renderCriticalPath(analysis: execution_graph_analysis.ExecutionGraphAnalysis) {
    const rows = this.pathRows(analysis, analysis.criticalPath, true);
    return (
      <div className="card execution-graph-card">
        <div className="content">
          <div className="title">Critical path</div>
          <div className="details">
            Speeding up a step only helps up to its drag; past that, a parallel branch becomes the new critical path.
            Edge drag is the speedup from removing the dependency on the previous step itself (e.g. by decoupling the
            inputs).
          </div>
          {this.renderComponentLegend(rows)}
          {this.renderPathTable("critical-path", rows, { showDrag: true })}
        </div>
      </div>
    );
  }

  // Drag of removing every dependency ON a target, for critical-path
  // targets.
  private renderTargetDepDrags(analysis: execution_graph_analysis.ExecutionGraphAnalysis) {
    if (!analysis.targetDepDrags.length) return null;
    const cpMillis = Math.max(toNumber(analysis.criticalPath?.durationMillis), 1);
    const tableId = "target-deps";
    const rows = this.sortRows(tableId, analysis.targetDepDrags, {
      target: (td) => td.targetLabel,
      drag: (td) => toNumber(td.dragMillis),
      percent: (td) => toNumber(td.dragMillis),
    });
    return (
      <div className="card execution-graph-card">
        <div className="content">
          <div className="title">Removing all dependencies on a target</div>
          <div className="details">
            How much shorter would the critical path be if no step had to wait for any of this target's steps?
          </div>
          <div className="execution-graph-table">
            <div className="execution-graph-table-header">
              {this.sortableHeader(tableId, "target", "Target", "col-name")}
              {this.sortableHeader(tableId, "drag", "Drag", "col-num")}
              {this.sortableHeader(tableId, "percent", "% of path", "col-num")}
              <div className="col-newpath">New critical path</div>
            </div>
            {rows.map((td) => {
              const pathKey = `tdeps:${td.targetLabel}`;
              const expanded = this.state.expandedPath === pathKey;
              return (
                <React.Fragment key={td.targetLabel}>
                  <div className="execution-graph-table-row">
                    <div className="col-name">
                      <div className="step-name" title={td.targetLabel}>
                        {td.targetLabel}
                      </div>
                    </div>
                    <div className="col-num">{format.durationMillis(toNumber(td.dragMillis))}</div>
                    <div className="col-num">{format.percent(toNumber(td.dragMillis) / cpMillis)}%</div>
                    <div className="col-newpath">
                      {td.newCriticalPath ? (
                        <div
                          className="newpath-toggle"
                          role="button"
                          onClick={() => this.setState({ expandedPath: expanded ? undefined : pathKey })}>
                          {expanded ? <ChevronDown className="chevron" /> : <ChevronRight className="chevron" />}
                          {format.durationMillis(toNumber(td.newCriticalPath.durationMillis))}
                        </div>
                      ) : (
                        "—"
                      )}
                    </div>
                  </div>
                  {expanded && td.newCriticalPath && (
                    <div className="execution-graph-expanded-path">
                      <div className="expanded-path-title">
                        Critical path if nothing waited on "{td.targetLabel}" (
                        {format.durationMillis(toNumber(td.newCriticalPath.durationMillis))})
                      </div>
                      {this.renderExpandedPath(pathKey, td.newCriticalPath)}
                    </div>
                  )}
                </React.Fragment>
              );
            })}
          </div>
        </div>
      </div>
    );
  }

  private renderFactors(analysis: execution_graph_analysis.ExecutionGraphAnalysis) {
    const cpMillis = Math.max(toNumber(analysis.criticalPath?.durationMillis), 1);
    const tableId = "factors";
    const factors = this.sortRows(tableId, analysis.factorDrags.slice(0, MAX_FACTOR_ROWS), {
      factor: (fd) => fd.factor,
      type: (fd) => factorTypeLabel(fd.type),
      total: (fd) => toNumber(fd.totalMillis),
      cp: (fd) => toNumber(fd.criticalPathMillis),
      drag: (fd) => toNumber(fd.dragMillis),
      percent: (fd) => toNumber(fd.dragMillis),
    });
    return (
      <div className="card execution-graph-card">
        <div className="content">
          <div className="title">Drag by factor</div>
          <div className="details">
            How much faster would this build have been if a factor cost nothing anywhere in the build? Components
            (queue, network, …) and overhead groups are summed across all steps; mnemonics, rule classes, and targets
            cover the process time of their steps.
          </div>
          <div className="execution-graph-table">
            <div className="execution-graph-table-header">
              {this.sortableHeader(tableId, "factor", "Factor", "col-name")}
              {this.sortableHeader(tableId, "type", "Type", "col-type")}
              {this.sortableHeader(tableId, "total", "Total time", "col-num")}
              {this.sortableHeader(tableId, "cp", "On critical path", "col-num wide")}
              {this.sortableHeader(tableId, "drag", "Drag", "col-num")}
              {this.sortableHeader(tableId, "percent", "% of path", "col-num")}
              <div className="col-newpath">New critical path</div>
            </div>
            {factors.map((fd) => {
              const drag = toNumber(fd.dragMillis);
              const pathKey = `factor:${fd.factor}`;
              const expanded = this.state.expandedPath === pathKey;
              const expandable = drag > 0 && !!fd.newCriticalPath;
              return (
                <React.Fragment key={fd.factor}>
                  <div className="execution-graph-table-row">
                    <div className="col-name">
                      <div className="step-name" title={fd.factor}>
                        {fd.factor}
                      </div>
                      {factorDescription(fd as execution_graph_analysis.FactorDrag) && (
                        <div className="step-subtitle">
                          {factorDescription(fd as execution_graph_analysis.FactorDrag)}
                        </div>
                      )}
                    </div>
                    <div className="col-type">{factorTypeLabel(fd.type)}</div>
                    <div className="col-num">{format.durationMillis(toNumber(fd.totalMillis))}</div>
                    <div className="col-num wide">{format.durationMillis(toNumber(fd.criticalPathMillis))}</div>
                    <div className="col-num">{format.durationMillis(drag)}</div>
                    <div className="col-num">{format.percent(drag / cpMillis)}%</div>
                    <div className="col-newpath">
                      {expandable ? (
                        <div
                          className="newpath-toggle"
                          role="button"
                          onClick={() => this.setState({ expandedPath: expanded ? undefined : pathKey })}>
                          {expanded ? <ChevronDown className="chevron" /> : <ChevronRight className="chevron" />}
                          {format.durationMillis(toNumber(fd.newCriticalPath?.durationMillis))}
                        </div>
                      ) : (
                        "—"
                      )}
                    </div>
                  </div>
                  {expandable && expanded && fd.newCriticalPath && (
                    <div className="execution-graph-expanded-path">
                      <div className="expanded-path-title">
                        Critical path if "{fd.factor}" took no time (
                        {format.durationMillis(toNumber(fd.newCriticalPath?.durationMillis))})
                      </div>
                      {this.renderExpandedPath(pathKey, fd.newCriticalPath)}
                    </div>
                  )}
                </React.Fragment>
              );
            })}
          </div>
          {analysis.factorDrags.length > MAX_FACTOR_ROWS && (
            <div className="details">
              Showing the top {MAX_FACTOR_ROWS} of {analysis.factorDrags.length} factors.
            </div>
          )}
        </div>
      </div>
    );
  }
}
