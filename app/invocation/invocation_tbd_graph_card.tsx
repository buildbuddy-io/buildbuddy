import type { ElementDatum, EdgeData as G6EdgeData, NodeData as G6NodeData, ID, IElementEvent, IEvent } from "@antv/g6";
import { CanvasEvent, Graph as G6Graph, GraphEvent, NodeEvent } from "@antv/g6";
import {
  Dice5 as DiceIcon,
  File as FileIcon,
  Folder as FolderIcon,
  Info,
  Link as LinkIcon,
  Package as PackageIcon,
} from "lucide-react";
import React from "react";
import * as varint from "varint";
import { build } from "../../proto/remote_execution_ts_proto";
import { tools } from "../../proto/spawn_ts_proto";
import DigestComponent from "../components/digest/digest";
import Link, { TextLink } from "../components/link/link";
import SearchBar from "../components/search_bar/search_bar";
import rpcService from "../service/rpc_service";
import { digestToString } from "../util/cache";
import InvocationModel from "./invocation_model";

interface Props {
  model: InvocationModel;
  search: URLSearchParams;
  filter: string;
}

interface IndexedData {
  files: Map<number, tools.protos.ExecLogEntry.File>;
  directories: Map<number, tools.protos.ExecLogEntry.Directory>;
  symlinks: Map<number, tools.protos.ExecLogEntry.UnresolvedSymlink>;
  runfiles: Map<number, tools.protos.ExecLogEntry.RunfilesTree>;
  inputSets: Map<number, tools.protos.ExecLogEntry.InputSet>;
  spawns: Map<number, tools.protos.ExecLogEntry.Spawn>;
  symlinkActions: Map<number, tools.protos.ExecLogEntry.SymlinkAction>;
  symlinkEntrySets: Map<number, tools.protos.ExecLogEntry.SymlinkEntrySet>;
  labelSet: Set<string>;
  outputProducers: Map<number, number>; // artifact ID -> spawn ID
  expandedInputSets: Map<number, Set<number>>; // cache for expanded input sets
  consumedArtifacts: Set<number>; // artifact IDs that are consumed by any spawn (inputs or tools)
  pathToFileId: Map<string, number>;
  pathToDirId: Map<string, number>;
  pathToSymlinkId: Map<string, number>;
  // Artifacts produced by symlink actions and mapping to their producer action id
  symlinkProducedArtifacts: Set<number>;
  symlinkOutputProducers: Map<number, number>; // artifact ID -> symlink action ID
}

type ArtifactType = "file" | "directory" | "symlink" | "runfiles";
type ArtifactCategory = "source" | "tool" | "generated" | "intermediate";

interface GraphNode {
  id: string;
  type: "spawn" | "artifact";
  nodeId: number; // ID in the indexed data
  artifactType?: ArtifactType; // Only for artifact nodes
  artifactCategory?: ArtifactCategory; // Classification of artifact purpose
  label: string;
  isIntermediate?: boolean; // For artifacts that are both produced and consumed
  hasProducer?: boolean; // Whether this artifact is produced by a spawn in view
  hasConsumer?: boolean; // Whether this artifact is consumed by a spawn in view
  // Spawn-specific metadata for tooltips/coloring
  spawnHasValidOutputs?: boolean;
  spawnHasInvalidOutputPaths?: boolean;
  primaryOutputLabel?: string;
  platform?: string;
  isSymlinkAction?: boolean; // For symlink actions to use different coloring
  // Tool chain metadata
  isToolSpawn?: boolean; // Indicates this spawn uses tools from runfiles
  toolChain?: string[]; // Names of tools used (e.g., ["protoc", "protoc-gen-protobufjs"])
  inputSetHierarchy?: number[]; // Chain of input set IDs this node belongs to
  nonSelectable?: boolean; // For synthetic/placeholder nodes (e.g., unresolved paths)
}

interface GraphEdge {
  from: string;
  to: string;
  type: "input" | "output";
  // For input edges, indicate whether it comes from tools or sources
  inputRole?: "src" | "tool";
}

interface GraphData {
  nodes: GraphNode[];
  edges: GraphEdge[];
  spawnLayers: number[][];
  symlinkActions: [number, tools.protos.ExecLogEntry.SymlinkAction][];
  // Computed layer index for each symlink action (aligned with spawn layers)
  symlinkLayers: Map<number, number>;
}

interface State {
  loading: boolean;
  error: string | null;
  log: tools.protos.ExecLogEntry[];
  indexedData: IndexedData | null;
  selectedLabel: string | null;
  breadcrumbs: string[];
  selectedItem: { type: "spawn" | "artifact" | "symlink"; id: string | number } | null;
  orientation: "top-to-bottom" | "left-to-right";
  chartLoading: boolean;
  // Debug spacing parameters
  debugMode: boolean; // whether the panel is expanded
  debugEnabled: boolean; // whether debug UI is available (via URL param)
  interestingOnly: boolean; // whether to render only interesting artifacts
  expandedSpawns: Set<number>; // Set of spawn IDs that have all artifacts expanded
  spacing: {
    xGap: number;
    minRowHeight: number;
    artifactOffset: number;
    spawnSymbolSize: number;
    symlinkSymbolSize: number;
    innerMargin: number;
    rowGap: number;
    spawnBandGap: number;
    interestingArtifactSize: number;
    uninterestingArtifactSize: number;
    maxArtifactsPerSpawn: number;
    inputRectMaxHeight: number;
    inputRectRowSpacing: number;
    inputRectColSpacing: number;
    rowExtraPadding: number;
    spawnShape: "circle" | "rect" | "roundRect";
    symlinkShape: "circle" | "rect" | "roundRect";
    spawnWidth: number;
    spawnHeight: number;
    symlinkWidth: number;
    symlinkHeight: number;
  };
  detailsExpand: {
    inputs: boolean;
    tools: boolean;
    outputs: boolean;
    command: boolean;
    env?: boolean;
    platform?: boolean;
  };
}

export default class InvocationTbdGraphCardComponent extends React.Component<Props, State> {
  private chartRef = React.createRef<HTMLDivElement>();
  private g6Graph: G6Graph | null = null;
  private g6SelectionFocusTimeout: NodeJS.Timeout | null = null;
  private isComponentMounted = true;
  private suppressURLUpdate = false;
  private textMeasureCanvas: HTMLCanvasElement | null = null;
  private handleKeydownBound?: (e: KeyboardEvent) => void;

  constructor(props: Props) {
    super(props);
    this.state = {
      loading: false,
      error: null,
      log: [],
      indexedData: null,
      selectedLabel: null,
      breadcrumbs: [],
      selectedItem: null,
      orientation: "top-to-bottom",
      chartLoading: false,
      debugMode: false,
      debugEnabled: false,
      interestingOnly: true,
      expandedSpawns: new Set(),
      spacing: {
        xGap: 10000, // Much wider horizontal spacing for cleaner layout
        minRowHeight: 10000,
        artifactOffset: 84,
        spawnSymbolSize: 120,
        symlinkSymbolSize: 120,
        innerMargin: 24,
        rowGap: 1, // Minimal vertical spacing between rows
        spawnBandGap: 360,
        interestingArtifactSize: 30, // Interesting (orange) artifacts
        uninterestingArtifactSize: 20, // Uninteresting (grey) artifacts
        maxArtifactsPerSpawn: 800, // Maximum artifacts displayed per spawn
        inputRectMaxHeight: 5000, // Cap for the input block height (in px)
        inputRectRowSpacing: 800, // Vertical spacing between rows inside input rectangle
        inputRectColSpacing: 800, // Horizontal spacing between columns inside input rectangle
        rowExtraPadding: 0, // Extra padding added to each row height
        spawnShape: "circle",
        symlinkShape: "circle",
        spawnWidth: 160,
        spawnHeight: 100,
        symlinkWidth: 160,
        symlinkHeight: 100,
      },
      detailsExpand: { inputs: false, tools: false, outputs: false, command: false, env: false, platform: false },
    };
  }

  componentDidMount() {
    this.initializeFromURL();
    this.fetchExecutionLog();
    window.addEventListener("resize", this.handleResize);
    window.addEventListener("popstate", this.handlePopState);
    // Global up/down arrow navigation between labels when not focused in an input
    this.handleKeydownBound = this.handleGlobalKeyDown.bind(this);
    window.addEventListener("keydown", this.handleKeydownBound);
  }

  componentDidUpdate(_prevProps: Props, prevState: State) {
    if (prevState.selectedLabel !== this.state.selectedLabel || prevState.indexedData !== this.state.indexedData) {
      this.renderChart();
    }
    if (prevState.selectedItem !== this.state.selectedItem) {
      // Apply persistent hover-style focus without re-rendering the chart
      this.applySelectionFocus();
    }
    if (prevState.selectedItem !== this.state.selectedItem) {
      if (this.suppressURLUpdate) {
        this.suppressURLUpdate = false;
      } else {
        this.pushSelectionToURL();
      }
    }
  }

  componentWillUnmount() {
    this.isComponentMounted = false;
    if (this.g6Graph) {
      try {
        // Cancel any pending timeout to prevent operations on destroyed graph
        if (this.g6SelectionFocusTimeout) {
          clearTimeout(this.g6SelectionFocusTimeout);
          this.g6SelectionFocusTimeout = null;
        }
        this.g6Graph.destroy();
      } catch {}
      this.g6Graph = null;
    }
    window.removeEventListener("resize", this.handleResize);
    window.removeEventListener("popstate", this.handlePopState);
    if (this.handleKeydownBound) {
      window.removeEventListener("keydown", this.handleKeydownBound);
      this.handleKeydownBound = undefined;
    }
  }

  private isInteractiveElement(target: EventTarget | null): boolean {
    const el = target as HTMLElement | null;
    if (!el) return false;
    const tag = el.tagName?.toLowerCase();
    if (["input", "textarea", "select", "button"].includes(tag)) return true;
    if ((el as any).isContentEditable) return true;
    return false;
  }

  private handleGlobalKeyDown(e: KeyboardEvent) {
    if (this.isInteractiveElement(e.target)) return; // don't hijack when typing in inputs
    if (!this.state.indexedData) return;
    if (e.key === "ArrowDown") {
      this.navigateLabelDelta(1);
      e.preventDefault();
    } else if (e.key === "ArrowUp") {
      this.navigateLabelDelta(-1);
      e.preventDefault();
    }
  }

  private getFilteredLabels(): string[] {
    const { indexedData } = this.state;
    const filtered = indexedData
      ? Array.from(indexedData.labelSet)
          .filter((label) => !this.props.filter || label.includes(this.props.filter))
          .sort((a, b) => this.compareLabels(a || "", b || ""))
      : [];
    return filtered;
  }

  private navigateLabelDelta(delta: number) {
    const labels = this.getFilteredLabels();
    if (labels.length === 0) return;
    const current = this.state.selectedLabel;
    let idx = current ? labels.indexOf(current) : -1;
    if (idx === -1) {
      idx = delta > 0 ? -1 : 0; // if going next, start before first; if prev, start from 0 so prev goes to last
    }
    const nextIdx = (idx + delta + labels.length) % labels.length;
    const nextLabel = labels[nextIdx];
    this.handleTargetPicked(nextLabel);
  }

  private navigateLabelRandom() {
    const labels = this.getFilteredLabels();
    if (labels.length === 0) return;
    if (labels.length === 1) {
      this.handleTargetPicked(labels[0]);
      return;
    }
    const current = this.state.selectedLabel;
    let nextLabel = current;
    // Try a few times to pick a different label
    for (let i = 0; i < 5; i++) {
      const idx = Math.floor(Math.random() * labels.length);
      nextLabel = labels[idx];
      if (nextLabel !== current) break;
    }
    if (nextLabel) this.handleTargetPicked(nextLabel);
  }

  private handleResize = () => {
    if (this.g6Graph && this.chartRef.current) {
      try {
        const width = this.chartRef.current.clientWidth || 800;
        const height = this.chartRef.current.clientHeight || 600;
        this.g6Graph.setSize(width, height);
        // Keep the view fitted after resize
        try {
          const g: any = this.g6Graph as any;
          g.fitView?.();
          // Nudge zoom in a bit for readability after resize
          const z = g.getZoom?.();
          if (typeof z === "number") {
            const center = g.getCanvasCenter?.();
            g.zoomTo?.(Math.min(z * 1.2, 2.0), false, center);
          }
        } catch {}
      } catch {}
    }
  };

  // (zoom button handlers removed as part of revert)

  initializeFromURL() {
    const tbdLabel = this.props.search.get("tbdLabel");
    const tbdDebug = this.props.search.get("tbdDebug");
    const tbdSel = this.props.search.get("tbdSel");
    const tbdInteresting = this.props.search.get("tbdInteresting");
    if (tbdLabel) {
      this.setState({ selectedLabel: tbdLabel });
    }
    const debugOn = tbdDebug === "1";
    if (debugOn) {
      this.setState({ debugEnabled: true, debugMode: true });
    }
    if (tbdInteresting === "1") this.setState({ interestingOnly: true });
    if (tbdInteresting === "0") this.setState({ interestingOnly: false });
    if (tbdSel) {
      const sel = this.decodeSelectionParam(tbdSel);
      if (sel) this.setState({ selectedItem: sel });
    }
  }

  updateURL(label: string | null) {
    const url = new URL(window.location.href);
    if (label) {
      url.searchParams.set("tbdLabel", label);
    } else {
      url.searchParams.delete("tbdLabel");
    }
    // Persist interesting-only mode explicitly (default is true)
    url.searchParams.set("tbdInteresting", this.state.interestingOnly ? "1" : "0");
    window.history.pushState({}, "", url.toString());
  }

  private encodeSelectionParam(item: State["selectedItem"]): string | null {
    if (!item) return null;
    if (item.type === "spawn") return `spawn:${item.id}`;
    if (item.type === "symlink") return `symlink:${item.id}`;
    if (item.type === "artifact") return `artifact:${item.id}`;
    return null;
  }

  private decodeSelectionParam(sel: string): State["selectedItem"] | null {
    const m = sel.match(/^(spawn|symlink|artifact):(.+)$/);
    if (!m) return null;
    const kind = m[1] as "spawn" | "symlink" | "artifact";
    const idStr = m[2];
    const idNum = parseInt(idStr);
    const id: any = isNaN(idNum) ? idStr : idNum;
    return { type: kind, id };
  }

  private pushSelectionToURL() {
    const url = new URL(window.location.href);
    const selVal = this.encodeSelectionParam(this.state.selectedItem);
    if (selVal) url.searchParams.set("tbdSel", selVal);
    else url.searchParams.delete("tbdSel");
    window.history.pushState({}, "", url.toString());
  }

  private handlePopState = () => {
    const url = new URL(window.location.href);
    const label = url.searchParams.get("tbdLabel");
    const sel = url.searchParams.get("tbdSel");
    const selectedItem = sel ? this.decodeSelectionParam(sel) : null;
    const tbdInteresting = url.searchParams.get("tbdInteresting");
    let interestingOnly = this.state.interestingOnly;
    if (tbdInteresting === "1") interestingOnly = true;
    if (tbdInteresting === "0") interestingOnly = false;
    this.suppressURLUpdate = true;
    this.setState({ selectedLabel: label, selectedItem, interestingOnly });
  };

  private updateSpacing = (key: keyof State["spacing"], value: any) => {
    this.setState(
      (prevState) => ({
        spacing: {
          ...prevState.spacing,
          [key]: value,
        },
      }),
      () => {
        // Re-render chart with new spacing
        if (this.state.selectedLabel && this.state.indexedData) {
          this.renderChart();
        }
      }
    );
  };

  private toggleDebugMode = () => {
    this.setState((prevState) => ({
      debugMode: !prevState.debugMode,
    }));
  };

  private toggleSpawnExpansion = (spawnId: number) => {
    this.setState(
      (prevState) => {
        const newExpandedSpawns = new Set(prevState.expandedSpawns);
        if (newExpandedSpawns.has(spawnId)) {
          newExpandedSpawns.delete(spawnId);
        } else {
          newExpandedSpawns.add(spawnId);
        }
        return { expandedSpawns: newExpandedSpawns };
      },
      () => {
        // Re-render chart with new expansion state
        this.renderChart();
      }
    );
  };

  render() {
    const { loading, error } = this.state;

    if (loading) {
      return <div className="loading" />;
    }

    return (
      <div>
        <div className="card expanded tbd-graph-card">
          <div className="content">
            {this.renderHeader()}
            {error && (
              <div className="error-container">
                <div>Error: {error}</div>
              </div>
            )}
            {!error && this.renderGraphArea()}
          </div>
        </div>
        {this.renderDetailsCard()}
      </div>
    );
  }

  private renderHeader() {
    const { indexedData } = this.state;
    const filteredLabels = indexedData
      ? Array.from(indexedData.labelSet)
          .filter((label) => !this.props.filter || label.includes(this.props.filter))
          .sort((a, b) => this.compareLabels(a || "", b || ""))
      : [];

    return (
      <>
        <div className="invocation-content-header">
          <div className="invocation-sort-controls" style={{ width: "100%", justifyContent: "center" }}>
            <div style={{ width: "100%", maxWidth: 1024 }}>
              <div style={{ display: "flex", alignItems: "center", gap: 8 }}>
                <div style={{ flex: 1 }}>
                  <SearchBar<string>
                    title="No matches"
                    placeholder="Search target label"
                    initialValue={this.state.selectedLabel || ""}
                    fetchResults={this.fetchTargetResults}
                    renderResultWithQuery={(label, q) => this.renderMatchedLabel(label, q)}
                    onResultPicked={(label) => this.handleTargetPicked(label)}
                    overrideArrowNavigation={true}
                    onArrowDown={() => this.navigateLabelDelta(1)}
                    onArrowUp={() => this.navigateLabelDelta(-1)}
                    emptyState={
                      <div style={{ maxHeight: 360, overflowY: "auto" }}>
                        {filteredLabels.map((label) => (
                          <div
                            key={label}
                            className="search-bar-result"
                            onMouseDown={() => this.handleTargetPicked(label)}>
                            {label}
                          </div>
                        ))}
                      </div>
                    }
                  />
                </div>
                <div className="tbd-next-prev" style={{ display: "inline-flex", alignItems: "center" }}>
                  <button
                    className="tbd-prev-btn"
                    onClick={() => this.navigateLabelDelta(-1)}
                    title="Previous target (Arrow Up)">
                    ↑ Prev
                  </button>
                  <button
                    className="tbd-next-btn"
                    onClick={() => this.navigateLabelDelta(1)}
                    style={{ marginLeft: 6 }}
                    title="Next target (Arrow Down)">
                    Next ↓
                  </button>
                  <button
                    className="tbd-random-btn"
                    onClick={() => this.navigateLabelRandom()}
                    style={{ marginLeft: 6, display: "inline-flex", alignItems: "center", gap: 4 }}
                    title="Random target">
                    <DiceIcon className="icon" />
                  </button>
                </div>
              </div>
              <div style={{ display: "flex", justifyContent: "flex-end", marginTop: 8 }}>
                <label style={{ display: "inline-flex", alignItems: "center", gap: 6, fontSize: 12 }}>
                  <input
                    type="checkbox"
                    checked={!!this.state.interestingOnly}
                    onChange={(e) => this.toggleInterestingOnly(e.target.checked)}
                  />
                  <span title="Show only artifacts that are both produced and consumed">Interesting Only</span>
                </label>
              </div>
            </div>
          </div>
        </div>
        {(this.state.selectedLabel || this.state.breadcrumbs.length > 0) &&
          (() => {
            const crumbs = this.state.breadcrumbs;
            const current = this.state.selectedLabel || "";
            // Derive a constant character threshold from 3/4 of 1072px graph width at ~6.5px/char
            const totalChars = crumbs.join("").length + current.length;
            const multiLine = crumbs.length + (current ? 1 : 0) >= 4 || totalChars > 120;
            return (
              <div
                className={`breadcrumbs${multiLine ? " breadcrumbs-multiline" : ""}`}
                style={{
                  marginTop: 8,
                  fontSize: 11,
                  display: multiLine ? "flex" : undefined,
                  flexDirection: multiLine ? "column" : undefined,
                  gap: multiLine ? 2 : undefined,
                }}>
                {crumbs.map((crumb, index) => (
                  <span key={`crumb-wrap-${index}`}>
                    <TextLink
                      className="breadcrumb"
                      href={this.getTbdUrlForLabel(crumb)}
                      onClick={(e) => {
                        e.preventDefault();
                        this.handleBreadcrumbClick(index);
                      }}>
                      {crumb}
                    </TextLink>
                  </span>
                ))}
                {current && (
                  <span key="crumb-current" style={{ color: "#999" }}>
                    {current}
                  </span>
                )}
              </div>
            );
          })()}
      </>
    );
  }

  private renderMatchedLabel(label: string, query: string): JSX.Element {
    const q = (query || "").toLowerCase();
    const text = label || "";
    const lower = text.toLowerCase();
    if (!q) return <div className="wrap">{text}</div>;

    // Exact substring highlight first
    const exactIdx = lower.indexOf(q);
    if (exactIdx >= 0) {
      const before = text.slice(0, exactIdx);
      const match = text.slice(exactIdx, exactIdx + q.length);
      const after = text.slice(exactIdx + q.length);
      return (
        <div className="wrap">
          {before}
          <mark>{match}</mark>
          {after}
        </div>
      );
    }

    // Fuzzy highlight: mark characters that match in order
    const out: Array<JSX.Element | string> = [];
    let qi = 0;
    for (let i = 0; i < text.length; i++) {
      const ch = text[i];
      if (qi < q.length && ch.toLowerCase() === q[qi]) {
        out.push(<mark key={`m-${i}`}>{ch}</mark>);
        qi++;
      } else {
        out.push(ch);
      }
    }
    return <div className="wrap">{out}</div>;
  }

  private renderGraphArea() {
    if (!this.state.selectedLabel || !this.state.indexedData) {
      return <div className="tbd-graph-area" />;
    }

    return (
      <div className="tbd-graph-area">
        {/* Debug Controls (visible only when enabled via URL param) */}
        {this.state.debugEnabled && (
          <div style={{ padding: "8px", borderBottom: "1px solid #eee", backgroundColor: "#f8f9fa" }}>
            <button onClick={this.toggleDebugMode} style={{ marginBottom: "8px" }}>
              {this.state.debugMode ? "Hide" : "Show"} Debug Controls
            </button>
            {this.state.debugMode && this.renderDebugControls()}
          </div>
        )}

        {this.state.chartLoading && (
          <div className="chart-loading-overlay">
            <div>Rendering graph...</div>
          </div>
        )}
        <div
          ref={this.chartRef}
          style={{ width: "100%", height: "820px", opacity: this.state.chartLoading ? 0.5 : 1 }}
        />
      </div>
    );
  }

  private toggleInterestingOnly = (checked: boolean) => {
    this.setState({ interestingOnly: checked }, () => {
      // Update URL param for persistence
      this.updateURL(this.state.selectedLabel || null);
      // Rerender the chart with new filtering
      if (this.g6Graph) this.updateG6GraphData();
      else this.renderChart();
    });
  };

  private renderDebugControls() {
    const { spacing } = this.state;
    type KeyDef = { key: keyof State["spacing"]; label: string; description: string; unit?: string; max?: number };
    const groups: Array<{ title: string; items: KeyDef[] }> = [
      {
        title: "Layout Bands",
        items: [
          { key: "xGap", label: "X Gap", description: "Horizontal gap between spawn nodes" },
          { key: "minRowHeight", label: "Min Row Height", description: "Minimum height of a row" },
          { key: "rowGap", label: "Row Gap", description: "Vertical space between rows" },
          { key: "spawnBandGap", label: "Spawn Band Gap", description: "Vertical space between spawn bands" },
          { key: "innerMargin", label: "Inner Margin", description: "Extra padding inside rows/bands" },
          {
            key: "rowExtraPadding",
            label: "Row Extra Padding",
            description: "Additional pixels added to each row's height",
          },
        ],
      },
      {
        title: "Node Shapes",
        items: [],
      },
      {
        title: "Node Sizes",
        items: [
          { key: "spawnSymbolSize", label: "Spawn Circle Size", description: "Diameter when spawn shape is circle" },
          {
            key: "symlinkSymbolSize",
            label: "Symlink Circle Size",
            description: "Diameter when symlink shape is circle",
          },
          {
            key: "interestingArtifactSize",
            label: "Interesting Artifact Size",
            description: "Size of orange (interesting) artifacts",
          },
          {
            key: "uninterestingArtifactSize",
            label: "Uninteresting Artifact Size",
            description: "Size of grey (uninteresting) artifacts",
          },
        ],
      },
      {
        title: "Artifacts",
        items: [
          {
            key: "artifactOffset",
            label: "Artifact Offset",
            description: "Default spacing between ungrouped artifacts",
          },
          {
            key: "maxArtifactsPerSpawn",
            label: "Max Artifacts Per Spawn",
            description: "Maximum number of artifacts shown per spawn (performance limit)",
            max: 200,
          },
        ],
      },
      {
        title: "Input Rectangle",
        items: [
          {
            key: "inputRectMaxHeight",
            label: "Input Block Max Height",
            description: "Max height for inputs rectangle (wraps into columns)",
          },
          {
            key: "inputRectRowSpacing",
            label: "Row Spacing",
            description: "Vertical spacing between rows inside the input rectangle",
          },
          {
            key: "inputRectColSpacing",
            label: "Column Spacing",
            description: "Horizontal spacing between columns inside the input rectangle",
          },
        ],
      },
    ];

    const renderItem = ({ key, label, description, unit = "px", max }: KeyDef) => (
      <div key={key as string} style={{ display: "flex", flexDirection: "column", gap: "4px" }}>
        <label style={{ fontSize: "12px", fontWeight: "bold" }}>
          {label}: {spacing[key]}
          {key !== "maxArtifactsPerSpawn" ? unit : ""}
        </label>
        <div style={{ fontSize: "11px", color: "#666", marginBottom: "4px" }}>{description}</div>
        <div style={{ display: "flex", gap: "8px", alignItems: "center" }}>
          <input
            type="range"
            min="1"
            max={(max || (key === "maxArtifactsPerSpawn" ? 200 : 5000)).toString()}
            value={spacing[key] as number}
            onChange={(e) => this.updateSpacing(key, parseInt(e.target.value))}
            style={{ flex: 1 }}
          />
          <input
            type="number"
            min="1"
            max={(max || (key === "maxArtifactsPerSpawn" ? 200 : 5000)).toString()}
            value={spacing[key] as number}
            onChange={(e) => this.updateSpacing(key, parseInt(e.target.value) || 1)}
            style={{ width: "70px", padding: "2px 4px", fontSize: "12px" }}
          />
        </div>
      </div>
    );

    return (
      <div style={{ display: "flex", flexDirection: "column", gap: 16, marginTop: 8 }}>
        {groups.map((group) => (
          <div key={group.title} style={{ border: "1px solid #eee", borderRadius: 6, padding: 10 }}>
            <div style={{ fontSize: 12, fontWeight: 700, marginBottom: 8 }}>{group.title}</div>
            {group.title === "Node Shapes" ? (
              <div style={{ display: "grid", gridTemplateColumns: "repeat(auto-fit, minmax(300px, 1fr))", gap: 12 }}>
                <div style={{ display: "flex", flexDirection: "column", gap: 4 }}>
                  <label style={{ fontSize: 12, fontWeight: 700 }}>Spawn Shape</label>
                  <select
                    value={this.state.spacing.spawnShape as any}
                    onChange={(e) => this.updateSpacing("spawnShape", e.target.value)}
                    style={{ padding: "4px 6px", fontSize: 12 }}>
                    <option value="rect">Rectangle</option>
                    <option value="roundRect">Rounded Rectangle</option>
                    <option value="circle">Circle</option>
                  </select>
                </div>
                <div style={{ display: "flex", flexDirection: "column", gap: 4 }}>
                  <label style={{ fontSize: 12, fontWeight: 700 }}>
                    Spawn Width: {this.state.spacing.spawnWidth}px
                  </label>
                  <input
                    type="range"
                    min="20"
                    max="600"
                    value={this.state.spacing.spawnWidth}
                    onChange={(e) => this.updateSpacing("spawnWidth", parseInt(e.target.value))}
                  />
                </div>
                <div style={{ display: "flex", flexDirection: "column", gap: 4 }}>
                  <label style={{ fontSize: 12, fontWeight: 700 }}>
                    Spawn Height: {this.state.spacing.spawnHeight}px
                  </label>
                  <input
                    type="range"
                    min="20"
                    max="600"
                    value={this.state.spacing.spawnHeight}
                    onChange={(e) => this.updateSpacing("spawnHeight", parseInt(e.target.value))}
                  />
                </div>
                <div style={{ display: "flex", flexDirection: "column", gap: 4 }}>
                  <label style={{ fontSize: 12, fontWeight: 700 }}>Symlink Shape</label>
                  <select
                    value={this.state.spacing.symlinkShape as any}
                    onChange={(e) => this.updateSpacing("symlinkShape", e.target.value)}
                    style={{ padding: "4px 6px", fontSize: 12 }}>
                    <option value="rect">Rectangle</option>
                    <option value="roundRect">Rounded Rectangle</option>
                    <option value="circle">Circle</option>
                  </select>
                </div>
                <div style={{ display: "flex", flexDirection: "column", gap: 4 }}>
                  <label style={{ fontSize: 12, fontWeight: 700 }}>
                    Symlink Width: {this.state.spacing.symlinkWidth}px
                  </label>
                  <input
                    type="range"
                    min="20"
                    max="600"
                    value={this.state.spacing.symlinkWidth}
                    onChange={(e) => this.updateSpacing("symlinkWidth", parseInt(e.target.value))}
                  />
                </div>
                <div style={{ display: "flex", flexDirection: "column", gap: 4 }}>
                  <label style={{ fontSize: 12, fontWeight: 700 }}>
                    Symlink Height: {this.state.spacing.symlinkHeight}px
                  </label>
                  <input
                    type="range"
                    min="20"
                    max="600"
                    value={this.state.spacing.symlinkHeight}
                    onChange={(e) => this.updateSpacing("symlinkHeight", parseInt(e.target.value))}
                  />
                </div>
              </div>
            ) : (
              <div style={{ display: "grid", gridTemplateColumns: "repeat(auto-fit, minmax(300px, 1fr))", gap: 12 }}>
                {group.items.map((it) => renderItem(it))}
              </div>
            )}
          </div>
        ))}
      </div>
    );
  }

  private renderDetailsCard() {
    if (!this.state.indexedData) return null;
    if (!this.state.selectedItem) return null;

    const isArtifact = this.state.selectedItem.type === "artifact";
    let headerIcon: JSX.Element = <Info className="icon purple" />;
    let headerTitle: string = "Details";

    if (isArtifact) {
      const artifactId = this.state.selectedItem.id as number;
      let artifactType: ArtifactType | undefined;
      let artifactPath = "";
      if (this.state.indexedData.files.has(artifactId)) {
        artifactType = "file";
        artifactPath = this.state.indexedData.files.get(artifactId)?.path || "";
      } else if (this.state.indexedData.directories.has(artifactId)) {
        artifactType = "directory";
        artifactPath = this.state.indexedData.directories.get(artifactId)?.path || "";
      } else if (this.state.indexedData.symlinks.has(artifactId)) {
        artifactType = "symlink";
        artifactPath = this.state.indexedData.symlinks.get(artifactId)?.path || "";
      } else if (this.state.indexedData.runfiles.has(artifactId)) {
        artifactType = "runfiles";
        artifactPath = this.state.indexedData.runfiles.get(artifactId)?.path || "";
      }
      headerIcon = this.getArtifactTypeIcon(artifactType);
      headerTitle = artifactPath || "Artifact";
    }

    const content =
      this.state.selectedItem.type === "spawn"
        ? this.renderSpawnDetails(this.state.selectedItem.id as number)
        : this.state.selectedItem.type === "symlink"
          ? this.renderSymlinkDetails(this.state.selectedItem.id as number)
          : this.renderArtifactDetails(this.state.selectedItem.id as number);

    return (
      <div className="card">
        {headerIcon}
        <div className="content">
          <div className="title">{headerTitle}</div>
          {content}
        </div>
      </div>
    );
  }

  private renderSpawnDetails(spawnId: number) {
    const spawn = this.state.indexedData!.spawns.get(spawnId);
    if (!spawn) {
      return <div className="details">Spawn not found</div>;
    }

    return (
      <div className="details">
        <div className="invocation-section">
          <div className="invocation-section-title">Target label</div>
          <div>
            {!spawn.targetLabel || spawn.targetLabel === this.state.selectedLabel ? (
              <span style={{ color: "#999" }}>{spawn.targetLabel || "Unknown"}</span>
            ) : (
              <TextLink
                href={this.getTbdUrlForLabel(spawn.targetLabel)}
                onClick={(e) => {
                  e.preventDefault();
                  this.navigateToSpawn(spawn.targetLabel);
                }}>
                {spawn.targetLabel}
              </TextLink>
            )}
          </div>
        </div>
        <div className="invocation-section">
          <div className="invocation-section-title">Mnemonic</div>
          <div>
            {(() => {
              const href = this.getActionUrlForSpawn(spawn);
              const label = spawn.mnemonic || "Unknown";
              return href ? <TextLink href={href}>{label}</TextLink> : <>{label}</>;
            })()}
          </div>
        </div>
        {(() => {
          // Some logs encode missing IDs as 0; avoid rendering a stray "0" by not short-circuiting on the value.
          const hasToolSet = typeof spawn.toolSetId === "number" && spawn.toolSetId !== 0;
          if (!hasToolSet) return null;
          const toolChain = this.extractToolChain(spawn, this.state.indexedData!);
          if (toolChain.length === 0) return null;
          return (
            <div className="invocation-section">
              <div className="invocation-section-title">Toolchain</div>
              <div>{toolChain.join(" · ")}</div>
            </div>
          );
        })()}
        {/* Action section removed; mnemonic now links to action when available */}
        {spawn.args &&
          (spawn.args as any[]).length > 0 &&
          (() => {
            const args: string[] = (spawn.args as any[]).map(String);
            if (args.length === 0) return null;

            // Build one-arg-per-line, grouping flag + value pairs.
            const lines: string[] = [];
            // First line is the command itself
            lines.push(args[0]);
            for (let i = 1; i < args.length; i++) {
              const a = args[i];
              const next = i + 1 < args.length ? args[i + 1] : undefined;
              if (a.startsWith("-") && next !== undefined && !next.startsWith("-")) {
                lines.push(`${a} ${next}`);
                i++; // skip next
              } else {
                lines.push(a);
              }
            }

            const MAX_LINES = 60;
            const expanded = this.state.detailsExpand.command;
            let head: string[] = lines;
            let tail: string[] = [];
            if (!expanded && lines.length > MAX_LINES) {
              const headCount = Math.ceil(MAX_LINES / 2);
              const tailCount = Math.floor(MAX_LINES / 2);
              head = lines.slice(0, headCount);
              tail = lines.slice(lines.length - tailCount);
            }

            return (
              <div className="invocation-section">
                <div className="invocation-section-title">Command</div>
                <div className="wrap" style={{ fontFamily: "monospace", whiteSpace: "pre", overflowX: "auto" }}>
                  {head.map((l, idx) => {
                    const isFirst = idx === 0;
                    const indent = isFirst ? "" : "  ";
                    // Add a trailing backslash if there are more lines (either more head items or any tail items)
                    const continueLine = tail.length > 0 || idx < head.length - 1;
                    return <div key={`cmd-h-${idx}`}>{`${indent}${l}${continueLine ? " \\" : ""}`}</div>;
                  })}
                  {!expanded && lines.length > MAX_LINES && (
                    <div>
                      <TextLink
                        href="#"
                        onClick={(e) => {
                          e.preventDefault();
                          this.setState((s) => ({ detailsExpand: { ...s.detailsExpand, command: true } }));
                        }}>
                        ...
                      </TextLink>
                    </div>
                  )}
                  {tail.map((l, idx) => {
                    const isLast = idx === tail.length - 1; // last of full command
                    const indent = "  ";
                    return <div key={`cmd-t-${idx}`}>{`${indent}${l}${!isLast ? " \\" : ""}`}</div>;
                  })}
                </div>
              </div>
            );
          })()}
        {(() => {
          // Group execution-related fields
          const rows: Array<[string, string | number]> = [];
          if (spawn.runner) rows.push(["Runner", spawn.runner]);
          if (spawn.status) rows.push(["Status", spawn.status]);
          rows.push(["Exit code", spawn.exitCode || 0]);
          if (rows.length === 0) return null;
          return (
            <div className="invocation-section">
              <div className="invocation-section-title">Execution</div>
              <div>
                {rows.map(([k, v], i) => (
                  <div key={`exec-${i}`}>
                    {k}: {String(v)}
                  </div>
                ))}
              </div>
            </div>
          );
        })()}
        {(() => {
          // Group cache/remoting fields
          const rc: any = (spawn as any).remoteCacheable;
          const rows: Array<[string, string]> = [];
          rows.push(["Cache hit", spawn.cacheHit ? "Yes" : "No"]);
          rows.push(["Cacheable", spawn.cacheable ? "Yes" : "No"]);
          if (typeof rc === "boolean") rows.push(["Remote cacheable", rc ? "Yes" : "No"]);
          rows.push(["Remotable", spawn.remotable ? "Yes" : "No"]);
          return (
            <div className="invocation-section">
              <div className="invocation-section-title">Cache & Remoting</div>
              <div>
                {rows.map(([k, v], i) => (
                  <div key={`cache-${i}`}>
                    {k}: {v}
                  </div>
                ))}
              </div>
            </div>
          );
        })()}
        {(() => {
          // Group timing fields
          const tm: any = (spawn as any).timeoutMillis;
          const toStringSafe = (v: any) => {
            try {
              if (!v) return "0";
              if (typeof v === "number") return String(v);
              if (typeof v === "string") return v;
              if (typeof v?.toString === "function") return v.toString();
            } catch {}
            return String(v || 0);
          };
          const parts: Array<[string, string]> = [];
          if (spawn.metrics?.startTime) parts.push(["Start time", this.formatTimestamp(spawn.metrics.startTime)]);
          if (spawn.metrics?.totalTime) parts.push(["Total time", this.formatDuration(spawn.metrics.totalTime)]);
          if (spawn.metrics?.executionWallTime)
            parts.push(["Execution time", this.formatDuration(spawn.metrics.executionWallTime)]);
          if (tm && toStringSafe(tm) !== "0") parts.push(["Timeout", `${toStringSafe(tm)} ms`]);
          if (parts.length === 0) return null;
          return (
            <div className="invocation-section">
              <div className="invocation-section-title">Timing</div>
              <div>
                {parts.map(([k, v], i) => (
                  <div key={`tim-${i}`}>
                    {k}: {v}
                  </div>
                ))}
              </div>
            </div>
          );
        })()}
        {(() => {
          // Platform properties
          const props = (spawn.platform?.properties || []).slice();
          if (!props.length) return null;
          props.sort((a, b) => (a.name || "").localeCompare(b.name || ""));
          const limit = 12;
          const expanded = !!this.state.detailsExpand.platform;
          const list = expanded ? props : props.slice(0, limit);
          return (
            <div className="invocation-section">
              <div className="invocation-section-title">Platform</div>
              <div>
                {list.map((p, idx) => (
                  <div key={`plat-${idx}`} className="wrap">
                    <span style={{ fontFamily: "monospace" }}>
                      {(p.name || "").toString()}={(p.value || "").toString()}
                    </span>
                  </div>
                ))}
                {!expanded && props.length > list.length && (
                  <div>
                    <TextLink
                      href="#"
                      onClick={(e) => {
                        e.preventDefault();
                        this.setState((s) => ({ detailsExpand: { ...s.detailsExpand, platform: true } }));
                      }}>
                      ...
                    </TextLink>
                  </div>
                )}
              </div>
            </div>
          );
        })()}
        {(() => {
          // Environment variables
          const env = (spawn.envVars || []).slice();
          if (!env.length) return null;
          env.sort((a, b) => (a.name || "").localeCompare(b.name || ""));
          const limit = 20;
          const expanded = !!this.state.detailsExpand.env;
          const list = expanded ? env : env.slice(0, limit);
          return (
            <div className="invocation-section">
              <div className="invocation-section-title">Environment</div>
              <div>
                {list.map((e, idx) => (
                  <div key={`env-${idx}`} className="wrap">
                    <span style={{ fontFamily: "monospace" }}>
                      {(e.name || "").toString()}={(e.value || "").toString()}
                    </span>
                  </div>
                ))}
                {!expanded && env.length > list.length && (
                  <div>
                    <TextLink
                      href="#"
                      onClick={(e) => {
                        e.preventDefault();
                        this.setState((s) => ({ detailsExpand: { ...s.detailsExpand, env: true } }));
                      }}>
                      ...
                    </TextLink>
                  </div>
                )}
              </div>
            </div>
          );
        })()}
        {(() => {
          const data = this.state.indexedData!;
          const labelArtifact = (
            id: number
          ): { id: number; label: string; digest?: build.bazel.remote.execution.v2.Digest } | null => {
            if (data.files.has(id)) {
              const f = data.files.get(id)!;
              const path = f.path || "unknown";
              const basename = path.split("/").pop() || path;
              return { id, label: basename, digest: f.digest as any };
            }
            if (data.directories.has(id)) return { id, label: this.getDirectoryLabel(data.directories.get(id)!) };
            if (data.symlinks.has(id)) return { id, label: this.getSymlinkLabel(data.symlinks.get(id)!) };
            if (data.runfiles.has(id)) return { id, label: this.getRunfilesLabel(data.runfiles.get(id)!) };
            return null;
          };
          // Inputs sample list
          let inputItems: { id: number; label: string; digest?: build.bazel.remote.execution.v2.Digest }[] = [];
          let inputTotal = 0;
          const inputLimit = 12;
          try {
            if (spawn.inputSetId) {
              const ids = Array.from(this.expandInputSet(spawn.inputSetId, data));
              inputTotal = ids.length;
              inputItems = ids.map(labelArtifact).filter(Boolean) as {
                id: number;
                label: string;
                digest?: build.bazel.remote.execution.v2.Digest;
              }[];
              inputItems.sort((a, b) => a.label.localeCompare(b.label));
              if (!this.state.detailsExpand.inputs && inputItems.length > inputLimit) {
                inputItems = inputItems.slice(0, inputLimit);
              }
            }
          } catch {}
          // Tools sample list
          let toolItems: { id: number; label: string; digest?: build.bazel.remote.execution.v2.Digest }[] = [];
          let toolTotal = 0;
          const toolLimit = 12;
          try {
            if (spawn.toolSetId) {
              const ids = Array.from(this.expandInputSet(spawn.toolSetId, data));
              toolTotal = ids.length;
              toolItems = ids.map(labelArtifact).filter(Boolean) as {
                id: number;
                label: string;
                digest?: build.bazel.remote.execution.v2.Digest;
              }[];
              toolItems.sort((a, b) => a.label.localeCompare(b.label));
              if (!this.state.detailsExpand.tools && toolItems.length > toolLimit) {
                toolItems = toolItems.slice(0, toolLimit);
              }
            }
          } catch {}
          // Outputs list (usually small)
          const outputIds: number[] = [];
          for (const o of spawn.outputs || []) {
            let outputId =
              (o as any).outputId || (o as any).fileId || (o as any).directoryId || (o as any).unresolvedSymlinkId;
            if (outputId) outputIds.push(outputId);
          }
          let outputItems = outputIds.map(labelArtifact).filter(Boolean) as {
            id: number;
            label: string;
            digest?: build.bazel.remote.execution.v2.Digest;
          }[];
          outputItems.sort((a, b) => a.label.localeCompare(b.label));
          const outputTotal = outputItems.length;
          const outputLimit = 12;
          if (!this.state.detailsExpand.outputs && outputItems.length > outputLimit) {
            outputItems = outputItems.slice(0, outputLimit);
          }

          return (
            <>
              <div className="invocation-section">
                <div className="invocation-section-title">I/O Summary</div>
                <div>
                  Inputs: {inputTotal}
                  {toolTotal ? ` · Tools: ${toolTotal}` : ""} · Outputs: {outputIds.length}
                </div>
              </div>
              {inputItems.length > 0 && (
                <div className="invocation-section">
                  <div className="invocation-section-title">Inputs</div>
                  <div>
                    {inputItems.map((it) => (
                      <div key={`in-${it.id}`}>
                        <TextLink
                          href="#"
                          onClick={(e) => {
                            e.preventDefault();
                            this.setState({ selectedItem: { type: "artifact", id: it.id } });
                          }}>
                          <span className="file-name">
                            {it.label}
                            {it.digest && <DigestComponent digest={it.digest} />}
                          </span>
                        </TextLink>
                      </div>
                    ))}
                    {!this.state.detailsExpand.inputs && inputTotal > inputItems.length && (
                      <div>
                        <TextLink
                          href="#"
                          onClick={(e) => {
                            e.preventDefault();
                            this.setState((s) => ({ detailsExpand: { ...s.detailsExpand, inputs: true } }));
                          }}>
                          ...
                        </TextLink>
                      </div>
                    )}
                  </div>
                </div>
              )}
              {toolItems.length > 0 && (
                <div className="invocation-section">
                  <div className="invocation-section-title">Tools</div>
                  <div>
                    {toolItems.map((it) => (
                      <div key={`tool-${it.id}`}>
                        <TextLink
                          href="#"
                          onClick={(e) => {
                            e.preventDefault();
                            this.setState({ selectedItem: { type: "artifact", id: it.id } });
                          }}>
                          <span className="file-name">
                            {it.label}
                            {it.digest && <DigestComponent digest={it.digest} />}
                          </span>
                        </TextLink>
                      </div>
                    ))}
                    {!this.state.detailsExpand.tools && toolTotal > toolItems.length && (
                      <div>
                        <TextLink
                          href="#"
                          onClick={(e) => {
                            e.preventDefault();
                            this.setState((s) => ({ detailsExpand: { ...s.detailsExpand, tools: true } }));
                          }}>
                          ...
                        </TextLink>
                      </div>
                    )}
                  </div>
                </div>
              )}
              {outputItems.length > 0 && (
                <div className="invocation-section">
                  <div className="invocation-section-title">Outputs</div>
                  <div>
                    {outputItems.map((it) => (
                      <div key={`out-${it.id}`}>
                        <TextLink
                          href="#"
                          onClick={(e) => {
                            e.preventDefault();
                            this.setState({ selectedItem: { type: "artifact", id: it.id } });
                          }}>
                          <span className="file-name">
                            {it.label}
                            {it.digest && <DigestComponent digest={it.digest} />}
                          </span>
                        </TextLink>
                      </div>
                    ))}
                    {!this.state.detailsExpand.outputs && outputTotal > outputItems.length && (
                      <div>
                        <TextLink
                          href="#"
                          onClick={(e) => {
                            e.preventDefault();
                            this.setState((s) => ({ detailsExpand: { ...s.detailsExpand, outputs: true } }));
                          }}>
                          ...
                        </TextLink>
                      </div>
                    )}
                  </div>
                </div>
              )}
            </>
          );
        })()}
      </div>
    );
  }

  private renderSymlinkDetails(id: number) {
    const action = this.state.indexedData!.symlinkActions.get(id);
    if (!action) return <div className="details">Symlink action not found</div>;
    const data = this.state.indexedData!;
    const resolveArtifactId = (p?: string | null) => {
      if (!p) return undefined;
      return data.pathToFileId.get(p) || data.pathToDirId.get(p) || data.pathToSymlinkId.get(p);
    };
    const inputArtifactId = resolveArtifactId((action as any).inputPath);
    const outputArtifactId = resolveArtifactId((action as any).outputPath);
    return (
      <div className="details">
        <div className="invocation-section">
          <div className="invocation-section-title">Target label</div>
          <div>
            {!action.targetLabel || action.targetLabel === this.state.selectedLabel ? (
              <span style={{ color: "#999" }}>{action.targetLabel || "Unknown"}</span>
            ) : (
              <TextLink
                href={this.getTbdUrlForLabel(action.targetLabel)}
                onClick={(e) => {
                  e.preventDefault();
                  this.navigateToSpawn(action.targetLabel);
                }}>
                {action.targetLabel}
              </TextLink>
            )}
          </div>
        </div>
        <div className="invocation-section">
          <div className="invocation-section-title">Mnemonic</div>
          <div>{action.mnemonic || "Symlink"}</div>
        </div>
        {(() => {
          // If output resolves to a file with a digest, show a Digest download link
          if (outputArtifactId !== undefined && this.state.indexedData?.files.has(outputArtifactId)) {
            const f = this.state.indexedData.files.get(outputArtifactId)!;
            if (f.digest && f.digest.hash && f.digest.hash !== "") {
              return (
                <div className="invocation-section">
                  <div className="invocation-section-title">Digest</div>
                  <div>
                    <Link href={this.getFileBytestreamHref(f)} target="_blank">
                      <DigestComponent digest={f.digest} expanded />
                    </Link>
                  </div>
                </div>
              );
            }
          }
          return null;
        })()}
        <div className="invocation-section">
          <div className="invocation-section-title">Input path</div>
          {inputArtifactId !== undefined ? (
            <TextLink
              className="wrap"
              href="#"
              onClick={(e) => {
                e.preventDefault();
                this.setState({ selectedItem: { type: "artifact", id: inputArtifactId } });
              }}>
              {(action as any).inputPath || ""}
            </TextLink>
          ) : (
            <div className="wrap">{(action as any).inputPath || ""}</div>
          )}
        </div>
        <div className="invocation-section">
          <div className="invocation-section-title">Output path</div>
          {outputArtifactId !== undefined ? (
            <TextLink
              className="wrap"
              href="#"
              onClick={(e) => {
                e.preventDefault();
                this.setState({ selectedItem: { type: "artifact", id: outputArtifactId } });
              }}>
              {(action as any).outputPath || ""}
            </TextLink>
          ) : (
            <div className="wrap">{(action as any).outputPath || ""}</div>
          )}
        </div>
      </div>
    );
  }

  private getActionUrlForSpawn(spawn: tools.protos.ExecLogEntry.Spawn): string | null {
    if (!spawn.digest?.hash || !this.props.model) return null;
    const digest = new build.bazel.remote.execution.v2.Digest({
      hash: spawn.digest.hash,
      sizeBytes: spawn.digest.sizeBytes as any,
    });
    const digestStr = digestToString(digest);
    return `/invocation/${this.props.model.getInvocationId()}?actionDigest=${digestStr}#action`;
  }

  private renderArtifactDetails(artifactId: number) {
    const { indexedData } = this.state;
    if (!indexedData) return <p>No data</p>;

    let artifactInfo: any = null;
    let artifactType: ArtifactType = "file";

    if (indexedData.files.has(artifactId)) {
      artifactInfo = indexedData.files.get(artifactId);
      artifactType = "file";
    } else if (indexedData.directories.has(artifactId)) {
      artifactInfo = indexedData.directories.get(artifactId);
      artifactType = "directory";
    } else if (indexedData.symlinks.has(artifactId)) {
      artifactInfo = indexedData.symlinks.get(artifactId);
      artifactType = "symlink";
    } else if (indexedData.runfiles.has(artifactId)) {
      artifactInfo = indexedData.runfiles.get(artifactId);
      artifactType = "runfiles";
    }

    if (!artifactInfo) {
      return <p>Artifact not found</p>;
    }

    const producerSpawnId = indexedData.outputProducers.get(artifactId);
    const producerSpawn = producerSpawnId ? indexedData.spawns.get(producerSpawnId) : null;

    return (
      <div className="details">
        {artifactType === "file" && artifactInfo.digest && (
          <div className="invocation-section">
            <div className="invocation-section-title">Digest</div>
            <div>
              <Link href={this.getFileBytestreamHref(artifactInfo)} target="_blank">
                <DigestComponent digest={artifactInfo.digest} expanded={true} />
              </Link>
            </div>
          </div>
        )}

        {artifactType === "directory" && (
          <div className="invocation-section">
            <div className="invocation-section-title">Contents ({artifactInfo.files?.length || 0})</div>
            <div>
              {(artifactInfo.files || []).map((f: tools.protos.ExecLogEntry.File, idx: number) => (
                <div key={idx} className="artifact-line">
                  <span className="artifact-name">{f.path || "file"}</span>
                  {f.digest && (
                    <span style={{ marginLeft: 8 }}>
                      <Link href={this.getFileBytestreamHref(f)} target="_blank">
                        <DigestComponent digest={f.digest} />
                      </Link>
                    </span>
                  )}
                </div>
              ))}
            </div>
          </div>
        )}

        {artifactType === "symlink" && (
          <div className="invocation-section">
            <div className="invocation-section-title">Target</div>
            <div className="wrap">{artifactInfo.targetPath || "N/A"}</div>
          </div>
        )}

        {artifactType === "runfiles" && (
          <>
            {artifactInfo.repoMappingManifest?.digest?.hash &&
              artifactInfo.repoMappingManifest.digest.hash !== "" &&
              artifactInfo.repoMappingManifest.digest.sizeBytes !== 0 && (
                <div className="invocation-section">
                  <div className="invocation-section-title">Repo mapping</div>
                  <div>
                    {/* If sizeBytes is present we can link; otherwise just show digest */}
                    {artifactInfo.repoMappingManifest.digest.sizeBytes !== undefined &&
                    artifactInfo.repoMappingManifest.digest.sizeBytes !== null ? (
                      <Link
                        href={this.getFileBytestreamHref({
                          path: `${artifactInfo.path || ""}/_repo_mapping`,
                          digest: artifactInfo.repoMappingManifest.digest,
                        } as tools.protos.ExecLogEntry.File)}
                        target="_blank">
                        <DigestComponent digest={artifactInfo.repoMappingManifest.digest} />
                      </Link>
                    ) : (
                      <DigestComponent digest={artifactInfo.repoMappingManifest.digest} />
                    )}
                  </div>
                </div>
              )}
            {(() => {
              const d = this.state.indexedData!;
              const rt = artifactInfo as tools.protos.ExecLogEntry.RunfilesTree;
              // Build a single Inputs section (union of canonical artifacts + symlink targets)
              const inputMap = new Map<
                number,
                { id: number; label: string; digest?: build.bazel.remote.execution.v2.Digest }
              >();
              try {
                if ((rt as any).inputSetId) {
                  const ids = Array.from(this.expandInputSet((rt as any).inputSetId, d));
                  for (const id of ids) {
                    if (d.files.has(id)) {
                      const f = d.files.get(id)!;
                      inputMap.set(id, { id, label: f.path || "unknown", digest: f.digest || undefined });
                    } else if (d.directories.has(id)) {
                      const dir = d.directories.get(id)!;
                      inputMap.set(id, { id, label: dir.path || "unknown" });
                    } else if (d.symlinks.has(id)) {
                      const s = d.symlinks.get(id)!;
                      inputMap.set(id, { id, label: s.path || "unknown" });
                    } else if (d.runfiles.has(id)) {
                      const rf2 = d.runfiles.get(id)!;
                      inputMap.set(id, { id, label: rf2.path || "unknown" });
                    }
                  }
                }
              } catch (e) {
                // fall through; Inputs section will still render anything we did collect
              }

              const symlinks = this.expandSymlinkEntrySet((rt as any).symlinksId || (rt as any).symlinks_id, d);
              const rootSymlinks = this.expandSymlinkEntrySet(
                (rt as any).rootSymlinksId || (rt as any).root_symlinks_id,
                d
              );
              const addEntryId = (entryId?: number) => {
                if (!entryId) return;
                if (inputMap.has(entryId)) return;
                if (d.files.has(entryId))
                  inputMap.set(entryId, {
                    id: entryId,
                    label: d.files.get(entryId)!.path || "unknown",
                    digest: d.files.get(entryId)!.digest || undefined,
                  });
                else if (d.directories.has(entryId))
                  inputMap.set(entryId, { id: entryId, label: d.directories.get(entryId)!.path || "unknown" });
                else if (d.symlinks.has(entryId))
                  inputMap.set(entryId, { id: entryId, label: d.symlinks.get(entryId)!.path || "unknown" });
                else if (d.runfiles.has(entryId))
                  inputMap.set(entryId, { id: entryId, label: d.runfiles.get(entryId)!.path || "unknown" });
              };
              symlinks.forEach((eid) => addEntryId(eid));
              rootSymlinks.forEach((eid) => addEntryId(eid));

              const inputRows = Array.from(inputMap.values()).sort((a, b) => a.label.localeCompare(b.label));

              const sections: JSX.Element[] = [];
              sections.push(
                <div className="invocation-section" key="rf-inputs">
                  <div className="invocation-section-title">Inputs</div>
                  <div>
                    {inputRows.length === 0 ? (
                      <span>None</span>
                    ) : (
                      inputRows.map((r) => (
                        <div key={r.id}>
                          <Link
                            href="#"
                            onClick={(e) => {
                              e.preventDefault();
                              this.setState({ selectedItem: { type: "artifact", id: r.id } });
                            }}>
                            {r.digest ? (
                              <span className="file-name">
                                {r.label}
                                <DigestComponent digest={r.digest} />
                              </span>
                            ) : (
                              r.label
                            )}
                          </Link>
                        </div>
                      ))
                    )}
                  </div>
                </div>
              );

              // Empty files
              const emptyFiles: string[] = (rt as any).emptyFiles || (rt as any).empty_files || [];
              if (emptyFiles.length > 0) {
                sections.push(
                  <div className="invocation-section" key="rf-empty">
                    <div className="invocation-section-title">Empty files</div>
                    <div>
                      {emptyFiles.sort().map((p) => (
                        <div key={p} className="wrap">
                          ∅ {p}
                        </div>
                      ))}
                    </div>
                  </div>
                );
              }

              return sections;
            })()}
          </>
        )}

        {(() => {
          // Compute exact symlink producer (prefer exact outputPath match, then normalized, then map)
          const data = this.state.indexedData!;
          const getArtifactPath = (id: number): string | undefined => {
            if (data.files.has(id)) return data.files.get(id)!.path || undefined;
            if (data.directories.has(id)) return data.directories.get(id)!.path || undefined;
            if (data.symlinks.has(id)) return data.symlinks.get(id)!.path || undefined;
            if (data.runfiles.has(id)) return data.runfiles.get(id)!.path || undefined;
            return undefined;
          };
          const norm = (p?: string) => (p ? p.replace(/-opt-exec-ST-[^/]+\//, "-opt-exec/") : p);
          let producerSymlinkActionId: number | null = null;
          const artifactPath = getArtifactPath(artifactId);
          if (artifactPath) {
            for (const [aid, act] of data.symlinkActions.entries()) {
              const outPath: string | undefined = (act as any).outputPath || (act as any).output_path;
              if (outPath && outPath === artifactPath) {
                producerSymlinkActionId = aid;
                break;
              }
            }
            if (producerSymlinkActionId === null) {
              const apn = norm(artifactPath);
              for (const [aid, act] of data.symlinkActions.entries()) {
                const outPath: string | undefined = (act as any).outputPath || (act as any).output_path;
                if (outPath && norm(outPath) === apn) {
                  producerSymlinkActionId = aid;
                  break;
                }
              }
            }
          }
          if (producerSymlinkActionId === null) {
            const mapped = data.symlinkOutputProducers.get(artifactId);
            if (mapped) producerSymlinkActionId = mapped;
          }
          const hasProducer = !!producerSpawn || producerSymlinkActionId !== null;
          if (!hasProducer) return null;
          return (
            <div className="invocation-section">
              <div className="invocation-section-title">Producer</div>
              <div>{this.renderProducerTree(artifactId)}</div>
            </div>
          );
        })()}

        <div className="invocation-section">
          <div className="invocation-section-title">Consumers</div>
          <div>{this.renderConsumerList(artifactId)}</div>
        </div>
      </div>
    );
  }

  private renderConsumerList(artifactId: number) {
    const { indexedData } = this.state;
    if (!indexedData) return null;
    const consumers = this.findArtifactConsumers(artifactId, indexedData);
    const symlinkConsumers = this.findSymlinkActionConsumers(artifactId, indexedData);
    if (consumers.length === 0 && symlinkConsumers.length === 0) {
      return <span>None</span>;
    }

    // Group by target label and then disambiguate by mnemonic, filename, platform (same as spawn labels)
    type ConsumerMeta = {
      kind: "spawn" | "symlink";
      id: number; // spawnId or actionId
      label: string; // target label
      mnemonic: string;
      filename: string;
      platform: string;
    };

    const extractPlatform = (p?: string | null) => {
      if (!p) return "";
      const m = p.match(/^bazel-out\/([^/]+)\//);
      return m ? m[1] : "";
    };
    const extractFilename = (p?: string | null) => {
      if (!p) return "";
      const parts = p.split("/");
      return parts[parts.length - 1] || "";
    };
    const getPrimaryOutputPath = (spawn: tools.protos.ExecLogEntry.Spawn, data: IndexedData) => {
      let primaryOutputPath: string | undefined;
      let sawFile = false;
      for (const o of spawn.outputs || []) {
        let outId =
          (o as any).outputId || (o as any).fileId || (o as any).directoryId || (o as any).unresolvedSymlinkId;
        if (outId) {
          if (data.files.has(outId)) {
            if (!primaryOutputPath) primaryOutputPath = data.files.get(outId)!.path || "";
            sawFile = true;
          } else if (!sawFile && data.directories.has(outId)) {
            if (!primaryOutputPath) primaryOutputPath = data.directories.get(outId)!.path || "";
          }
        }
      }
      return primaryOutputPath;
    };

    const byTarget = new Map<string, ConsumerMeta[]>();
    for (const c of consumers) {
      const target = (c.spawn.targetLabel || "").trim();
      const primaryPath = getPrimaryOutputPath(c.spawn, indexedData);
      const meta: ConsumerMeta = {
        kind: "spawn",
        id: c.spawnId,
        label: target,
        mnemonic: (c.spawn.mnemonic || "").trim(),
        filename: extractFilename(primaryPath),
        platform: extractPlatform(primaryPath),
      };
      if (!byTarget.has(target)) byTarget.set(target, []);
      byTarget.get(target)!.push(meta);
    }
    for (const s of symlinkConsumers) {
      const target = (s.action.targetLabel || "").trim();
      const outPath = (s.action as any).outputPath as string | undefined;
      const meta: ConsumerMeta = {
        kind: "symlink",
        id: s.actionId,
        label: target,
        mnemonic: (s.action.mnemonic || "Symlink").trim(),
        filename: extractFilename(outPath),
        platform: extractPlatform(outPath),
      };
      if (!byTarget.has(target)) byTarget.set(target, []);
      byTarget.get(target)!.push(meta);
    }

    // For each target, disambiguate entries like spawn labels: if same mnemonic repeats, first add filename; if still dup, use platform.
    const renderTargetGroup = (target: string, metas: ConsumerMeta[]) => {
      // Sort by mnemonic then filename then platform for stable output
      metas.sort(
        (a, b) =>
          a.mnemonic.localeCompare(b.mnemonic) ||
          a.filename.localeCompare(b.filename) ||
          a.platform.localeCompare(b.platform)
      );

      const byMnemonic = new Map<string, ConsumerMeta[]>();
      for (const m of metas) {
        if (!byMnemonic.has(m.mnemonic)) byMnemonic.set(m.mnemonic, []);
        byMnemonic.get(m.mnemonic)!.push(m);
      }

      const childLines: Array<{ key: string; text: string; kind: "spawn" | "symlink"; id: number }> = [];
      for (const [mnemonic, list] of byMnemonic.entries()) {
        if (list.length === 1) {
          childLines.push({
            key: `${mnemonic}:${list[0].id}`,
            text: `${mnemonic}`,
            kind: list[0].kind,
            id: list[0].id,
          });
          continue;
        }
        // Multiple with same mnemonic — group by filename
        const byFilename = new Map<string, ConsumerMeta[]>();
        for (const m of list) {
          const f = m.filename || "";
          if (!byFilename.has(f)) byFilename.set(f, []);
          byFilename.get(f)!.push(m);
        }
        for (const [file, fileList] of byFilename.entries()) {
          if (file && fileList.length === 1) {
            const m = fileList[0];
            childLines.push({ key: `${mnemonic}:${m.id}`, text: `${mnemonic} (${file})`, kind: m.kind, id: m.id });
          } else if (file && fileList.length > 1) {
            // Duplicate even with same filename — switch to platform
            for (const m of fileList) {
              const plat = m.platform || "";
              childLines.push({
                key: `${mnemonic}:${plat}:${m.id}`,
                text: `${mnemonic}${plat ? ` (${plat})` : ""}`,
                kind: m.kind,
                id: m.id,
              });
            }
          } else if (!file) {
            // No filename — use platform if available
            for (const m of fileList) {
              const plat = m.platform || "";
              childLines.push({
                key: `${mnemonic}:${plat}:${m.id}`,
                text: `${mnemonic}${plat ? ` (${plat})` : ""}`,
                kind: m.kind,
                id: m.id,
              });
            }
          }
        }
      }

      const targetLabelEl =
        target === this.state.selectedLabel ? (
          <span style={{ color: "#999" }}>{target}</span>
        ) : (
          <TextLink
            href={this.getTbdUrlForLabel(target)}
            onClick={(e) => {
              e.preventDefault();
              this.navigateToSpawn(target);
            }}>
            {target}
          </TextLink>
        );

      return (
        <div key={`cons-group-${target}`}>
          <div>{targetLabelEl}</div>
          {childLines.map((child) => (
            <div key={`cons-child-${target}-${child.key}`} style={{ marginLeft: 16 }}>
              <span style={{ color: "#999" }}>→ </span>
              {child.kind === "spawn" ? (
                <TextLink
                  href={this.getTbdUrlForSpawn(target, child.id)}
                  onClick={(e) => {
                    e.preventDefault();
                    this.navigateToSpawnWithSelection(target, child.id);
                  }}>
                  {child.text}
                </TextLink>
              ) : (
                <TextLink
                  href={this.getTbdUrlForSymlink(target, child.id)}
                  onClick={(e) => {
                    e.preventDefault();
                    this.navigateToSymlinkWithSelection(target, child.id);
                  }}>
                  {child.text}
                </TextLink>
              )}
            </div>
          ))}
        </div>
      );
    };

    // Stable order: by target label using compareLabels
    const groups = Array.from(byTarget.entries()).sort((a, b) => this.compareLabels(a[0] || "", b[0] || ""));

    return <div>{groups.map(([label, metas]) => renderTargetGroup(label, metas))}</div>;
  }

  private renderProducerTree(artifactId: number) {
    const { indexedData } = this.state;
    if (!indexedData) return null;

    type ProducerMeta = {
      kind: "spawn" | "symlink";
      id: number;
      label: string;
      mnemonic: string;
      filename: string;
      platform: string;
    };

    const extractPlatform = (p?: string | null) => {
      if (!p) return "";
      const m = p.match(/^bazel-out\/([^/]+)\//);
      return m ? m[1] : "";
    };
    const extractFilename = (p?: string | null) => {
      if (!p) return "";
      const parts = p.split("/");
      return parts[parts.length - 1] || "";
    };

    const producers: ProducerMeta[] = [];

    // Spawn producer
    const spawnId = indexedData.outputProducers.get(artifactId);
    if (spawnId !== undefined) {
      const spawn = indexedData.spawns.get(spawnId);
      if (spawn) {
        let primaryOutputPath: string | undefined;
        let sawFile = false;
        for (const o of spawn.outputs || []) {
          let outId =
            (o as any).outputId || (o as any).fileId || (o as any).directoryId || (o as any).unresolvedSymlinkId;
          if (outId) {
            if (indexedData.files.has(outId)) {
              if (!primaryOutputPath) primaryOutputPath = indexedData.files.get(outId)!.path || "";
              sawFile = true;
            } else if (!sawFile && indexedData.directories.has(outId)) {
              if (!primaryOutputPath) primaryOutputPath = indexedData.directories.get(outId)!.path || "";
            }
          }
        }
        producers.push({
          kind: "spawn",
          id: spawnId,
          label: (spawn.targetLabel || "").trim(),
          mnemonic: (spawn.mnemonic || "").trim(),
          filename: extractFilename(primaryOutputPath),
          platform: extractPlatform(primaryOutputPath),
        });
      }
    }

    // Symlink producer(s)
    const getArtifactPath = (id: number): string | undefined => {
      if (indexedData.files.has(id)) return indexedData.files.get(id)!.path || undefined;
      if (indexedData.directories.has(id)) return indexedData.directories.get(id)!.path || undefined;
      if (indexedData.symlinks.has(id)) return indexedData.symlinks.get(id)!.path || undefined;
      if (indexedData.runfiles.has(id)) return indexedData.runfiles.get(id)!.path || undefined;
      return undefined;
    };
    const norm = (p?: string) => (p ? p.replace(/-opt-exec-ST-[^/]+\//, "-opt-exec/") : p);
    const artifactPath = getArtifactPath(artifactId);
    const addActionMeta = (aid: number, act: tools.protos.ExecLogEntry.SymlinkAction) => {
      const outPath: string | undefined = (act as any).outputPath || (act as any).output_path;
      producers.push({
        kind: "symlink",
        id: aid,
        label: (act.targetLabel || "").trim(),
        mnemonic: (act.mnemonic || "Symlink").trim(),
        filename: extractFilename(outPath),
        platform: extractPlatform(outPath),
      });
    };
    if (artifactPath) {
      for (const [aid, act] of indexedData.symlinkActions.entries()) {
        const outPath: string | undefined = (act as any).outputPath || (act as any).output_path;
        if (outPath && outPath === artifactPath) addActionMeta(aid, act);
      }
      if (!producers.some((p) => p.kind === "symlink")) {
        const apn = norm(artifactPath);
        for (const [aid, act] of indexedData.symlinkActions.entries()) {
          const outPath: string | undefined = (act as any).outputPath || (act as any).output_path;
          if (outPath && norm(outPath) === apn) addActionMeta(aid, act);
        }
      }
    }
    // Fallback to precomputed mapping only if we could not determine any symlink producer yet
    if (!producers.some((p) => p.kind === "symlink")) {
      const mappedAid = indexedData.symlinkOutputProducers.get(artifactId);
      if (mappedAid !== undefined) {
        const act = indexedData.symlinkActions.get(mappedAid);
        if (act) addActionMeta(mappedAid, act);
      }
    }

    if (producers.length === 0) return <span>None</span>;

    const byTarget = new Map<string, ProducerMeta[]>();
    for (const m of producers) {
      const t = m.label;
      if (!byTarget.has(t)) byTarget.set(t, []);
      byTarget.get(t)!.push(m);
    }

    const renderTargetGroup = (target: string, metas: ProducerMeta[]) => {
      metas.sort(
        (a, b) =>
          a.mnemonic.localeCompare(b.mnemonic) ||
          a.filename.localeCompare(b.filename) ||
          a.platform.localeCompare(b.platform)
      );
      const byMnemonic = new Map<string, ProducerMeta[]>();
      for (const m of metas) {
        if (!byMnemonic.has(m.mnemonic)) byMnemonic.set(m.mnemonic, []);
        byMnemonic.get(m.mnemonic)!.push(m);
      }
      const lines: Array<{ key: string; text: string; kind: "spawn" | "symlink"; id: number }> = [];
      for (const [mnemonic, list] of byMnemonic.entries()) {
        if (list.length === 1) {
          const m = list[0];
          lines.push({ key: `${mnemonic}:${m.id}`, text: `${mnemonic}`, kind: m.kind, id: m.id });
          continue;
        }
        const byFilename = new Map<string, ProducerMeta[]>();
        for (const m of list) {
          const f = m.filename || "";
          if (!byFilename.has(f)) byFilename.set(f, []);
          byFilename.get(f)!.push(m);
        }
        for (const [file, fileList] of byFilename.entries()) {
          if (file && fileList.length === 1) {
            const m = fileList[0];
            lines.push({ key: `${mnemonic}:${m.id}`, text: `${mnemonic} (${file})`, kind: m.kind, id: m.id });
          } else if (file && fileList.length > 1) {
            for (const m of fileList) {
              const plat = m.platform || "";
              lines.push({
                key: `${mnemonic}:${plat}:${m.id}`,
                text: `${mnemonic}${plat ? ` (${plat})` : ""}`,
                kind: m.kind,
                id: m.id,
              });
            }
          } else if (!file) {
            for (const m of fileList) {
              const plat = m.platform || "";
              lines.push({
                key: `${mnemonic}:${plat}:${m.id}`,
                text: `${mnemonic}${plat ? ` (${plat})` : ""}`,
                kind: m.kind,
                id: m.id,
              });
            }
          }
        }
      }

      const targetLabelEl =
        target === this.state.selectedLabel ? (
          <span style={{ color: "#999" }}>{target}</span>
        ) : (
          <TextLink
            href={this.getTbdUrlForLabel(target)}
            onClick={(e) => {
              e.preventDefault();
              this.navigateToSpawn(target);
            }}>
            {target}
          </TextLink>
        );

      return (
        <div key={`prod-group-${target}`}>
          <div>{targetLabelEl}</div>
          {lines.map((line) => (
            <div key={`prod-child-${target}-${line.key}`} style={{ marginLeft: 16 }}>
              <span style={{ color: "#999" }}>→ </span>
              {line.kind === "spawn" ? (
                <TextLink
                  href={this.getTbdUrlForSpawn(target, line.id)}
                  onClick={(e) => {
                    e.preventDefault();
                    this.navigateToSpawnWithSelection(target, line.id);
                  }}>
                  {line.text}
                </TextLink>
              ) : (
                <TextLink
                  href={this.getTbdUrlForSymlink(target, line.id)}
                  onClick={(e) => {
                    e.preventDefault();
                    this.navigateToSymlinkWithSelection(target, line.id);
                  }}>
                  {line.text}
                </TextLink>
              )}
            </div>
          ))}
        </div>
      );
    };

    const groups = Array.from(byTarget.entries()).sort((a, b) => this.compareLabels(a[0] || "", b[0] || ""));
    return <div>{groups.map(([label, metas]) => renderTargetGroup(label, metas))}</div>;
  }

  private findArtifactConsumers(
    artifactId: number,
    data: IndexedData
  ): Array<{ spawnId: number; spawn: tools.protos.ExecLogEntry.Spawn }> {
    const out: Array<{ spawnId: number; spawn: tools.protos.ExecLogEntry.Spawn }> = [];
    for (const [spawnId, spawn] of data.spawns.entries()) {
      let isConsumer = false;
      try {
        if (spawn.inputSetId) {
          const inputs = this.expandInputSet(spawn.inputSetId, data);
          if (inputs.has(artifactId)) isConsumer = true;
        }
        if (!isConsumer && spawn.toolSetId) {
          const toolsSet = this.expandInputSet(spawn.toolSetId, data);
          if (toolsSet.has(artifactId)) isConsumer = true;
        }
      } catch {}
      if (isConsumer) {
        out.push({ spawnId, spawn });
      }
    }
    // Stable sort: //-prefixed labels first, then @-prefixed, then others; tie-break by mnemonic then id
    out.sort((a, b) => {
      const labelCmp = this.compareLabels(a.spawn.targetLabel || "", b.spawn.targetLabel || "");
      if (labelCmp !== 0) return labelCmp;
      const m = (a.spawn.mnemonic || "").localeCompare(b.spawn.mnemonic || "");
      if (m !== 0) return m;
      return a.spawnId - b.spawnId;
    });
    return out;
  }

  private findSymlinkActionConsumers(
    artifactId: number,
    data: IndexedData
  ): Array<{ actionId: number; action: tools.protos.ExecLogEntry.SymlinkAction }> {
    const out: Array<{ actionId: number; action: tools.protos.ExecLogEntry.SymlinkAction }> = [];
    // Resolve an artifact id from a path
    const resolveArtifactId = (p?: string | null) => {
      if (!p) return undefined as number | undefined;
      return data.pathToFileId.get(p) || data.pathToDirId.get(p) || data.pathToSymlinkId.get(p);
    };
    for (const [actionId, action] of data.symlinkActions.entries()) {
      const inId = resolveArtifactId((action as any).inputPath);
      if (inId === artifactId) {
        out.push({ actionId, action });
      }
    }
    // Sort by label then mnemonic then id for stability
    out.sort((a, b) => {
      const l = this.compareLabels(a.action.targetLabel || "", b.action.targetLabel || "");
      if (l !== 0) return l;
      const m = (a.action.mnemonic || "").localeCompare(b.action.mnemonic || "");
      if (m !== 0) return m;
      return a.actionId - b.actionId;
    });
    return out;
  }

  private getFileBytestreamHref(file: tools.protos.ExecLogEntry.File): string {
    if (!file?.digest?.hash) return "#";
    const digest = new build.bazel.remote.execution.v2.Digest({
      hash: file.digest.hash,
      sizeBytes: file.digest.sizeBytes as any,
    });
    const bytestream = this.props.model.getBytestreamURL(digest);
    const filename = (file.path || "").split("/").pop() || "file";
    return rpcService.getBytestreamUrl(bytestream, this.props.model.getInvocationId(), { filename });
  }

  private getTbdUrlForLabel(label: string): string {
    const invId = this.props.model.getInvocationId();
    const search = new URLSearchParams(window.location.search);
    search.set("tbdLabel", label);
    return `/invocation/${invId}?${search.toString()}`;
  }

  private getTbdUrlForSpawn(label: string, spawnId: number): string {
    const invId = this.props.model.getInvocationId();
    const search = new URLSearchParams(window.location.search);
    search.set("tbdLabel", label);
    search.set("tbdSel", `spawn:${spawnId}`);
    return `/invocation/${invId}?${search.toString()}`;
  }

  private getTbdUrlForSymlink(label: string, actionId: number): string {
    const invId = this.props.model.getInvocationId();
    const search = new URLSearchParams(window.location.search);
    search.set("tbdLabel", label);
    search.set("tbdSel", `symlink:${actionId}`);
    return `/invocation/${invId}?${search.toString()}`;
  }

  private formatDuration(duration: any): string {
    if (!duration) return "N/A";
    const seconds = parseInt(duration.seconds || "0");
    const nanos = parseInt(duration.nanos || "0");
    const totalMs = seconds * 1000 + nanos / 1000000;

    if (totalMs < 1000) {
      return `${totalMs.toFixed(1)}ms`;
    } else if (totalMs < 60000) {
      return `${(totalMs / 1000).toFixed(2)}s`;
    } else {
      const minutes = Math.floor(totalMs / 60000);
      const remainingSeconds = ((totalMs % 60000) / 1000).toFixed(1);
      return `${minutes}m ${remainingSeconds}s`;
    }
  }

  private formatTimestamp(ts: any): string {
    try {
      if (!ts) return "N/A";
      if (typeof ts === "string") {
        const d = new Date(ts);
        if (!isNaN(d.getTime())) return d.toLocaleString();
        return ts;
      }
      const seconds = parseInt(ts.seconds || "0");
      const nanos = parseInt(ts.nanos || "0");
      const ms = seconds * 1000 + Math.floor(nanos / 1e6);
      const d = new Date(ms);
      if (!isNaN(d.getTime())) return d.toLocaleString();
      return String(ts);
    } catch {
      return String(ts);
    }
  }

  private navigateToSpawn(targetLabel: string | null | undefined) {
    if (!targetLabel) return;

    const newBreadcrumbs = [...this.state.breadcrumbs];
    if (this.state.selectedLabel && this.state.selectedLabel !== targetLabel) {
      newBreadcrumbs.push(this.state.selectedLabel);
    }

    this.setState({
      selectedLabel: targetLabel,
      breadcrumbs: newBreadcrumbs,
      selectedItem: null,
      detailsExpand: { inputs: false, tools: false, outputs: false, command: false, env: false, platform: false },
    });
    this.updateURL(targetLabel);
  }

  private navigateToSpawnWithSelection(targetLabel: string | null | undefined, spawnId: number) {
    if (!targetLabel) return;
    const newBreadcrumbs = [...this.state.breadcrumbs];
    if (this.state.selectedLabel) {
      newBreadcrumbs.push(this.state.selectedLabel);
    }
    this.setState({
      selectedLabel: targetLabel,
      breadcrumbs: newBreadcrumbs,
      selectedItem: { type: "spawn", id: spawnId },
      detailsExpand: { inputs: false, tools: false, outputs: false, command: false, env: false, platform: false },
    });
    // Update URL label immediately; tbdSel will be added by pushSelectionToURL in componentDidUpdate
    this.updateURL(targetLabel);
  }

  private navigateToSymlinkWithSelection(targetLabel: string | null | undefined, actionId: number) {
    if (!targetLabel) return;
    const newBreadcrumbs = [...this.state.breadcrumbs];
    if (this.state.selectedLabel) {
      newBreadcrumbs.push(this.state.selectedLabel);
    }
    this.setState({
      selectedLabel: targetLabel,
      breadcrumbs: newBreadcrumbs,
      selectedItem: { type: "symlink", id: actionId },
      detailsExpand: { inputs: false, tools: false, outputs: false, command: false, env: false, platform: false },
    });
    this.updateURL(targetLabel);
  }

  private fetchTargetResults = async (search: string): Promise<string[]> => {
    const labels = this.state.indexedData ? Array.from(this.state.indexedData.labelSet) : [];
    const q = search.trim().toLowerCase();
    if (!q) return labels.sort((a, b) => this.compareLabels(a || "", b || ""));

    // Rank: 0) exact eq, 1) substring (earlier index first), 2) fuzzy
    type Cand = { label: string; lower: string; group: number; idx: number };
    const cands: Cand[] = [];
    for (const label of labels) {
      const lower = (label || "").toLowerCase();
      if (lower === q) {
        cands.push({ label, lower, group: 0, idx: 0 });
        continue;
      }
      const idx = lower.indexOf(q);
      if (idx >= 0) {
        cands.push({ label, lower, group: 1, idx });
        continue;
      }
      if (this.fuzzyMatch(lower, q)) {
        cands.push({ label, lower, group: 2, idx: Number.MAX_SAFE_INTEGER });
      }
    }
    cands.sort((a, b) => {
      if (a.group !== b.group) return a.group - b.group;
      if (a.group === 1 && a.idx !== b.idx) return a.idx - b.idx; // earlier substring position
      return this.compareLabels(a.label || "", b.label || "");
    });
    return cands.map((c) => c.label);
  };

  private handleTargetPicked = (label: string) => {
    const selectedLabel = label || null;
    this.setState(
      {
        selectedLabel,
        breadcrumbs: selectedLabel ? [] : this.state.breadcrumbs,
        selectedItem: null,
        detailsExpand: { inputs: false, tools: false, outputs: false, command: false, env: false, platform: false },
      },
      () => this.renderChart()
    );
    this.updateURL(selectedLabel);
  };

  private fuzzyMatch = (text: string, query: string): boolean => {
    // Simple in-order fuzzy match: all query characters appear in order within text
    let ti = 0;
    for (let qi = 0; qi < query.length; qi++) {
      const qc = query[qi];
      ti = text.indexOf(qc, ti);
      if (ti === -1) return false;
      ti++;
    }
    return true;
  };

  // No orientation toggle in UI; keep default orientation.

  private handleBreadcrumbClick = (index: number) => {
    const selectedLabel = this.state.breadcrumbs[index];
    const newBreadcrumbs = this.state.breadcrumbs.slice(0, index);
    this.setState({
      selectedLabel,
      breadcrumbs: newBreadcrumbs,
      detailsExpand: { inputs: false, tools: false, outputs: false, command: false, env: false, platform: false },
    });
    this.updateURL(selectedLabel);
  };

  private async fetchExecutionLog() {
    if (!this.props.model.buildToolLogs) {
      this.setState({ error: "No build tool logs available" });
      return;
    }

    const executionLogFile = this.props.model.buildToolLogs.log.find(
      (l: any) =>
        (l.name === "execution.log" || l.name === "execution_log.binpb.zst") && l.uri.startsWith("bytestream://")
    );

    if (!executionLogFile) {
      this.setState({ error: "No execution log found" });
      return;
    }

    this.setState({ loading: true, error: null });

    try {
      // Set the stored encoding header to prevent the server from double-compressing.
      const init: RequestInit = {
        headers: { "X-Stored-Encoding-Hint": "zstd" },
      };

      const body = await rpcService.fetchBytestreamFile(
        executionLogFile.uri,
        this.props.model.getInvocationId(),
        "arraybuffer",
        { init }
      );

      if (!body) {
        throw new Error("Response body is null");
      }

      const entries = this.decodeExecutionLog(body);
      const indexedData = this.buildIndexes(entries);
      this.setState({
        log: entries,
        indexedData,
        loading: false,
      });
    } catch (error) {
      console.error("Failed to fetch execution log:", error);
      this.setState({
        loading: false,
        error: `Failed to load execution log: ${error}`,
      });
    }
  }

  private decodeExecutionLog(body: ArrayBuffer): tools.protos.ExecLogEntry[] {
    const entries: tools.protos.ExecLogEntry[] = [];
    const byteArray = new Uint8Array(body);

    let successCount = 0;
    let skipCount = 0;

    for (let offset = 0; offset < body.byteLength; ) {
      try {
        const length = varint.decode(byteArray, offset);
        const bytes = varint.decode.bytes || 0;
        offset += bytes;

        if (offset + length > body.byteLength) {
          console.warn(`Truncated entry at offset ${offset}, stopping decode`);
          break;
        }

        const entry = tools.protos.ExecLogEntry.decode(byteArray.subarray(offset, offset + length));
        entries.push(entry);
        successCount++;
        offset += length;
      } catch (error) {
        skipCount++;
        console.warn(`Failed to decode entry at offset ${offset}, skipping:`, error);

        // Skip ahead by one byte to try to find valid data
        offset++;

        // If we've skipped too many entries in a row, stop
        if (skipCount > 100 && successCount === 0) {
          console.error("Too many decode failures with no success, stopping");
          break;
        }
      }
    }

    console.log(`Decoded ${successCount} entries, skipped ${skipCount} invalid entries`);
    return entries;
  }

  private buildIndexes(entries: tools.protos.ExecLogEntry[]): IndexedData {
    const data: IndexedData = {
      files: new Map(),
      directories: new Map(),
      symlinks: new Map(),
      runfiles: new Map(),
      inputSets: new Map(),
      spawns: new Map(),
      symlinkActions: new Map(),
      symlinkEntrySets: new Map(),
      labelSet: new Set(),
      outputProducers: new Map(),
      expandedInputSets: new Map(),
      consumedArtifacts: new Set(),
      pathToFileId: new Map(),
      pathToDirId: new Map(),
      pathToSymlinkId: new Map(),
      symlinkProducedArtifacts: new Set(),
      symlinkOutputProducers: new Map(),
    };

    let nextSpawnId = 1;
    let nextSymlinkActionId = 1;

    for (const entry of entries) {
      const id = entry.id || 0;

      if (entry.file) {
        data.files.set(id, entry.file);
        if (entry.file.path) data.pathToFileId.set(entry.file.path, id);
      } else if (entry.directory) {
        data.directories.set(id, entry.directory);
        if (entry.directory.path) data.pathToDirId.set(entry.directory.path, id);
      } else if (entry.unresolvedSymlink) {
        data.symlinks.set(id, entry.unresolvedSymlink);
        if (entry.unresolvedSymlink.path) data.pathToSymlinkId.set(entry.unresolvedSymlink.path, id);
      } else if (entry.runfilesTree) {
        data.runfiles.set(id, entry.runfilesTree);
      } else if (entry.inputSet) {
        data.inputSets.set(id, entry.inputSet);
      } else if (entry.spawn) {
        // Assign a stable spawn ID if missing
        const spawnId = id || nextSpawnId++;
        data.spawns.set(spawnId, entry.spawn);

        if (entry.spawn.targetLabel) {
          data.labelSet.add(entry.spawn.targetLabel);
        }

        // Build output producers map
        for (const output of entry.spawn.outputs || []) {
          if (output.outputId) {
            data.outputProducers.set(output.outputId, spawnId);
          }
          // Handle deprecated fields for compatibility
          if (output.fileId) {
            data.outputProducers.set(output.fileId, spawnId);
          }
          if (output.directoryId) {
            data.outputProducers.set(output.directoryId, spawnId);
          }
          if (output.unresolvedSymlinkId) {
            data.outputProducers.set(output.unresolvedSymlinkId, spawnId);
          }
        }
      } else if (entry.symlinkAction) {
        // Assign a stable symlink action ID if missing
        const actionId = id || nextSymlinkActionId++;
        data.symlinkActions.set(actionId, entry.symlinkAction);
        if (entry.symlinkAction.targetLabel) {
          data.labelSet.add(entry.symlinkAction.targetLabel);
        }
      } else if (entry.symlinkEntrySet) {
        data.symlinkEntrySets.set(id, entry.symlinkEntrySet);
      }
    }

    // After all entries are indexed, map symlink action outputs to artifact IDs
    const normalizeOutPath = (p?: string) => {
      if (!p) return p;
      // Normalize bazel-out exec path to drop optional "-ST-<hash>" segment
      // Example: bazel-out/<plat>-opt-exec-ST-<hash>/bin/... -> bazel-out/<plat>-opt-exec/bin/...
      return p.replace(/-opt-exec-ST-[^/]+\//, "-opt-exec/");
    };
    for (const [aid, action] of data.symlinkActions.entries()) {
      const outPathRaw: string | undefined = (action as any).outputPath || (action as any).output_path;
      const tryMap = (p?: string) => {
        if (!p) return;
        const outId = data.pathToSymlinkId.get(p) || data.pathToFileId.get(p) || data.pathToDirId.get(p);
        if (outId !== undefined) {
          data.symlinkProducedArtifacts.add(outId);
          if (!data.symlinkOutputProducers.has(outId)) {
            data.symlinkOutputProducers.set(outId, aid);
          }
        }
      };
      // Prefer raw path mapping; only map normalized if nothing mapped yet
      tryMap(outPathRaw);
      tryMap(normalizeOutPath(outPathRaw));
    }

    // Build a global set of consumed artifacts across all spawns (inputs + tools)
    try {
      for (const [_spawnId, spawn] of data.spawns.entries()) {
        if (spawn.inputSetId) {
          const inputSet = this.expandInputSet(spawn.inputSetId, data);
          for (const artifactId of inputSet) {
            data.consumedArtifacts.add(artifactId);
          }
        }
        if (spawn.toolSetId) {
          const toolSet = this.expandInputSet(spawn.toolSetId, data);
          for (const artifactId of toolSet) {
            data.consumedArtifacts.add(artifactId);
          }
        }
      }
      // Include symlink action inputs as consumers as well
      const resolveArtifactId = (p?: string | null) => {
        if (!p) return undefined as number | undefined;
        return data.pathToFileId.get(p) || data.pathToDirId.get(p) || data.pathToSymlinkId.get(p);
      };
      for (const [_id, action] of data.symlinkActions.entries()) {
        const inId = resolveArtifactId((action as any).inputPath);
        if (inId !== undefined) {
          data.consumedArtifacts.add(inId);
        }
      }
    } catch (e) {
      console.warn("Failed computing global consumed artifacts:", e);
    }

    return data;
  }

  private classifyArtifact(artifactId: number, data: IndexedData): ArtifactCategory {
    // Check if it's produced by any spawn (generated)
    const isProduced = data.outputProducers.has(artifactId);

    // Check if it's consumed by any spawn
    const isConsumed = data.consumedArtifacts.has(artifactId);

    // Check if it's a tool (binary executable)
    if (data.files.has(artifactId)) {
      const file = data.files.get(artifactId)!;
      const path = file.path || "";

      // Identify tools by path patterns and executable nature
      if (
        path.includes("/bin/") ||
        path.endsWith("/protoc") ||
        path.endsWith("/protoc-gen-protobufjs") ||
        path.includes("_/protoc-gen-") ||
        (path.includes("bazel-out") && path.includes("bin") && !path.includes(".proto"))
      ) {
        return "tool";
      }

      // Source files are typically in the workspace root and not produced
      if (!isProduced && (path.endsWith(".proto") || path.startsWith("proto/"))) {
        return "source";
      }
    }

    // Runfiles are typically tool-related
    if (data.runfiles.has(artifactId)) {
      return "tool";
    }

    // Intermediate artifacts are both produced and consumed
    if (isProduced && isConsumed) {
      return "intermediate";
    }

    // Generated artifacts are produced but not necessarily consumed
    if (isProduced) {
      return "generated";
    }

    // Default to source for unconsumed, unproduced artifacts
    return "source";
  }

  private extractToolChain(spawn: tools.protos.ExecLogEntry.Spawn, data: IndexedData): string[] {
    const tools: string[] = [];

    if (!spawn.toolSetId) return tools;

    try {
      const toolSet = this.expandInputSet(spawn.toolSetId, data);
      for (const artifactId of toolSet) {
        if (data.files.has(artifactId)) {
          const file = data.files.get(artifactId)!;
          const path = file.path || "";
          const basename = path.split("/").pop() || "";

          // Extract tool names from common patterns
          if (basename === "protoc" || basename.startsWith("protoc-gen-")) {
            tools.push(basename);
          } else if (path.includes("/bin/") && !path.includes(".runfiles")) {
            tools.push(basename);
          }
        } else if (data.runfiles.has(artifactId)) {
          const runfiles = data.runfiles.get(artifactId)!;
          const path = runfiles.path || "";
          if (path.includes("protoc")) {
            tools.push("protoc");
          }
          if (path.includes("protoc-gen-")) {
            tools.push("protoc-gen-protobufjs");
          }
        }
      }
    } catch (e) {
      console.warn("Failed to extract tool chain:", e);
    }

    return [...new Set(tools)]; // Remove duplicates
  }

  private prioritizeAndLimitArtifacts(
    artifactIds: number[],
    spawnId: number,
    data: IndexedData,
    maxCount: number
  ): { limited: number[]; hasOverflow: boolean; totalCount: number } {
    const isExpanded = this.state.expandedSpawns.has(spawnId);

    if (isExpanded) {
      // If expanded, show all artifacts
      return { limited: artifactIds, hasOverflow: false, totalCount: artifactIds.length };
    }

    if (artifactIds.length <= maxCount) {
      // If we have fewer artifacts than the limit, show all
      return { limited: artifactIds, hasOverflow: false, totalCount: artifactIds.length };
    }

    // Prioritize interesting artifacts
    const prioritized = [...artifactIds].sort((a, b) => {
      const aIsInteresting = this.isArtifactInteresting(a, data);
      const bIsInteresting = this.isArtifactInteresting(b, data);

      if (aIsInteresting && !bIsInteresting) return -1;
      if (!aIsInteresting && bIsInteresting) return 1;
      return 0; // Keep original order for same priority
    });

    return {
      limited: prioritized.slice(0, maxCount),
      hasOverflow: true,
      totalCount: artifactIds.length,
    };
  }

  private isArtifactInteresting(artifactId: number, data: IndexedData): boolean {
    const hasProducer = data.outputProducers.has(artifactId) || data.symlinkProducedArtifacts.has(artifactId);
    const hasConsumer = data.consumedArtifacts.has(artifactId);
    // Keep it simple: interesting iff both produced and consumed in view
    return hasProducer && hasConsumer;
  }

  private createOverflowNode(spawnId: number, hiddenCount: number): GraphNode {
    return {
      id: `overflow-${spawnId}`,
      type: "artifact",
      nodeId: -1, // Special ID for overflow nodes
      artifactType: "file",
      artifactCategory: "intermediate",
      label: `...+${hiddenCount}`,
    };
  }

  private expandInputSet(id: number, data: IndexedData): Set<number> {
    // Use cache if available
    if (data.expandedInputSets.has(id)) {
      return data.expandedInputSets.get(id)!;
    }

    const inputSet = data.inputSets.get(id);
    if (!inputSet) {
      return new Set();
    }

    const result = new Set<number>();

    // Add transitive sets first (postorder traversal)
    for (const transitiveId of inputSet.transitiveSetIds || []) {
      const transitiveSet = this.expandInputSet(transitiveId, data);
      for (const artifactId of transitiveSet) {
        result.add(artifactId);
      }
    }

    // Add direct entries
    for (const artifactId of inputSet.inputIds || []) {
      result.add(artifactId);
    }

    // Handle deprecated fields for backward compatibility
    for (const fileId of inputSet.fileIds || []) {
      result.add(fileId);
    }
    for (const directoryId of inputSet.directoryIds || []) {
      result.add(directoryId);
    }
    for (const symlinkId of inputSet.unresolvedSymlinkIds || []) {
      result.add(symlinkId);
    }

    // Cache the result
    data.expandedInputSets.set(id, result);
    return result;
  }

  private expandSymlinkEntrySet(id?: number, data?: IndexedData): Map<string, number> {
    const result = new Map<string, number>();
    if (!id || !data) return result;
    const visited = new Set<number>();
    const dfs = (sid: number) => {
      if (!sid || visited.has(sid)) return;
      visited.add(sid);
      const set = data.symlinkEntrySets.get(sid);
      if (!set) return;
      // Transitive first (postorder)
      for (const tid of set.transitiveSetIds || []) dfs(tid);
      // Then direct entries; later entries win on collisions
      const direct: any = (set as any).directEntries || (set as any).direct_entries;
      if (direct) {
        for (const [relPath, entryId] of Object.entries(direct as Record<string, number>)) {
          result.set(relPath, entryId as number);
        }
      }
    };
    dfs(id);
    return result;
  }

  private buildGraphData(targetLabel: string, data: IndexedData): GraphData {
    let targetSpawns = Array.from(data.spawns.entries()).filter(([_, spawn]) => spawn.targetLabel === targetLabel);
    const targetSymlinkActions = Array.from(data.symlinkActions.entries()).filter(
      ([_, a]) => a.targetLabel === targetLabel
    );

    if (targetSpawns.length === 0 && targetSymlinkActions.length === 0) {
      return { nodes: [], edges: [], spawnLayers: [], symlinkActions: [], symlinkLayers: new Map() };
    }

    // Apply performance caps
    const MAX_SPAWNS = 50; // Limit number of spawns per view
    const MAX_ARTIFACTS_PER_SPAWN = this.state.spacing.maxArtifactsPerSpawn; // Limit artifacts per spawn

    // Prioritize important spawns (ensure TestRunner and GoLink stay within limit)
    const spawnPriority = (s?: tools.protos.ExecLogEntry.Spawn): number => {
      const m = (s?.mnemonic || "").trim();
      if (m === "TestRunner") return 0;
      if (m === "GoLink") return 1;
      return 2;
    };
    targetSpawns = targetSpawns
      .map((e, idx) => ({ e, idx, prio: spawnPriority(e[1]) }))
      .sort((a, b) => a.prio - b.prio || a.idx - b.idx)
      .map((x) => x.e);

    let limitedSpawns = targetSpawns;
    const limitedSymlinkActions = Array.from(targetSymlinkActions).slice(
      0,
      Math.max(0, MAX_SPAWNS - limitedSpawns.length)
    );
    if (targetSpawns.length > MAX_SPAWNS) {
      console.warn(`Too many spawns (${targetSpawns.length}), limiting to ${MAX_SPAWNS}`);
      limitedSpawns = targetSpawns.slice(0, MAX_SPAWNS);
    }

    const nodes: GraphNode[] = [];
    const edges: GraphEdge[] = [];
    const extraNodes: GraphNode[] = []; // synthetic nodes for runfiles contents
    const artifactNodes = new Map<number, GraphNode>(); // artifact ID -> node
    const interestingOnly = !!this.state.interestingOnly;

    // Helper: extract platform and filename from a bazel-out path
    const extractPlatform = (p?: string | null) => {
      if (!p) return "";
      const m = p.match(/^bazel-out\/([^/]+)\//);
      return m ? m[1] : "";
    };
    const extractFilename = (p?: string | null) => {
      if (!p) return "";
      const parts = p.split("/");
      return parts[parts.length - 1] || "";
    };

    // Create spawn nodes
    const spawnNodes = new Map<number, GraphNode>();
    const spawnMeta = new Map<
      number,
      { mnemonic: string; primaryPath?: string; filename?: string; platform?: string }
    >();
    // Helper to compute a stable string id for a spawn based on first file output path, else dir, else label+mnemonic.
    const usedSpawnStrIds = new Set<string>();
    const makeUnique = (base: string) => {
      let key = base || "";
      if (!key) key = "unknown";
      let cand = key;
      let i = 1;
      while (usedSpawnStrIds.has(cand)) {
        cand = `${key}#${++i}`;
      }
      usedSpawnStrIds.add(cand);
      return cand;
    };
    const computeSpawnStrId = (spawn: tools.protos.ExecLogEntry.Spawn, meta: { primaryPath?: string }): string => {
      // Prefer first file output path (primaryPath chosen earlier favors file), else directory path
      const p = (meta.primaryPath || "").trim();
      if (p) return makeUnique(`spawn:${p}`);
      const fallbackLabel = (spawn.targetLabel || "").trim();
      const fallbackMnemonic = (spawn.mnemonic || "").trim();
      return makeUnique(`spawn:${fallbackLabel}#${fallbackMnemonic}`);
    };

    for (const [spawnId, spawn] of limitedSpawns) {
      // Determine output status and a primary output label for tooltips
      let hasValidOutput = false;
      let hasInvalidOutputPath = false;
      let primaryOutputLabel: string | undefined;
      let primaryOutputPath: string | undefined;
      let sawFile = false;
      for (const o of spawn.outputs || []) {
        let outId = o.outputId;
        if (!outId) {
          outId = o.fileId || o.directoryId || o.unresolvedSymlinkId;
        }
        if (outId) {
          hasValidOutput = true;
          if (data.files.has(outId)) {
            if (!primaryOutputPath) primaryOutputPath = data.files.get(outId)!.path || "";
            if (!primaryOutputLabel) primaryOutputLabel = this.getFileLabel(data.files.get(outId)!);
            sawFile = true;
          } else if (!sawFile && data.directories.has(outId)) {
            if (!primaryOutputPath) primaryOutputPath = data.directories.get(outId)!.path || "";
            if (!primaryOutputLabel) primaryOutputLabel = this.getDirectoryLabel(data.directories.get(outId)!);
          } else if (!primaryOutputLabel && data.symlinks.has(outId)) {
            primaryOutputLabel = this.getSymlinkLabel(data.symlinks.get(outId)!);
          } else if (!primaryOutputLabel && data.runfiles.has(outId)) {
            primaryOutputLabel = this.getRunfilesLabel(data.runfiles.get(outId)!);
          }
        }
        if (o.invalidOutputPath) {
          hasInvalidOutputPath = true;
        }
      }
      const toolChain = this.extractToolChain(spawn, data);
      const spawnStrId = computeSpawnStrId(spawn, { primaryPath: primaryOutputPath });
      const spawnNode: GraphNode = {
        id: spawnStrId,
        type: "spawn",
        nodeId: spawnId,
        label: spawn.mnemonic || "Unknown Action",
        spawnHasValidOutputs: hasValidOutput,
        spawnHasInvalidOutputPaths: hasInvalidOutputPath,
        primaryOutputLabel,
        platform: extractPlatform(primaryOutputPath),
        isToolSpawn: toolChain.length > 0,
        toolChain,
      };
      nodes.push(spawnNode);
      spawnNodes.set(spawnId, spawnNode);
      spawnMeta.set(spawnId, {
        mnemonic: spawn.mnemonic || "",
        primaryPath: primaryOutputPath,
        filename: extractFilename(primaryOutputPath),
        platform: extractPlatform(primaryOutputPath),
      });
    }

    // Disambiguate spawns with same mnemonic by appending filename, then platform if needed
    const byMnemonic = new Map<string, number[]>();
    for (const [id, meta] of spawnMeta.entries()) {
      const m = meta.mnemonic || "";
      if (!byMnemonic.has(m)) byMnemonic.set(m, []);
      byMnemonic.get(m)!.push(id);
    }
    for (const [mnemonic, ids] of byMnemonic.entries()) {
      if (ids.length <= 1) continue;
      const byFilename = new Map<string, number[]>();
      for (const id of ids) {
        const meta = spawnMeta.get(id)!;
        const file = meta.filename || "";
        const node = spawnNodes.get(id)!;
        if (file) node.label = `${mnemonic}\n${file}`;
        if (!byFilename.has(file)) byFilename.set(file, []);
        byFilename.get(file)!.push(id);
      }
      for (const [_file, dupIds] of byFilename.entries()) {
        if (dupIds.length <= 1) continue;
        for (const id of dupIds) {
          const meta = spawnMeta.get(id)!;
          const node = spawnNodes.get(id)!;
          const platform = meta.platform || "";
          // For duplicates, prefer platform as the main identifier (omit filename on the label)
          node.label = `${mnemonic}\n${platform}`;
        }
      }
    }

    // Create symlink action nodes and connect to artifacts by path when possible
    const symlinkNodes = new Map<number, GraphNode>();
    const symlinkMeta = new Map<number, { mnemonic: string; filename?: string; platform?: string; outPath?: string }>();
    // Deduplicate unresolved input paths across symlink actions so we render a single input node per path
    const ghostInputNodeByPath = new Map<string, string>();
    for (const [sid, action] of limitedSymlinkActions) {
      const outPath: string | undefined = (action as any).outputPath || (action as any).output_path;
      const platform = extractPlatform(outPath);
      const filename = extractFilename(outPath);
      const symlinkKeyBase =
        (outPath && outPath.trim()) || `${(action.targetLabel || "").trim()}#${(action.mnemonic || "Symlink").trim()}`;
      const symlinkStrId = makeUnique(`symlink:${symlinkKeyBase}`);
      const symlinkNode: GraphNode = {
        id: symlinkStrId,
        type: "spawn",
        nodeId: sid,
        label: action.mnemonic || "SymlinkAction",
        spawnHasValidOutputs: true,
        isSymlinkAction: true, // Mark this as a symlink action for different coloring
        primaryOutputLabel: outPath,
        platform,
      };
      nodes.push(symlinkNode);
      symlinkNodes.set(sid, symlinkNode);
      symlinkMeta.set(sid, {
        mnemonic: action.mnemonic || "SymlinkAction",
        filename,
        platform,
        outPath,
      });

      if ((action as any).inputPath) {
        const inputPath = (action as any).inputPath as string;
        const fileId = data.pathToFileId.get(inputPath) || data.pathToDirId.get(inputPath);
        const symlinkTargetId = data.pathToSymlinkId.get(inputPath);
        const inId = fileId || symlinkTargetId;
        // Always render symlink action input (node + edge), regardless of interestingOnly
        if (inId) {
          const artifactNode = this.getOrCreateArtifactNode(inId, data, artifactNodes);
          if (artifactNode) {
            edges.push({ from: artifactNode.id, to: symlinkNode.id, type: "input" });
          }
        } else {
          // Create or reuse a non-selectable placeholder node for unresolved input path (shared across actions)
          let ghostId = ghostInputNodeByPath.get(inputPath);
          if (!ghostId) {
            ghostId = `missing:${inputPath}`;
            // Extract version from bazel external path like "external/io_bazel_bazel-6.2.1-darwin-arm64/file/downloaded"
            const versionMatch = inputPath.match(/bazel-([^-/]+)/);
            const label = versionMatch ? `bazel-${versionMatch[1]}` : inputPath.split("/")!.pop() || inputPath;
            const ghostNode: GraphNode = {
              id: ghostId,
              type: "artifact",
              nodeId: -1,
              artifactType: "file",
              artifactCategory: "source",
              label: label,
              nonSelectable: true,
            };
            nodes.push(ghostNode);
            ghostInputNodeByPath.set(inputPath, ghostId);
          }
          edges.push({ from: ghostId, to: symlinkNode.id, type: "input" });
        }
      }
      if ((action as any).outputPath) {
        const outputPath = (action as any).outputPath as string;
        const outId =
          data.pathToSymlinkId.get(outputPath) || data.pathToFileId.get(outputPath) || data.pathToDirId.get(outputPath);
        if (outId && (!interestingOnly || this.isArtifactInteresting(outId, data))) {
          const artifactNode = this.getOrCreateArtifactNode(outId, data, artifactNodes);
          if (artifactNode) {
            edges.push({ from: symlinkNode.id, to: artifactNode.id, type: "output" });
          }
        } else if (!outId && !interestingOnly) {
          // Create a non-selectable placeholder node for unresolved output path
          const ghostId = `missing-out:${outputPath}#${sid}`;
          const ghostNode: GraphNode = {
            id: ghostId,
            type: "artifact",
            nodeId: -1,
            artifactType: "file",
            artifactCategory: "generated",
            label: outputPath,
            nonSelectable: true,
          };
          nodes.push(ghostNode);
          edges.push({ from: symlinkNode.id, to: ghostId, type: "output" });
        }
      }
    }

    // Disambiguate symlink actions with same mnemonic similarly to spawns
    const symlinkByMnemonic = new Map<string, number[]>();
    for (const [id, meta] of symlinkMeta.entries()) {
      const m = meta.mnemonic || "";
      if (!symlinkByMnemonic.has(m)) symlinkByMnemonic.set(m, []);
      symlinkByMnemonic.get(m)!.push(id);
    }
    for (const [mnemonic, ids] of symlinkByMnemonic.entries()) {
      if (ids.length <= 1) continue;
      const byFilename = new Map<string, number[]>();
      for (const id of ids) {
        const meta = symlinkMeta.get(id)!;
        const file = meta.filename || "";
        const node = symlinkNodes.get(id)!;
        if (file) node.label = `${mnemonic}\n${file}`;
        if (!byFilename.has(file)) byFilename.set(file, []);
        byFilename.get(file)!.push(id);
      }
      for (const [_file, dupIds] of byFilename.entries()) {
        if (dupIds.length <= 1) continue;
        for (const id of dupIds) {
          const meta = symlinkMeta.get(id)!;
          const node = symlinkNodes.get(id)!;
          const plat = meta.platform || "";
          node.label = `${mnemonic}\n${plat}`;
        }
      }
    }

    // Process inputs and outputs for each spawn
    for (const [spawnId, spawn] of limitedSpawns) {
      const spawnNode = spawnNodes.get(spawnId)!;

      // Process inputs
      const srcArtifacts = new Set<number>();
      const toolArtifacts = new Set<number>();

      // Expand input sets with error handling
      try {
        if (spawn.inputSetId) {
          const inputSet = this.expandInputSet(spawn.inputSetId, data);
          for (const artifactId of inputSet) srcArtifacts.add(artifactId);
        }
        if (spawn.toolSetId) {
          const toolSet = this.expandInputSet(spawn.toolSetId, data);
          for (const artifactId of toolSet) toolArtifacts.add(artifactId);
        }
      } catch (error) {
        console.warn(`Failed to expand input/tool sets for spawn ${spawnId}:`, error);
      }

      // Create input artifact nodes and edges (separately for src and tool)
      const isRunfilesBridge = (rid: number): boolean => {
        try {
          if (!data.runfiles.has(rid)) return false;
          const rf = data.runfiles.get(rid)!;
          if (!rf.inputSetId) return false;
          const ids = this.expandInputSet(rf.inputSetId, data);
          for (const cid of ids) if (data.outputProducers.has(cid)) return true;
        } catch {}
        return false;
      };
      const srcFiltered = Array.from(srcArtifacts).filter((id) => {
        if (!interestingOnly) return true;
        if (this.isArtifactInteresting(id, data)) return true;
        // Preserve runfiles inputs that bridge produced binaries to the TestRunner
        if (data.runfiles.has(id) && isRunfilesBridge(id)) return true;
        return false;
      });
      const srcResult = this.prioritizeAndLimitArtifacts(srcFiltered, spawnId, data, MAX_ARTIFACTS_PER_SPAWN);
      for (const artifactId of srcResult.limited) {
        try {
          const artifactNode = this.getOrCreateArtifactNode(artifactId, data, artifactNodes);
          if (artifactNode) {
            edges.push({
              from: artifactNode.id,
              to: spawnNode.id,
              type: "input",
              inputRole: "src",
            });
          }
        } catch (error) {
          console.warn(`Failed to create artifact node ${artifactId}:`, error);
        }
      }
      if (srcResult.hasOverflow && !interestingOnly) {
        const hiddenCount = srcResult.totalCount - srcResult.limited.length;
        const overflowNode: GraphNode = {
          id: `overflow-src-${spawnId}`,
          type: "artifact",
          nodeId: -1,
          artifactType: "file",
          artifactCategory: "intermediate",
          label: `...+${hiddenCount}`,
        };
        artifactNodes.set(-(spawnId * 2 + 1), overflowNode);
        edges.push({ from: overflowNode.id, to: spawnNode.id, type: "input", inputRole: "src" });
      }

      // Tools
      const toolFiltered = Array.from(toolArtifacts).filter(
        (id) => !interestingOnly || this.isArtifactInteresting(id, data)
      );
      const toolResult = this.prioritizeAndLimitArtifacts(toolFiltered, spawnId, data, MAX_ARTIFACTS_PER_SPAWN);
      for (const artifactId of toolResult.limited) {
        try {
          const artifactNode = this.getOrCreateArtifactNode(artifactId, data, artifactNodes);
          if (artifactNode) {
            edges.push({ from: artifactNode.id, to: spawnNode.id, type: "input", inputRole: "tool" });
          }
        } catch (error) {
          console.warn(`Failed to create tool artifact node ${artifactId}:`, error);
        }
      }
      if (toolResult.hasOverflow && !interestingOnly) {
        const hiddenCount = toolResult.totalCount - toolResult.limited.length;
        const overflowNode: GraphNode = {
          id: `overflow-tool-${spawnId}`,
          type: "artifact",
          nodeId: -1,
          artifactType: "file",
          artifactCategory: "intermediate",
          label: `...+${hiddenCount}`,
        };
        artifactNodes.set(-(spawnId * 2 + 2), overflowNode);
        edges.push({ from: overflowNode.id, to: spawnNode.id, type: "input", inputRole: "tool" });
      }

      // Process outputs
      for (const output of spawn.outputs || []) {
        let outputId = output.outputId;

        // Handle deprecated fields for compatibility
        if (!outputId) {
          outputId = output.fileId || output.directoryId || output.unresolvedSymlinkId;
        }

        if (outputId && (!interestingOnly || this.isArtifactInteresting(outputId, data))) {
          try {
            const artifactNode = this.getOrCreateArtifactNode(outputId, data, artifactNodes);
            if (artifactNode) {
              edges.push({
                from: spawnNode.id,
                to: artifactNode.id,
                type: "output",
              });
            }
          } catch (error) {
            console.warn(`Failed to create output artifact node ${outputId}:`, error);
          }
        }
      }
    }

    // Expand runfiles contents: inputs, symlink entries, and empty files
    try {
      // Helper: expand symlink entry sets (transitive)
      const expandSymlinkEntrySet = (setId?: number): Map<string, number> => {
        const result = new Map<string, number>();
        if (!setId) return result;
        const visited = new Set<number>();
        const dfs = (id: number) => {
          if (!id || visited.has(id)) return;
          visited.add(id);
          const set = data.symlinkEntrySets.get(id);
          if (!set) return;
          // First, process transitive to preserve postorder semantics
          for (const tid of set.transitiveSetIds || []) dfs(tid);
          // Then, apply direct entries (later entries override earlier ones)
          const direct: any = (set as any).directEntries || (set as any).direct_entries;
          if (direct) {
            for (const [relPath, entryId] of Object.entries(direct as Record<string, number>)) {
              result.set(relPath, entryId as number);
            }
          }
        };
        dfs(setId);
        return result;
      };

      // For each runfiles artifact node present in the graph, materialize its contents
      for (const [artifactId, _artNode] of Array.from(artifactNodes.entries())) {
        if (!data.runfiles.has(artifactId)) continue;
        const rf = data.runfiles.get(artifactId)!;
        const runfilesNodeId = `runfiles-${artifactId}`;

        // 1) Canonical artifacts included via input_set_id
        if (rf.inputSetId) {
          try {
            const ids = Array.from(this.expandInputSet(rf.inputSetId, data));
            for (const id of ids) {
              // In Interesting Only mode, keep child->runfiles edges if either
              // (a) the child artifact is interesting, or
              // (b) the child is produced by some spawn and the runfiles artifact itself is consumed
              if (
                interestingOnly &&
                !this.isArtifactInteresting(id, data) &&
                !(data.outputProducers.has(id) && data.consumedArtifacts.has(artifactId))
              ) {
                continue;
              }
              const child = this.getOrCreateArtifactNode(id, data, artifactNodes);
              if (child) {
                edges.push({ from: child.id, to: runfilesNodeId, type: "input" });
                // If interesting-only filtered out the usual spawn->artifact output edge, re-add it here
                // to preserve the GoLink -> binary -> runfiles -> TestRunner chain.
                if (interestingOnly) {
                  const prodSpawnId = data.outputProducers.get(id);
                  if (prodSpawnId !== undefined) {
                    const prodSpawnNode = spawnNodes.get(prodSpawnId);
                    if (prodSpawnNode) {
                      edges.push({ from: prodSpawnNode.id, to: child.id, type: "output" });
                    }
                  }
                }
              }
            }
          } catch {
            /* ignore expansion errors */
          }
        }

        // 2) Symlink entries under workspace subdir and runfiles root
        if (!interestingOnly) {
          const symlinks = expandSymlinkEntrySet((rf as any).symlinksId || (rf as any).symlinks_id);
          const rootSymlinks = expandSymlinkEntrySet((rf as any).rootSymlinksId || (rf as any).root_symlinks_id);
          const addSymlinkPlaceholder = (relPath: string, entryId?: number) => {
            // Try to render the target's label when available
            let targetLabel = "";
            if (entryId && data.files.has(entryId)) targetLabel = this.getFileLabel(data.files.get(entryId)!);
            else if (entryId && data.directories.has(entryId))
              targetLabel = this.getDirectoryLabel(data.directories.get(entryId)!);
            else if (entryId && data.symlinks.has(entryId))
              targetLabel = this.getSymlinkLabel(data.symlinks.get(entryId)!);
            const label = targetLabel ? `${relPath} → ${targetLabel}` : relPath;
            const id = `rf-sym:${artifactId}:${relPath}`;
            const node: GraphNode = {
              id,
              type: "artifact",
              nodeId: -1,
              artifactType: "symlink",
              artifactCategory: "tool",
              label,
              nonSelectable: true,
            };
            extraNodes.push(node);
            edges.push({ from: id, to: runfilesNodeId, type: "input" });
          };
          for (const [p, id] of symlinks.entries()) addSymlinkPlaceholder(p, id);
          for (const [p, id] of rootSymlinks.entries()) addSymlinkPlaceholder(p, id);

          // 3) Empty files
          for (const relPath of rf.emptyFiles || []) {
            const id = `rf-empty:${artifactId}:${relPath}`;
            const node: GraphNode = {
              id,
              type: "artifact",
              nodeId: -1,
              artifactType: "file",
              artifactCategory: "tool",
              label: `∅ ${relPath}`,
              nonSelectable: true,
            };
            extraNodes.push(node);
            edges.push({ from: id, to: runfilesNodeId, type: "input" });
          }
        }

        // Note: intentionally not adding a repo mapping manifest node/edge
      }
    } catch (e) {
      console.warn("Failed to expand runfiles contents:", e);
    }

    // Add all artifact nodes to the main nodes list
    for (const node of artifactNodes.values()) {
      nodes.push(node);
    }
    // Add synthetic runfiles content nodes
    for (const n of extraNodes) nodes.push(n);

    // Mark intermediate artifacts (both produced and consumed within this target)
    this.markIntermediateArtifacts(nodes, edges, data);

    // Compute spawn layers using topological sort
    const spawnLayers = this.computeSpawnLayers(limitedSpawns, data);

    // Compute layer indices for symlink actions relative to spawn layers
    const spawnLayerIndexById = new Map<number, number>();
    spawnLayers.forEach((layer, i) => layer.forEach((spawnId) => spawnLayerIndexById.set(spawnId, i)));

    const symlinkLayers = new Map<number, number>();

    // Helper to resolve a path to an artifact id
    const resolveArtifactId = (p?: string | null) => {
      if (!p) return undefined as number | undefined;
      return data.pathToFileId.get(p) || data.pathToDirId.get(p) || data.pathToSymlinkId.get(p);
    };

    // Fast lookup set of limited spawn IDs
    const limitedSpawnIdSet = new Set(limitedSpawns.map(([id]) => id));

    for (const [sid, action] of limitedSymlinkActions) {
      const producerLayers: number[] = [];
      const consumerLayers: number[] = [];

      // Producers: spawns that produce the symlink input artifact (if any)
      const inId = resolveArtifactId((action as any).inputPath);
      if (inId !== undefined) {
        const prodSpawnId = data.outputProducers.get(inId);
        if (prodSpawnId !== undefined && limitedSpawnIdSet.has(prodSpawnId)) {
          const layerIdx = spawnLayerIndexById.get(prodSpawnId);
          if (layerIdx !== undefined) producerLayers.push(layerIdx);
        }
      }

      // Consumers: spawns that consume the symlink output artifact (if any)
      const outId = resolveArtifactId((action as any).outputPath);
      if (outId !== undefined) {
        for (const [spawnId, spawn] of limitedSpawns) {
          let consumes = false;
          try {
            if (spawn.inputSetId) {
              const set = this.expandInputSet(spawn.inputSetId, data);
              if (set.has(outId)) consumes = true;
            }
            if (!consumes && spawn.toolSetId) {
              const set = this.expandInputSet(spawn.toolSetId, data);
              if (set.has(outId)) consumes = true;
            }
          } catch {
            /* ignore */
          }
          if (consumes) {
            const layerIdx = spawnLayerIndexById.get(spawnId);
            if (layerIdx !== undefined) consumerLayers.push(layerIdx);
          }
        }
      }

      let layer = 0;
      if (producerLayers.length > 0) {
        layer = Math.max(...producerLayers) + 1; // after producers
      }
      if (consumerLayers.length > 0) {
        layer = Math.min(layer, Math.min(...consumerLayers) - 1); // before consumers (may be negative)
      }
      symlinkLayers.set(sid, layer);
    }

    return { nodes, edges, spawnLayers, symlinkActions: limitedSymlinkActions, symlinkLayers };
  }

  private getOrCreateArtifactNode(
    artifactId: number,
    data: IndexedData,
    cache: Map<number, GraphNode>
  ): GraphNode | null {
    if (cache.has(artifactId)) {
      return cache.get(artifactId)!;
    }

    let node: GraphNode | null = null;
    const artifactCategory = this.classifyArtifact(artifactId, data);

    if (data.files.has(artifactId)) {
      const file = data.files.get(artifactId)!;
      const pathId = file.path?.replace(/[^a-zA-Z0-9]/g, "_") || `file-${artifactId}`;
      // Include artifactId to ensure uniqueness even if paths are similar after regex
      const nodeId = `file-${artifactId}-${pathId}`;

      node = {
        id: nodeId,
        type: "artifact",
        nodeId: artifactId,
        artifactType: "file",
        artifactCategory,
        label: this.getFileLabel(file),
      };
    } else if (data.directories.has(artifactId)) {
      const dir = data.directories.get(artifactId)!;
      const pathId = dir.path?.replace(/[^a-zA-Z0-9]/g, "_") || `dir-${artifactId}`;
      node = {
        id: `dir-${artifactId}-${pathId}`,
        type: "artifact",
        nodeId: artifactId,
        artifactType: "directory",
        artifactCategory,
        label: this.getDirectoryLabel(dir),
      };
    } else if (data.symlinks.has(artifactId)) {
      const symlink = data.symlinks.get(artifactId)!;
      const pathId = symlink.path?.replace(/[^a-zA-Z0-9]/g, "_") || `symlink-${artifactId}`;
      node = {
        id: `symlink-${artifactId}-${pathId}`,
        type: "artifact",
        nodeId: artifactId,
        artifactType: "symlink",
        artifactCategory,
        label: this.getSymlinkLabel(symlink),
      };
    } else if (data.runfiles.has(artifactId)) {
      const runfiles = data.runfiles.get(artifactId)!;
      node = {
        id: `runfiles-${artifactId}`,
        type: "artifact",
        nodeId: artifactId,
        artifactType: "runfiles",
        artifactCategory,
        label: this.getRunfilesLabel(runfiles),
      };
    }

    if (node) {
      cache.set(artifactId, node);
    }

    return node;
  }

  private getFileLabel(file: tools.protos.ExecLogEntry.File): string {
    const path = file.path || "unknown";
    const basename = path.split("/").pop() || path;

    // For bazel-out paths, include platform information to distinguish different configurations
    if (path.includes("bazel-out/") && path.includes("/bin/")) {
      const platformMatch = path.match(/bazel-out\/([^/]+)\//);
      if (platformMatch) {
        const platform = platformMatch[1];
        return `${basename} (${platform})`;
      }
    }

    // Do not append short hash; DigestComponent handles showing digests
    return basename;
  }

  private getDirectoryLabel(dir: tools.protos.ExecLogEntry.Directory): string {
    const path = dir.path || "unknown";
    const basename = path.split("/").pop() || path;
    const fileCount = dir.files?.length || 0;
    return `${basename}/ (${fileCount} files)`;
  }

  private getSymlinkLabel(symlink: tools.protos.ExecLogEntry.UnresolvedSymlink): string {
    const path = symlink.path || "unknown";
    const target = symlink.targetPath || "?";
    const basename = path.split("/").pop() || path;
    return `${basename} → ${target}`;
  }

  private getRunfilesLabel(runfiles: tools.protos.ExecLogEntry.RunfilesTree): string {
    const path = runfiles.path || "unknown";
    const basename = path.split("/").pop() || path;

    // Extract tool name from common patterns
    if (path.includes("protoc.runfiles")) {
      return "📦 protoc runfiles";
    }
    if (path.includes("protoc-gen-")) {
      return "📦 protoc-gen-protobufjs runfiles";
    }

    return `📦 ${basename}`;
  }

  private markIntermediateArtifacts(nodes: GraphNode[], edges: GraphEdge[], data: IndexedData) {
    const producedArtifacts = new Set<string>();
    const consumedArtifacts = new Set<string>();

    for (const edge of edges) {
      if (edge.type === "output") {
        producedArtifacts.add(edge.to);
      } else if (edge.type === "input") {
        consumedArtifacts.add(edge.from);
      }
    }

    for (const node of nodes) {
      if (node.type === "artifact") {
        const produced =
          producedArtifacts.has(node.id) ||
          (node.nodeId > 0 &&
            (data.outputProducers.has(node.nodeId) || data.symlinkProducedArtifacts.has(node.nodeId)));
        const consumed = consumedArtifacts.has(node.id) || (node.nodeId > 0 && data.consumedArtifacts.has(node.nodeId));
        node.isIntermediate = produced && consumed;
        node.hasProducer = produced;
        node.hasConsumer = consumed;
      }
    }
  }

  private computeSpawnLayers(targetSpawns: [number, tools.protos.ExecLogEntry.Spawn][], data: IndexedData): number[][] {
    // Build dependency graph between spawns
    const spawnDeps = new Map<number, Set<number>>(); // spawn -> dependencies

    for (const [spawnId] of targetSpawns) {
      spawnDeps.set(spawnId, new Set());
    }

    // Find dependencies via locally produced artifacts
    for (const [consumerSpawnId, consumerSpawn] of targetSpawns) {
      const inputArtifacts = new Set<number>();

      if (consumerSpawn.inputSetId) {
        const inputSet = this.expandInputSet(consumerSpawn.inputSetId, data);
        for (const artifactId of inputSet) {
          inputArtifacts.add(artifactId);
        }
      }

      if (consumerSpawn.toolSetId) {
        const toolSet = this.expandInputSet(consumerSpawn.toolSetId, data);
        for (const artifactId of toolSet) {
          inputArtifacts.add(artifactId);
        }
      }

      // Check if any input is produced by another spawn in this target
      for (const artifactId of inputArtifacts) {
        const producerSpawnId = data.outputProducers.get(artifactId);
        if (producerSpawnId && spawnDeps.has(producerSpawnId)) {
          spawnDeps.get(consumerSpawnId)!.add(producerSpawnId);
        }
      }

      // Also add deps via symlink actions: if the consumer uses a symlink output artifact
      // that ultimately points to an artifact produced by another spawn, depend on that spawn.
      for (const artifactId of inputArtifacts) {
        const symlinkActionId = data.symlinkOutputProducers.get(artifactId);
        if (symlinkActionId !== undefined) {
          const action = data.symlinkActions.get(symlinkActionId);
          if (action) {
            const inPath: string | undefined = (action as any).inputPath || (action as any).input_path;
            if (inPath) {
              const tryResolve = (p?: string) =>
                (p && (data.pathToFileId.get(p) || data.pathToDirId.get(p) || data.pathToSymlinkId.get(p))) as
                  | number
                  | undefined;
              let inArtId = tryResolve(inPath);
              if (inArtId === undefined) {
                // Normalize optional ST segment in exec paths (e.g., -opt-exec-ST-<hash>/ -> -opt-exec/)
                const norm = inPath.replace(/-opt-exec-ST-[^/]+\//, "-opt-exec/");
                inArtId = tryResolve(norm);
              }
              if (inArtId !== undefined) {
                const realProducer = data.outputProducers.get(inArtId);
                if (realProducer && spawnDeps.has(realProducer)) {
                  spawnDeps.get(consumerSpawnId)!.add(realProducer);
                }
              }
            }
          }
        }
      }

      // Heuristic: Some consumers (e.g., TestRunner) reference the produced binary via
      // a runfiles dir env var (e.g., TEST_SRCDIR, JAVA_RUNFILES, PYTHON_RUNFILES), which ends
      // with ".runfiles". Link that directory back to the binary path to infer dependency.
      try {
        const env = (consumerSpawn.envVars || []) as Array<{ name?: string; value?: string }>;
        const runfilesVars = new Set(["TEST_SRCDIR", "JAVA_RUNFILES", "PYTHON_RUNFILES"]);
        for (const ev of env) {
          const name = (ev.name || "").toUpperCase();
          const val = ev.value || "";
          if (!runfilesVars.has(name)) continue;
          if (!val.endsWith(".runfiles")) continue;
          const base = val.slice(0, -".runfiles".length);
          const tryResolve = (p?: string) =>
            (p && (data.pathToFileId.get(p) || data.pathToDirId.get(p) || data.pathToSymlinkId.get(p))) as
              | number
              | undefined;
          let binId = tryResolve(base);
          if (binId === undefined) {
            const norm = base.replace(/-opt-exec-ST-[^/]+\//, "-opt-exec/");
            binId = tryResolve(norm);
          }
          if (binId !== undefined) {
            const prod = data.outputProducers.get(binId);
            if (prod && spawnDeps.has(prod)) {
              spawnDeps.get(consumerSpawnId)!.add(prod);
            }
          }
        }
      } catch {
        /* ignore */
      }
    }

    // Topological sort with cycle handling
    const layers: number[][] = [];
    const remaining = new Set(spawnDeps.keys());

    while (remaining.size > 0) {
      const currentLayer: number[] = [];
      const layerCandidates = Array.from(remaining).filter((spawnId) => {
        const deps = spawnDeps.get(spawnId)!;
        return Array.from(deps).every((depId) => !remaining.has(depId));
      });

      if (layerCandidates.length === 0) {
        // Handle cycles by taking any remaining spawn
        const nextSpawn = Array.from(remaining)[0];
        currentLayer.push(nextSpawn);
        remaining.delete(nextSpawn);
      } else {
        for (const spawnId of layerCandidates) {
          currentLayer.push(spawnId);
          remaining.delete(spawnId);
        }
      }

      layers.push(currentLayer);
    }

    return layers;
  }

  private renderChart() {
    if (!this.chartRef.current || !this.state.selectedLabel || !this.state.indexedData) {
      return;
    }

    this.setState({ chartLoading: true });

    // If G6 graph exists, just update data instead of recreating
    if (this.g6Graph) {
      return this.updateG6GraphData();
    }

    // Clean container (managed by G6 instance)
    this.chartRef.current.innerHTML = "";
    const graphData = this.buildGraphData(this.state.selectedLabel, this.state.indexedData);
    if (graphData.nodes.length === 0) {
      const msg = document.createElement("div");
      msg.style.cssText = "display:flex;align-items:center;justify-content:center;width:100%;height:100%;color:#666;";
      msg.textContent = "No spawns found for this target";
      this.chartRef.current.appendChild(msg);
      this.setState({ chartLoading: false });
      return;
    }

    // G6 rendering
    try {
      const width = this.chartRef.current.clientWidth || 800;
      const height = this.chartRef.current.clientHeight || 600;

      // Quick lookup for nodes by id
      const nodeById = new Map(graphData.nodes.map((n) => [n.id, n]));

      // (Combos will be constructed after spawnPositions and nodes/edges mapping)
      // Map inputs by spawn and reverse map spawns by input artifact
      const inputsBySpawn = new Map<string, string[]>();
      const spawnsByInput = new Map<string, string[]>();
      for (const e of graphData.edges) {
        if (e.type !== "input") continue;
        const artifactId = e.from;
        const spawnId = e.to;
        if (typeof spawnId !== "string") continue;
        const n = nodeById.get(spawnId);
        if (!n || n.type !== "spawn") continue;
        if (!inputsBySpawn.has(spawnId)) inputsBySpawn.set(spawnId, []);
        const arr = inputsBySpawn.get(spawnId)!;
        if (!arr.includes(artifactId)) arr.push(artifactId);
        if (!spawnsByInput.has(artifactId)) spawnsByInput.set(artifactId, []);
        const arr2 = spawnsByInput.get(artifactId)!;
        if (!arr2.includes(spawnId)) arr2.push(spawnId);
      }
      // Map outputs by spawn and producer by artifact
      const outputsBySpawn = new Map<string, string[]>();
      const producerByArtifact = new Map<string, string>();
      for (const e of graphData.edges) {
        if (e.type !== "output") continue;
        const spawnId = e.from; // spawn -> artifact
        const artifactId = e.to;
        if (typeof spawnId !== "string") continue;
        const n = nodeById.get(spawnId);
        if (!n || n.type !== "spawn") continue;
        if (!outputsBySpawn.has(spawnId)) outputsBySpawn.set(spawnId, []);
        outputsBySpawn.get(spawnId)!.push(artifactId);
        producerByArtifact.set(artifactId, spawnId);
      }

      // Compute a unified rectangle size so that the largest spawn/symlink label fits comfortably
      const labelFontSize = 14; // px
      const fontFamily = "-apple-system,BlinkMacSystemFont,Segoe UI,Roboto,Helvetica,Arial,sans-serif";
      const font = `${labelFontSize}px ${fontFamily}`;
      const padX = 16;
      const padY = 10;
      const minRectW = 140;
      const minRectH = 44;
      let maxLabelW = 0;
      let maxLines = 1;
      for (const n of graphData.nodes) {
        if (n.type !== "spawn") continue; // spawn nodes also include symlink actions flagged via isSymlinkAction
        const text = (n.label || "").toString();
        const lines = text.split(/\n/);
        maxLines = Math.max(maxLines, lines.length);
        for (const line of lines) {
          const w = this.measureTextWidth(line, font);
          if (w > maxLabelW) maxLabelW = w;
        }
      }
      const lineHeight = Math.ceil(labelFontSize * 1.2);
      const rectW = Math.max(minRectW, Math.ceil(maxLabelW) + 2 * padX);
      const rectH = Math.max(minRectH, maxLines * lineHeight + 2 * padY);

      // Compute spawn/symlink positions using the band layout
      const spawnPositions = new Map<string, { x: number; y: number }>();
      // Track row indices and band metrics to place inputs/outputs on distinct rows
      const spawnRowIndexById = new Map<string, number>();
      let rowCentersArr: number[] = [];
      let rowHeightsArr: number[] = [];
      ((): void => {
        const { xGap, minRowHeight, artifactOffset, innerMargin, rowGap, spawnBandGap } = this.state.spacing;
        const spawnCoreHeight = rectH;
        const symlinkCoreHeight = rectH;
        const computeBandHeight = (inCount: number, outCount: number, coreHeight: number = spawnCoreHeight) => {
          const rowStep = this.state.spacing.inputRectRowSpacing || artifactOffset;
          const maxRows = Math.max(1, Math.floor(this.state.spacing.inputRectMaxHeight / Math.max(1, rowStep)));
          const halfRangeIn = Math.ceil(Math.min(inCount, maxRows) / 2) * rowStep;
          const halfRangeOut = Math.ceil(outCount / 2) * artifactOffset;
          const halfRange = Math.max(halfRangeIn, halfRangeOut);
          return (
            Math.max(minRowHeight, halfRange * 2 + coreHeight + innerMargin) + (this.state.spacing.rowExtraPadding || 0)
          );
        };
        const nodeLabelById = new Map<string, string>();
        graphData.nodes.forEach((n) => nodeLabelById.set(n.id, n.label || ""));
        const compareByLabel = (aId: string, bId: string) => {
          const a = (nodeLabelById.get(aId) || aId).toLowerCase();
          const b = (nodeLabelById.get(bId) || bId).toLowerCase();
          if (a < b) return -1;
          if (a > b) return 1;
          return aId.localeCompare(bId);
        };
        const spawnIdToNodeId = new Map<number, string>();
        const symlinkIdToNodeId = new Map<number, string>();
        for (const n of graphData.nodes) {
          if (n.type === "spawn") {
            if ((n as any).isSymlinkAction) symlinkIdToNodeId.set(n.nodeId, n.id);
            else spawnIdToNodeId.set(n.nodeId, n.id);
          }
        }
        const placeTopToBottom = () => {
          let minSymlinkLayer = Infinity,
            maxSymlinkLayer = -Infinity;
          graphData.symlinkLayers.forEach((layer) => {
            if (layer < minSymlinkLayer) minSymlinkLayer = layer;
            if (layer > maxSymlinkLayer) maxSymlinkLayer = layer;
          });
          const shift = isFinite(minSymlinkLayer) && minSymlinkLayer < 0 ? -minSymlinkLayer : 0;
          const totalRows = Math.max(
            graphData.spawnLayers.length + shift,
            isFinite(maxSymlinkLayer) ? maxSymlinkLayer + shift + 1 : 0
          );
          const rows: string[][] = Array.from({ length: totalRows }, () => []);
          graphData.spawnLayers.forEach((layer, layerIndex) => {
            const rowIndex = layerIndex + shift;
            layer
              .sort((a, b) => a - b)
              .forEach((spawnId) => rows[rowIndex].push(spawnIdToNodeId.get(spawnId) || `spawn-${spawnId}`));
          });
          graphData.symlinkLayers.forEach((layerIdx, sid) => {
            const rowIndex = layerIdx + shift;
            if (rowIndex >= 0 && rowIndex < rows.length)
              rows[rowIndex].push(symlinkIdToNodeId.get(sid) || `symlinkact-${sid}`);
          });
          const rowHeights = rows.map((nodeIds) => {
            let rowHeight = minRowHeight;
            for (const nodeId of nodeIds) {
              const isSpawnNode = !!graphData.nodes.find(
                (n) => n.id === nodeId && n.type === "spawn" && !(n as any).isSymlinkAction
              );
              if (isSpawnNode) {
                const inCount = (inputsBySpawn.get(nodeId) || []).length;
                const outCount = (outputsBySpawn.get(nodeId) || []).length;
                rowHeight = Math.max(rowHeight, computeBandHeight(inCount, outCount, spawnCoreHeight));
              }
            }
            return rowHeight;
          });
          rowHeightsArr = rowHeights;
          const rowCenters: number[] = [];
          let yCursor = 0;
          for (let i = 0; i < rowHeights.length; i++) {
            const center = yCursor + rowHeights[i] / 2;
            rowCenters.push(center);
            yCursor += rowHeights[i] + rowGap;
          }
          rowCentersArr = rowCenters;
          rows.forEach((nodeIds, rowIndex) => {
            const ordered = [...nodeIds].sort(compareByLabel);
            ordered.forEach((nodeId, j) => {
              const xBase = j * xGap;
              const yBase = rowCenters[rowIndex];
              spawnPositions.set(nodeId, { x: xBase, y: yBase });
              // Mark spawn rows (include symlink actions so their I/O can be placed relative to their row)
              const nd = nodeById.get(nodeId);
              if (nd && nd.type === "spawn") spawnRowIndexById.set(nodeId, rowIndex);
            });
          });
        };
        const placeLeftToRight = () => {
          let minSymlinkLayer = Infinity,
            maxSymlinkLayer = -Infinity;
          graphData.symlinkLayers.forEach((layer) => {
            if (layer < minSymlinkLayer) minSymlinkLayer = layer;
            if (layer > maxSymlinkLayer) maxSymlinkLayer = layer;
          });
          const shift = isFinite(minSymlinkLayer) && minSymlinkLayer < 0 ? -minSymlinkLayer : 0;
          const totalCols = Math.max(
            graphData.spawnLayers.length + shift,
            isFinite(maxSymlinkLayer) ? maxSymlinkLayer + shift + 1 : 0
          );
          const cols: string[][] = Array.from({ length: totalCols }, () => []);
          graphData.spawnLayers.forEach((layer, layerIndex) => {
            const colIndex = layerIndex + shift;
            layer
              .sort((a, b) => a - b)
              .forEach((spawnId) => cols[colIndex].push(spawnIdToNodeId.get(spawnId) || `spawn-${spawnId}`));
          });
          graphData.symlinkLayers.forEach((layerIdx, sid) => {
            const colIndex = layerIdx + shift;
            if (colIndex >= 0 && colIndex < cols.length)
              cols[colIndex].push(symlinkIdToNodeId.get(sid) || `symlinkact-${sid}`);
          });
          cols.forEach((nodeIds, colIndex) => {
            const ordered = [...nodeIds].sort(compareByLabel);
            let yCursor = 0;
            ordered.forEach((nodeId) => {
              const inCount = (inputsBySpawn.get(nodeId) || []).length;
              const outCount = (outputsBySpawn.get(nodeId) || []).length;
              const isSymlink = !!graphData.nodes.find((n) => n.id === nodeId && (n as any).isSymlinkAction);
              const bandHeight = computeBandHeight(inCount, outCount, isSymlink ? symlinkCoreHeight : spawnCoreHeight);
              const centerY = yCursor + bandHeight / 2;
              const xBase = colIndex * xGap;
              spawnPositions.set(nodeId, { x: xBase, y: centerY });
              yCursor += bandHeight + spawnBandGap;
              // Mark spawn rows (include symlink actions as well)
              const nd = nodeById.get(nodeId);
              if (nd && nd.type === "spawn") spawnRowIndexById.set(nodeId, colIndex);
            });
          });
        };
        if (this.state.orientation === "top-to-bottom") placeTopToBottom();
        else placeLeftToRight();
      })();
      // Compute absolute positions for all nodes (Option 1)
      const nodePositions = new Map<string, { x: number; y: number }>();
      // Seed with spawn/symlink positions
      for (const [id, p] of Array.from(spawnPositions.entries())) nodePositions.set(id, { x: p.x, y: p.y });
      // Place inputs in a rectangle above the spawn row using near-square, row-first packing
      {
        const step = 500; // increase spacing 10x for inputs
        const baseRows = 10;
        const baseCols = 20;
        const computeGrid = (count: number, maxRows: number, maxCols: number) => {
          const n = Math.max(0, count);
          if (n === 0) return { rows: 1, cols: 1, capacity: 1 };
          let rows = Math.max(1, Math.min(maxRows, Math.round(Math.sqrt(n))));
          let cols = Math.max(1, Math.min(maxCols, Math.ceil(n / rows)));
          rows = Math.max(1, Math.min(maxRows, Math.ceil(n / cols)));
          while (rows * cols < n && (cols < maxCols || rows < maxRows)) {
            if (cols <= rows && cols < maxCols) cols++;
            else if (rows < maxRows) rows++;
            else cols++;
          }
          return { rows, cols, capacity: rows * cols };
        };
        for (const [spawnNodeId, inputs] of inputsBySpawn.entries()) {
          const pos = spawnPositions.get(spawnNodeId);
          if (!pos) continue;
          const rowIdx = spawnRowIndexById.get(spawnNodeId);
          const rowCenter = rowIdx !== undefined ? rowCentersArr[rowIdx] : pos.y;
          const rowHeight = rowIdx !== undefined ? rowHeightsArr[rowIdx] : rectH;
          const spawnNode = nodeById.get(spawnNodeId);
          const spawnNumId = (spawnNode?.nodeId as number) || 0;
          const isExpanded = this.state.expandedSpawns.has(spawnNumId);
          const maxRows = isExpanded ? baseRows * 2 : baseRows;
          const maxCols = isExpanded ? baseCols * 2 : baseCols;
          const desired = inputs.length;
          // Compute near-square grid allowing room for an overflow cell
          const { rows, cols, capacity } = computeGrid(desired + 1, maxRows, maxCols);
          const centerY = (rowCenter || pos.y) - (rowHeight || rectH) / 2 - rectH * 1.2;
          const gridW = (cols - 1) * step;
          const gridH = (rows - 1) * step;
          const originX = pos.x - gridW / 2;
          const originY = centerY - gridH / 2;
          const toPlace = inputs.slice(0, Math.min(inputs.length, capacity));
          let maxIdx = toPlace.length - 1;
          let overflowNodeId: string | null = null;
          if (inputs.length > capacity) {
            // Reserve last slot for overflow synthetic node
            maxIdx = capacity - 2; // last index is overflow
            overflowNodeId = `of-in-${spawnNumId}`;
          }
          for (let i = 0, p = 0; i <= maxIdx; i++, p++) {
            const artId = toPlace[i];
            // Prefer producer placement only if producer row is strictly above the consumer row.
            // If the consumer is in the top layer (or at/above the producer), keep the input above the consumer.
            const prodNodeId = producerByArtifact.get(artId);
            const consumerRow = rowIdx ?? 0;
            if (prodNodeId) {
              const prodRow = spawnRowIndexById.get(prodNodeId) ?? Number.MAX_SAFE_INTEGER;
              if (prodRow < consumerRow) {
                continue; // let outputs placement near producer handle it
              }
            }
            if (!nodePositions.has(artId)) {
              // row-first fill
              const r = Math.floor(p / cols);
              const c = p % cols;
              let x = originX + c * step;
              let y = originY + r * step;
              const nd = nodeById.get(artId);
              if (nd && nd.type === "artifact" && nd.artifactType === "runfiles") {
                // Slight downward nudge towards the consumer row (kept small; may be overridden by side-by-side pass)
                y += step * 0.2;
              }
              nodePositions.set(artId, { x, y });
            }
          }
          if (overflowNodeId) {
            // Place overflow node in the last cell
            const p = capacity - 1;
            const r = Math.floor(p / cols);
            const c = p % cols;
            const x = originX + c * step;
            const y = originY + r * step;
            nodePositions.set(overflowNodeId, { x, y });
          }
        }
      }
      // Place outputs in a rectangle below the spawn row using near-square, row-first packing
      {
        const step = 500; // Increase step size to prevent overlap after compression (was 10)
        const baseRows = 10;
        const baseCols = 20;
        for (const [spawnNodeId, outs] of outputsBySpawn.entries()) {
          const pos = spawnPositions.get(spawnNodeId);
          if (!pos) continue;
          const rowIdx = spawnRowIndexById.get(spawnNodeId);
          const rowCenter = rowIdx !== undefined ? rowCentersArr[rowIdx] : pos.y;
          const rowHeight = rowIdx !== undefined ? rowHeightsArr[rowIdx] : rectH;
          const spawnNode = nodeById.get(spawnNodeId);
          const spawnNumId = (spawnNode?.nodeId as number) || 0;
          const isExpanded = this.state.expandedSpawns.has(spawnNumId);
          const maxRows = isExpanded ? baseRows * 2 : baseRows;
          const maxCols = isExpanded ? baseCols * 2 : baseCols;
          const desired = outs.length;
          const { rows, cols, capacity } = (function () {
            const n = Math.max(0, desired);
            if (n === 0) return { rows: 1, cols: 1, capacity: 1 };
            let rows = Math.max(1, Math.min(maxRows, Math.round(Math.sqrt(n + 1))));
            let cols = Math.max(1, Math.min(maxCols, Math.ceil((n + 1) / rows)));
            rows = Math.max(1, Math.min(maxRows, Math.ceil((n + 1) / cols)));
            while (rows * cols < n + 1 && (cols < maxCols || rows < maxRows)) {
              if (cols <= rows && cols < maxCols) cols++;
              else if (rows < maxRows) rows++;
              else cols++;
            }
            return { rows, cols, capacity: rows * cols };
          })();
          const centerY = (rowCenter || pos.y) + (rowHeight || rectH) / 2 + rectH * 1.2;
          const gridW = (cols - 1) * step;
          const gridH = (rows - 1) * step;
          const originX = pos.x - gridW / 2;
          const originY = centerY - gridH / 2;
          const toPlace = outs.slice(0, Math.min(outs.length, capacity));
          let maxIdx = toPlace.length - 1;
          let overflowNodeId: string | null = null;
          if (outs.length > capacity) {
            maxIdx = capacity - 2;
            overflowNodeId = `of-out-${spawnNumId}`;
          }
          for (let i = 0, p = 0; i <= maxIdx; i++, p++) {
            const artId = toPlace[i];
            if (!nodePositions.has(artId)) {
              const r = Math.floor(p / cols);
              const c = p % cols;
              const x = originX + c * step;
              const y = originY + r * step;

              nodePositions.set(artId, { x, y });
            }
          }
          if (overflowNodeId) {
            const p = capacity - 1;
            const r = Math.floor(p / cols);
            const c = p % cols;
            const x = originX + c * step;
            const y = originY + r * step;
            nodePositions.set(overflowNodeId, { x, y });
          }
        }
      }

      // Align runfiles next to their produced child (binary), side-by-side
      {
        const SIDE = 800; // pre-scale horizontal offset (~40px after scaling)
        // Collect children for each runfiles artifact via edges (artifact -> runfiles)
        const childrenByRunfiles = new Map<string, string[]>();
        for (const e of graphData.edges) {
          if (e.type !== "input") continue;
          const dst = e.to;
          const dstNode = nodeById.get(dst);
          if (!dstNode || dstNode.type !== "artifact" || dstNode.artifactType !== "runfiles") continue;
          const src = e.from;
          if (!childrenByRunfiles.has(dst)) childrenByRunfiles.set(dst, []);
          const arr = childrenByRunfiles.get(dst)!;
          if (!arr.includes(src)) arr.push(src);
        }

        for (const [rfId, children] of childrenByRunfiles.entries()) {
          // Prefer a child that is produced (spawn -> child exists) and has a position
          let anchorChild: string | null = null;
          for (const cid of children) {
            if (producerByArtifact.has(cid) && nodePositions.has(cid)) {
              anchorChild = cid;
              break;
            }
          }
          if (!anchorChild) continue;
          const anchPos = nodePositions.get(anchorChild)!;
          // Place runfiles to the right of the child at the same Y
          nodePositions.set(rfId, { x: anchPos.x + SIDE, y: anchPos.y });
        }
      }

      // Compress distances by 5x to reduce spacing
      {
        const SCALE = 1 / 20; // shrink distances to 5%
        let minX = Infinity,
          minY = Infinity;
        nodePositions.forEach(({ x, y }) => {
          if (x < minX) minX = x;
          if (y < minY) minY = y;
        });
        const margin = 40;
        nodePositions.forEach((pos, id) => {
          const nx = (pos.x - minX) * SCALE + margin;
          const ny = (pos.y - minY) * SCALE + margin;
          nodePositions.set(id, { x: nx, y: ny });
        });
      }
      const extraNodes: G6NodeData[] = [];
      const nodes: G6NodeData[] = graphData.nodes.map((n) => {
        // Derive per-node type for rendering shapes
        let nodeType: string = "circle";
        if (n.type === "spawn") nodeType = "rect";
        else if (n.artifactType === "directory") nodeType = "rect";
        else if (n.artifactType === "symlink" || n.artifactType === "runfiles") nodeType = "triangle";
        const p = nodePositions.get(n.id);
        const base: G6NodeData = {
          id: n.id,
          type: nodeType,
          data: n as any,
          style: p ? { x: p.x, y: p.y } : undefined,
        };
        return base;
      });
      // Add synthetic overflow nodes (if any positions were assigned)
      const makeOverflowNode = (
        id: string,
        x: number,
        y: number,
        parentSpawnId: number,
        io: "in" | "out"
      ): G6NodeData => ({
        id,
        type: "circle",
        data: { type: "artifact", isOverflow: true, parentSpawnId, io },
        style: { x, y },
      });
      for (const [id, pos] of nodePositions.entries()) {
        if (id.startsWith("of-in-") || id.startsWith("of-out-")) {
          const spawnId = parseInt(id.replace("of-in-", "").replace("of-out-", ""), 10);
          const io: "in" | "out" = id.startsWith("of-in-") ? "in" : "out";
          extraNodes.push(makeOverflowNode(id, pos.x, pos.y, isNaN(spawnId) ? -1 : spawnId, io));
        }
      }
      nodes.push(...extraNodes);
      const edges: G6EdgeData[] = graphData.edges.map((e) => {
        const srcNode = nodeById.get(e.from);
        const dstNode = nodeById.get(e.to);
        const edge: G6EdgeData = {
          source: e.from,
          target: e.to,
          data: { type: e.type },
          style: {} as any,
        } as G6EdgeData;
        // Route to ports
        if (e.type === "input" && dstNode && dstNode.type === "spawn") {
          // Artifact -> spawn input: send tools to tools port, others to srcs
          const role = (e as any).inputRole;
          if ((dstNode as any).isSymlinkAction) (edge.style as any).targetPort = "in";
          else (edge.style as any).targetPort = role === "tool" ? "tools" : "srcs";
        }
        if (e.type === "output" && srcNode && srcNode.type === "spawn") {
          // spawn -> artifact output
          (edge.style as any).sourcePort = "out";
        }
        return edge;
      });

      // No combos in Option 1 (absolute positioning)
      const combos: any[] = [];

      const graph = new G6Graph({
        container: this.chartRef.current,
        width,
        height,
        // Prevent excessive zoom-out and excessive zoom-in
        zoomRange: [0.5, 2.5],
        // No layout, no combos: absolute positions
        plugins: [
          {
            type: "tooltip",
            key: "tbd-tooltip",
            trigger: "hover",
            position: "top-right",
            enable: (e: IElementEvent, _items: ElementDatum[]) => ["node", "edge"].includes((e as any)?.targetType),
            onOpenChange: () => {},
            getContent: async (_evt: IElementEvent, items: ElementDatum[]) => {
              try {
                const it = items && items[0];
                const id: string | undefined = (it as any)?.id || (it as any)?.data?.id || (_evt as any)?.target?.id;
                if (!id) return "";
                const n = nodeById.get(id);
                if (n) {
                  // Overflow nodes
                  if (String(n.id).startsWith("overflow-")) {
                    return `<b>Hidden Artifacts</b><br/>${n.label} more artifacts<br/><i>Click to expand all</i>`;
                  }
                  if (n.type === "spawn") {
                    const primary = n.primaryOutputLabel
                      ? `<br/><span style=\"color:#666\">Primary: ${n.primaryOutputLabel}</span>`
                      : "";
                    const platform = n.platform ? `<br/><span style=\"color:#888\">Platform: ${n.platform}</span>` : "";
                    const tools =
                      n.toolChain && n.toolChain.length
                        ? `<br/><span style=\"color:#666\">Toolchain: ${n.toolChain.join(" · ")}</span>`
                        : "";
                    const status = n.spawnHasInvalidOutputPaths
                      ? `<br/><span style=\"color:#e67e22\">Has invalid outputs</span>`
                      : "";
                    return `<b>${n.label}</b>${primary}${platform}${tools}${status}`;
                  }
                  const category = n.artifactCategory ? ` (${n.artifactCategory})` : "";
                  return `<b>${n.label}</b><br/>Type: ${n.artifactType}${category}`;
                }
                // Edge tooltip
                if (items && items.length && (items[0] as any)?.data) {
                  const e: any = items[0];
                  const src = nodeById.get(e.source);
                  const dst = nodeById.get(e.target);
                  const edgeType = e?.data?.type;
                  if (edgeType === "input") {
                    const category = src?.artifactCategory ? ` (${src.artifactCategory})` : "";
                    return `<b>Input:</b> ${src?.label || e.source}${category}<br/><b>Used by:</b> ${dst?.label || e.target}`;
                  }
                  if (edgeType === "output") {
                    const category = dst?.artifactCategory ? ` (${dst.artifactCategory})` : "";
                    return `<b>Produced by:</b> ${src?.label || e.source}<br/><b>Output:</b> ${dst?.label || e.target}${category}`;
                  }
                }
              } catch {}
              return "";
            },
          },
        ],
        node: {
          style: (d: G6NodeData) => {
            const node = nodeById.get(d.id);
            // Fallback for synthetic overflow nodes not present in nodeById
            if (!node) {
              const idStr = String(d.id);
              const isOverflow =
                idStr.startsWith("overflow-") || idStr.startsWith("of-in-") || idStr.startsWith("of-out-");
              return isOverflow
                ? {
                    size: 12,
                    fill: "#bdc3c7",
                    stroke: "#ffffff",
                    label: true,
                    labelText: "...",
                    labelPlacement: "center",
                    labelFontSize: 12,
                  }
                : { size: 10, fill: "#bdc3c7", stroke: "#ffffff" };
            }
            const isSpawn = node.type === "spawn";
            const isSymlink = !!(node as any).isSymlinkAction;
            const hasProducer = (node as any).hasProducer === true;
            const hasConsumer = (node as any).hasConsumer === true;
            const isInteresting = node.type === "artifact" && hasProducer && hasConsumer;
            const fill = isSpawn ? (isSymlink ? "#8e44ad" : "#2c82c9") : isInteresting ? "#f39c12" : "#bdc3c7";
            const stroke = isSpawn ? (isSymlink ? "#8e44ad" : "#2c82c9") : "#ffffff";
            const size = isSpawn ? ([rectW, rectH] as any) : 10;
            // Show overflow label and spawn label; hide most artifact labels
            const idStr = String(node.id);
            const labelText = isSpawn
              ? node.label || ""
              : idStr.startsWith("overflow-") || idStr.startsWith("of-in-") || idStr.startsWith("of-out-")
                ? node.label || ""
                : "";
            const radius = isSpawn ? 8 : 4;
            // Non-selectable synthetic nodes use dashed border
            const borderType = (node as any).nonSelectable ? "dashed" : "solid";
            // Configure ports: spawns have left srcs/tools and right out; symlink actions have left in, right out
            let port = false as any;
            let ports: any[] | undefined;
            let portR: number | undefined;
            let portStroke: string | undefined;
            let portLineWidth: number | undefined;
            if (isSpawn) {
              port = true;
              portR = 3;
              portStroke = "#ffffff";
              portLineWidth = 1;
              if (isSymlink) {
                ports = [
                  { key: "in", placement: "top", fill: "#95a5a6" },
                  { key: "out", placement: "bottom", fill: "#2ecc71" },
                ];
              } else {
                ports = [
                  { key: "srcs", placement: [0.35, 0], fill: "#95a5a6" },
                  { key: "tools", placement: [0.65, 0], fill: "#f39c12" },
                  { key: "out", placement: "bottom", fill: "#2ecc71" },
                ];
              }
            }
            return {
              size,
              fill,
              stroke,
              label: true,
              labelText,
              labelPlacement: "center",
              labelFontSize,
              labelWordWrap: true,
              labelMaxWidth: rectW - 2 * padX,
              labelMaxLines: 50,
              labelFill: "#ffffff",
              radius,
              lineDash: borderType === "dashed" ? [4, 2] : undefined,
              port,
              ports,
              portR,
              portStroke,
              portLineWidth,
            };
          },
          state: {
            // Simplified: only 'active' vs 'inactive' visually.
            // Keep neighborActive/selected mapped to active for consistency.
            active: { lineWidth: 2.2, opacity: 1 },
            neighborActive: { lineWidth: 2.2, opacity: 1 },
            selected: { lineWidth: 2.2, opacity: 1 },
            inactive: { opacity: 0.22 },
          },
        },
        edge: {
          type: "cubic-vertical",
          style: (model: G6EdgeData) => {
            const t = model?.data?.type;
            const s: any = (model as any)?.style || {};
            return {
              stroke: t === "output" ? "#2ecc71" : "#c7cfd6",
              lineWidth: 1.2,
              sourcePort: s.sourcePort,
              targetPort: s.targetPort,
            } as any;
          },
          state: {
            // Same simplified model for edges
            active: { lineWidth: 2.0, opacity: 1 },
            neighborActive: { lineWidth: 2.0, opacity: 1 },
            selected: { lineWidth: 2.0, opacity: 1 },
            inactive: { opacity: 0.22 },
          },
        },
        behaviors: [
          "drag-canvas",
          "zoom-canvas",
          // Highlight relations on hover
          { key: "hover-activate", type: "hover-activate", degree: 1, direction: "both", inactiveState: "inactive" },
          // Single select on click; dim others
          {
            key: "click-select",
            type: "click-select",
            multiple: false,
            state: "active",
            unselectedState: "inactive",
            enable: (e: IEvent) => (e as any)?.targetType === "node",
          },
        ],
        autoFit: "view",
        padding: 20,
      });
      // Set data (G6 v5)
      graph.setData({ nodes, edges, combos });

      // Render with safety check
      try {
        graph.render();
      } catch (renderError) {
        console.warn("G6 render failed:", renderError);
        // Don't throw - continue with setup
      }

      // Update selected item (states are handled by click-select behavior)
      graph.on(NodeEvent.CLICK, (ev: IElementEvent) => {
        // G6 v5 event carries target.id; support older accessors as fallback
        const id: string = ev?.target?.id;
        if (!id) return;
        // Handle overflow node clicks: expand spawn
        if (id.startsWith("overflow-") || id.startsWith("of-in-") || id.startsWith("of-out-")) {
          const spawnIdStr = id
            .replace(/^overflow-(?:src-|tool-)?/, "")
            .replace("of-in-", "")
            .replace("of-out-", "");
          const spawnId = parseInt(spawnIdStr);
          if (!isNaN(spawnId)) {
            this.toggleSpawnExpansion(spawnId);
            return;
          }
        }
        const nodeDef = graphData.nodes.find((n) => n.id === id);
        if (!nodeDef) return;
        const clickedType: "spawn" | "symlink" | "artifact" =
          nodeDef.type === "spawn" ? ((nodeDef as any).isSymlinkAction ? "symlink" : "spawn") : "artifact";
        const clickedId = nodeDef.nodeId;
        const prev = this.state.selectedItem;
        const isSame = !!prev && prev.type === clickedType && prev.id === clickedId;
        // Just update the detail panel selection; visual states handled by behavior
        this.setState({
          selectedItem: isSame ? null : { type: clickedType, id: clickedId },
          detailsExpand: {
            inputs: false,
            tools: false,
            outputs: false,
            command: false,
            env: false,
            platform: false,
          },
        });
      });
      // Clear details when clicking empty canvas (deselect)
      graph.on(CanvasEvent.CLICK, () => {
        // Clear visual states and selection
        const allNodes = graph.getNodeData();
        const allEdges = graph.getEdgeData();
        const clearStates: Record<ID, string[]> = {} as any;
        allNodes.forEach((node) => (clearStates[node.id as ID] = []));
        allEdges.forEach((edge) => (clearStates[(edge.id || `${edge.source}->${edge.target}`) as ID] = []));
        graph.setElementState(clearStates, false);
        this.setState({
          selectedItem: null,
          detailsExpand: {
            inputs: false,
            tools: false,
            outputs: false,
            command: false,
            env: false,
            platform: false,
          },
        });
      });

      this.g6Graph = graph;
      // Apply persistent focus once after initial render
      graph.once(GraphEvent.AFTER_RENDER, () => {
        try {
          // Keep fit but zoom in for better readability
          const g: any = graph as any;
          g.fitView?.();
          const z = g.getZoom?.();
          if (typeof z === "number") {
            const center = g.getCanvasCenter?.();
            g.zoomTo?.(Math.min(z * 1.35, 2.0), false, center);
          }
        } catch {}
        if (this.isComponentMounted) this.applySelectionFocus();
      });
      this.setState({ chartLoading: false });
    } catch (error) {
      console.error("Failed to render G6 chart:", error);
      this.setState({ chartLoading: false, error: `G6 rendering failed: ${error}` });
    }
    return;
  }

  private updateG6GraphData() {
    // Recreate graph to ensure layout and positions are recomputed consistently
    if (!this.g6Graph || !this.isComponentMounted) return;

    const graphData = this.buildGraphData(this.state.selectedLabel!, this.state.indexedData!);
    if (graphData.nodes.length === 0) {
      this.setState({ chartLoading: false });
      return;
    }

    try {
      if (this.g6SelectionFocusTimeout) {
        clearTimeout(this.g6SelectionFocusTimeout);
        this.g6SelectionFocusTimeout = null;
      }
      this.g6Graph.destroy();
      this.g6Graph = null;
      this.setState({ chartLoading: true });
      // Re-enter renderChart to rebuild models & positions
      setTimeout(() => {
        if (this.isComponentMounted) this.renderChart();
      }, 0);
    } catch (error) {
      console.error("Failed to update G6 graph data:", error);
      this.setState({ chartLoading: false, error: `G6 update failed: ${error}` });
    }
  }

  private applySelectionFocus() {
    if (!this.isComponentMounted || !this.g6Graph) return;
    const graph = this.g6Graph;
    const sel = this.state.selectedItem;
    const nodeData = graph.getNodeData();
    const edgeData = graph.getEdgeData();
    if (nodeData.length === 0) return;

    // Clear all states first
    const clearNodeStates: Record<ID, string[]> = {} as any;
    const clearEdgeStates: Record<ID, string[]> = {} as any;
    for (const n of nodeData) clearNodeStates[n.id as ID] = [];
    for (const e of edgeData) clearEdgeStates[(e.id || `${e.source}->${e.target}`) as ID] = [];
    graph.setElementState(clearNodeStates, false);
    graph.setElementState(clearEdgeStates, false);

    // If nothing selected, set all active
    if (!sel) {
      const activeNodeStates: Record<ID, string[]> = {} as any;
      const activeEdgeStates: Record<ID, string[]> = {} as any;
      for (const n of nodeData) activeNodeStates[n.id as ID] = ["active"];
      for (const e of edgeData) activeEdgeStates[(e.id || `${e.source}->${e.target}`) as ID] = ["active"];
      graph.setElementState(activeNodeStates, true);
      graph.setElementState(activeEdgeStates, true);
      return;
    }

    // Find selected element id
    let targetId: string | null = null;
    for (const n of nodeData) {
      const d = (n?.data || {}) as any;
      if (!d || typeof d.nodeId !== "number") continue;
      if (sel.type === "spawn" && d.type === "spawn" && d.nodeId === sel.id && !d.isSymlinkAction) {
        targetId = n.id as string;
        break;
      }
      if (sel.type === "symlink" && d.type === "spawn" && d.isSymlinkAction && d.nodeId === sel.id) {
        targetId = n.id as string;
        break;
      }
      if (sel.type === "artifact" && d.type === "artifact" && d.nodeId === sel.id) {
        targetId = n.id as string;
        break;
      }
    }
    if (!targetId) return;

    // Highlight neighbors
    const neighbors: Set<string> = new Set();
    for (const e of edgeData) {
      if (e.source === targetId) neighbors.add(e.target as string);
      if (e.target === targetId) neighbors.add(e.source as string);
    }
    const nodeStates: Record<ID, string[]> = {} as any;
    nodeStates[targetId] = ["active"];
    for (const nid of neighbors) nodeStates[nid] = ["neighborActive"];
    for (const n of nodeData) if (!nodeStates[n.id as ID]) nodeStates[n.id as ID] = ["inactive"];
    graph.setElementState(nodeStates, true);
  }

  // Sort labels putting //-prefixed labels before @-prefixed labels; others after.
  private compareLabels(a: string, b: string): number {
    const bucket = (s: string) => (s.startsWith("//") ? 0 : s.startsWith("@") ? 1 : 2);
    const ba = bucket(a);
    const bb = bucket(b);
    if (ba !== bb) return ba - bb;
    return a.localeCompare(b);
  }

  private getArtifactTypeIcon(type?: ArtifactType): JSX.Element {
    switch (type) {
      case "file":
        return <FileIcon className="icon" />;
      case "directory":
        return <FolderIcon className="icon" />;
      case "symlink":
        return <LinkIcon className="icon" />;
      case "runfiles":
        return <PackageIcon className="icon" />;
      default:
        return <Info className="icon" />;
    }
  }

  private measureTextWidth(text: string, font: string): number {
    try {
      if (!this.textMeasureCanvas) {
        this.textMeasureCanvas = document.createElement("canvas");
      }
      const ctx = this.textMeasureCanvas.getContext("2d");
      if (!ctx) return text.length * 8; // fallback approximation
      ctx.font = font;
      const metrics = ctx.measureText(text);
      return metrics.width;
    } catch {
      // Fallback approximation
      return text.length * 8;
    }
  }
}
