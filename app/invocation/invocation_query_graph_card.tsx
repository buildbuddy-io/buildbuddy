import React from "react";
import InvocationModel from "./invocation_model";
import DagreGraph from "dagre-d3-react";
import { AlertCircle, List } from "lucide-react";

interface Props {
  buildLogs: string;
}

interface State {
  showLargeGraph?: boolean;
}

interface Edge {
  source: string;
  target: string;
}

const LARGE_GRAPH_EDGE_LIMIT = 1000;

const MIN_EDGES_FOR_INSIGHTS = 10;
const MAX_TARGETS_PER_INSIGHT = 10;

export default class QueryGraphCardComponent extends React.Component<Props, State> {
  state: State = {};

  onClickShowAnyway() {
    this.setState({ showLargeGraph: true });
  }

  render() {
    if (!this.props.buildLogs.includes("digraph mygraph {")) {
      return <></>;
    }

    let width = Math.min(800, window.innerWidth - 128);
    let output = this.props.buildLogs.replace(/.*?digraph.*?{(.*)}.*/gs, "$1");
    let lines = output.split("\n").map((line) => line.trim());
    let nodes = new Set<string>();
    let edges = new Array<Edge>();
    for (let line of lines) {
      // Parse edge declaration
      let groups = line?.match(/\"(?<source>.*?)\" -> \"(?<target>.*?)\"/)?.groups;
      if (groups?.source && groups?.target) {
        edges.push({ source: groups.source, target: groups.target });
        continue;
      }

      // Parse node declaration
      groups = line?.match(/^\"(?<node>.*?)\"$/)?.groups;
      if (groups?.node) {
        nodes.add(groups.node);
        continue;
      }

      // TODO: Display edge labels, like [label="//conditions:default"];
    }

    const fullGraphHidden = edges.length > LARGE_GRAPH_EDGE_LIMIT && !this.state.showLargeGraph;

    return (
      <>
        {!fullGraphHidden && (
          <div className="card invocation-query-graph-card">
            <DagreGraph
              className="invocation-query-graph"
              nodes={Array.from(nodes).map((node) => {
                return { id: node, label: node };
              })}
              links={edges}
              config={{
                rankdir: "LR",
                align: "UL",
                ranker: "tight-tree",
              }}
              width={`${width}px`}
              height={`60vh`}
              shape="rect"
              fitBoundaries
              zoomable
            />
          </div>
        )}
        {fullGraphHidden && (
          <div className="invocation-query-graph-hidden-card card">
            <AlertCircle className="icon red" />
            <div className="content">
              <div className="title">Graph</div>
              <div className="details">
                Graph contains more than {LARGE_GRAPH_EDGE_LIMIT} edges, and may take a long time to render.{" "}
                <span className="clickable show-anyway" onClick={this.onClickShowAnyway.bind(this)}>
                  Show anyway
                </span>
              </div>
            </div>
          </div>
        )}
        <div className="card invocation-query-graph-summary-card">
          <List className="icon" />
          <div className="content">
            <div className="title">Graph summary</div>
            <div className="graph-summary">
              <div className="summary-section">
                <div className="summary-header">Size</div>
                <div className="summary-table">
                  <div className="summary-numeric-value">{nodes.size}</div>
                  <div>Targets</div>
                  <div className="summary-numeric-value">{edges.length}</div>
                  <div>Dependency links</div>
                </div>
              </div>
              {renderInsightsSections(nodes, edges)}
            </div>
          </div>
        </div>
      </>
    );
  }
}

function renderInsightsSections(nodes: Set<string>, edges: Edge[]): React.ReactNode {
  // Don't bother showing insights for tiny graphs; they are probably
  // not useful.
  if (edges.length < MIN_EDGES_FOR_INSIGHTS) return null;

  const nodeToEdgesMap = new Map<string, Edge[]>();
  for (const edge of edges) {
    let sourceEdges = nodeToEdgesMap.get(edge.source);
    if (!sourceEdges) nodeToEdgesMap.set(edge.source, (sourceEdges = []));
    sourceEdges.push(edge);

    let targetEdges = nodeToEdgesMap.get(edge.target);
    if (!targetEdges) nodeToEdgesMap.set(edge.target, (targetEdges = []));
    targetEdges.push(edge);
  }

  const sourceNodeCounts = new Map<string, number>();
  const targetNodeCounts = new Map<string, number>();
  for (const edge of edges) {
    sourceNodeCounts.set(edge.source, (sourceNodeCounts.get(edge.source) || 0) + 1);
    targetNodeCounts.set(edge.target, (targetNodeCounts.get(edge.target) || 0) + 1);
  }
  const sortedSourceNodeCounts = [...sourceNodeCounts.entries()]
    .sort((a, b) => b[1] - a[1])
    .slice(0, MAX_TARGETS_PER_INSIGHT);
  const sortedTargetNodeCounts = [...targetNodeCounts.entries()]
    .sort((a, b) => b[1] - a[1])
    .slice(0, MAX_TARGETS_PER_INSIGHT);

  return (
    <>
      <div className="summary-section">
        <div className="summary-header">Targets most often used as a direct dependency</div>
        <div className="summary-table">
          {sortedTargetNodeCounts.map(([dep, count]) => (
            <React.Fragment key={dep}>
              <div className="summary-numeric-value">{count}</div>
              <div>{dep}</div>
            </React.Fragment>
          ))}
        </div>
      </div>
      <div className="summary-section">
        <div className="summary-header">Targets with the most direct dependencies</div>
        <div className="summary-table">
          {sortedSourceNodeCounts.map(([dep, count]) => (
            <React.Fragment key={dep}>
              <div className="summary-numeric-value">{count}</div>
              <div>{dep}</div>
            </React.Fragment>
          ))}
        </div>
      </div>
    </>
  );
}
