import React from "react";
import InvocationModel from "./invocation_model";
import DagreGraph from "dagre-d3-react";

interface Props {
  model: InvocationModel;
  expanded: boolean;
}

export default class QueryGraphCardComponent extends React.Component {
  props: Props;

  render() {
    if (!this.props.model.consoleBuffer.includes("digraph mygraph {")) {
      return <></>;
    }

    let width = Math.min(800, window.innerWidth - 128);
    let output = this.props.model.consoleBuffer.replace(/.*digraph.*{.*node \[.*\];\n(.*)}/gs, "$1");
    let lines = output.split("\n").map((line) => line.trim());
    let nodes = new Set<string>();
    let links = new Array<{ source: string; target: string }>();
    for (let line of lines) {
      let groups = line?.match(/\"(?<source>.*)\" -> \"(?<target>.*)\"/)?.groups;
      if (groups?.source && groups?.target) {
        links.push({ source: groups?.source, target: groups?.target });
      } else if (line && !line.startsWith("\u001b")) {
        nodes.add(line.replace(/\"/g, ""));
      }
    }

    return (
      <div className="card">
        <DagreGraph
          className="invocation-query-graph"
          nodes={Array.from(nodes).map((node) => {
            return { id: node, label: node };
          })}
          links={links}
          config={{
            rankdir: "LR",
            align: "UL",
            ranker: "tight-tree",
          }}
          width={`${width}px`}
          height={`80vh`}
          shape="rect"
          fitBoundaries
          zoomable
        />
      </div>
    );
  }
}
