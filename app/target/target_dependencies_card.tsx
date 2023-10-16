import React from "react";
import { execution_graph } from "../../proto/execution_graph_ts_proto";
import ExecutionGraphModel from "../invocation/execution_graph_model";
import { Split } from "lucide-react";

interface Props {
  target: string;
  graph: ExecutionGraphModel;
}

export default class TargetDependenciesCard extends React.Component<Props> {
  render() {
    console.log("ELLO");
    console.log(this.props.graph);
    const subGraph = this.props.graph.subgraphForTarget(this.props.target);
    const targetNodes = this.props.graph.getNodesForTarget(this.props.target);
    const foundTargets = targetNodes.length > 0;
    const leafDeps = subGraph.getLeafTargets();
    console.log(subGraph);
    console.log(leafDeps);

    return (
      <>
        <div className="card">
          <Split className="icon" />
          <div className="content">
            <div className="title">Actions</div>
            {!foundTargets && <div>Not found</div>}
            {foundTargets &&
              targetNodes.map((n) => {
                return <div>{n.description}</div>;
              })}
          </div>
        </div>
        <div className="card">
          <Split className="icon" />
          <div className="content">
            <div className="title">Dependencies rebuilt</div>
            {!foundTargets && <div>Not found</div>}
            {foundTargets &&
              leafDeps.map((n) => {
                return <div>{n}</div>;
              })}
          </div>
        </div>
      </>
    );
  }
}
