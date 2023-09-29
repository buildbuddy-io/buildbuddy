import React from "react";
import ExecutionGraphModel from "./execution_graph_model";

interface Props {
  graph: ExecutionGraphModel;
}

interface State {
  showLargeGraph?: boolean;
}

export default class ExecutionGraphCard extends React.Component<Props, State> {
  state: State = {};

  render() {
    console.log(this.props.graph);
    return <div>GREAT JOB</div>;
  }
}
