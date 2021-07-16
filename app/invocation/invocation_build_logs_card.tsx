import React from "react";
import InvocationModel from "./invocation_model";
import { TerminalComponent } from "../terminal/terminal";

interface Props {
  model: InvocationModel;
  expanded: boolean;
  dark: boolean;
}

export default class BuildLogsCardComponent extends React.Component {
  props: Props;

  render() {
    return (
      <div className={`card ${this.props.dark ? "dark" : "light-terminal"} ${this.props.expanded ? "expanded" : ""}`}>
        <img className="icon" src={this.props.dark ? "/image/log-circle-light.svg" : "/image/log-circle.svg"} />
        <div className="content">
          <div className="title">Build logs </div>
          <div className="details">
            <TerminalComponent value={this.props.model.consoleBuffer} />
          </div>
        </div>
      </div>
    );
  }
}
