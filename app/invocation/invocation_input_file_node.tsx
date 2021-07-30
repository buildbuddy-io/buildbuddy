import React from "react";
import { build } from "../../proto/remote_execution_ts_proto";

interface Props {
  node: any;
  treeShaToExpanded: Map<string, boolean>;
  treeShaToChildrenMap: Map<string, any[]>;
  handleFileClicked: any;
}

interface State {}

export default class InputFileComponent extends React.Component<Props, State> {
  render() {
    const expanded = this.props.treeShaToExpanded.get(
      this.props.node.digest.hash + "/" + this.props.node.digest.sizeBytes
    );
    return (
      <div className={`code-sidebar-node`}>
        <div
          className={`code-sidebar-node-name ${expanded ? "code-sidebar-node-expanded" : ""} ${
            this.props.node.hasOwnProperty("isExecutable") ? "code-sidebar-file": "code-sidebar-folder"
          }`}
          onClick={() => this.props.handleFileClicked(this.props.node)}>
          {this.props.node.name}
        </div>
        {expanded && (
          <div className="code-sidebar-node-children">
            {this.props.treeShaToChildrenMap
              .get(this.props.node.digest.hash + "/" + this.props.node.digest.sizeBytes)
              .map((child: any) => (
                <InputFileComponent
                  node={child}
                  treeShaToExpanded={this.props.treeShaToExpanded}
                  treeShaToChildrenMap={this.props.treeShaToChildrenMap}
                  handleFileClicked={this.props.handleFileClicked}
                />
              ))}
          </div>
        )}
      </div>
    );
  }
}
