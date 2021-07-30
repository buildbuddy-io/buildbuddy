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
      <div className={`input-file-tree-node`}>
        <div
          className={`input-file-tree-node-name ${expanded ? "input-file-tree-node-expanded" : ""} ${
            this.props.node.hasOwnProperty("isExecutable") ? "input-file-tree-file" : "input-file-tree-folder"
          }`}
          onClick={() => this.props.handleFileClicked(this.props.node)}>
          <span>
            {this.props.node.hasOwnProperty("isExecutable") ? (
              <img className="file-icon" src="/image/download.svg" />
            ) : (
              <span>
                {expanded ? (
                  <img className="file-icon" src="/image/folder-minus.svg" />
                ) : (
                  <img className="file-icon" src="/image/folder-plus.svg" />
                )}
              </span>
            )}
          </span>{" "}
          <span>{this.props.node.name}</span>
        </div>
        {expanded && (
          <div className="input-file-tree-node-children">
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
