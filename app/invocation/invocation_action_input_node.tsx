import React from "react";
import { build } from "../../proto/remote_execution_ts_proto";
import { Download, FolderMinus, FolderPlus } from "lucide-react";
import DigestComponent from "../components/digest/digest";

interface Props {
  node: InputNode;
  treeShaToExpanded: Map<string, boolean>;
  treeShaToChildrenMap: Map<string, InputNode[]>;
  handleFileClicked: any;
}

interface State {}

export interface InputNode {
  obj: build.bazel.remote.execution.v2.IFileNode | build.bazel.remote.execution.v2.IDirectoryNode;
  type: "file" | "dir";
}

export default class InputNodeComponent extends React.Component<Props, State> {
  render() {
    const expanded = this.props.treeShaToExpanded.get(
      this.props.node.obj.digest.hash + "/" + this.props.node.obj.digest.sizeBytes
    );
    return (
      <div className={`input-tree-node`}>
        <div
          className={`input-tree-node-name ${expanded ? "input-tree-node-expanded" : ""}`}
          onClick={() => this.props.handleFileClicked(this.props.node)}>
          <span>
            {this.props.node.type == "file" ? (
              <Download className="icon file-icon" />
            ) : (
              <>
                {expanded ? (
                  <FolderMinus className="icon file-icon folder-icon" />
                ) : (
                  <FolderPlus className="icon file-icon folder-icon" />
                )}
              </>
            )}
          </span>{" "}
          <span className="input-tree-node-label">{this.props.node.obj.name}</span>
          <DigestComponent digest={this.props.node.obj.digest} />
        </div>
        {expanded && (
          <div className="input-tree-node-children">
            {this.props.treeShaToChildrenMap
              .get(this.props.node.obj.digest.hash + "/" + this.props.node.obj.digest.sizeBytes)
              .map((child: any) => (
                <InputNodeComponent
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
