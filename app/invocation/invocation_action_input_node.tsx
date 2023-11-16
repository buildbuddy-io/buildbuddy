import React from "react";
import { build } from "../../proto/remote_execution_ts_proto";
import { Download, FolderMinus, FolderPlus } from "lucide-react";
import DigestComponent from "../components/digest/digest";
import format from "../format/format";

interface Props {
  node: InputNode;
  treeShaToExpanded: Map<string, boolean>;
  treeShaToChildrenMap: Map<string, InputNode[]>;
  treeShaToTotalSizeMap: Map<string, [Number, Number]>;
  handleFileClicked: any;
}

interface State {}

export interface InputNode {
  obj: build.bazel.remote.execution.v2.FileNode | build.bazel.remote.execution.v2.DirectoryNode;
  type: "file" | "dir";
}

function getChildCountText(childCount: Number) {
  if (childCount === 0) {
    return "empty";
  } else if (childCount === 1) {
    return "1 child";
  }
  return childCount + " children";
}

export default class InputNodeComponent extends React.Component<Props, State> {
  render() {
    const digestString = this.props.node.obj.digest?.hash + "/" + this.props.node.obj.digest?.sizeBytes;
    const sizeInfo = this.props.treeShaToTotalSizeMap.get(digestString);
    const expanded = this.props.treeShaToExpanded.get(digestString);
    return (
      <div className="input-tree-node">
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
          {sizeInfo ? (
            <span className="input-tree-node-size">{`${format.bytes(+sizeInfo[0])} (${getChildCountText(
              sizeInfo[1]
            )})`}</span>
          ) : (
            ""
          )}
          {this.props.node.obj?.digest && <DigestComponent digest={this.props.node.obj.digest} />}
        </div>
        {expanded && (
          <div className="input-tree-node-children">
            {this.props.treeShaToChildrenMap.get(digestString)?.map((child: any) => (
              <InputNodeComponent
                node={child}
                treeShaToExpanded={this.props.treeShaToExpanded}
                treeShaToChildrenMap={this.props.treeShaToChildrenMap}
                treeShaToTotalSizeMap={this.props.treeShaToTotalSizeMap}
                handleFileClicked={this.props.handleFileClicked}
              />
            ))}
          </div>
        )}
      </div>
    );
  }
}
