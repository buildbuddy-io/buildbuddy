import React from "react";
import { build } from "../../proto/remote_execution_ts_proto";
import { ArrowRight, Download, FileSymlink, FolderMinus, FolderPlus } from "lucide-react";
import DigestComponent from "../components/digest/digest";
import format from "../format/format";

interface Props {
  node: TreeNode;
  treeShaToExpanded: Map<string, boolean>;
  treeShaToChildrenMap: Map<string, TreeNode[]>;
  treeShaToTotalSizeMap: Map<string, [Number, Number]>;
  handleFileClicked: any;
}

interface State {}

type FileNode = build.bazel.remote.execution.v2.FileNode;
type DirectoryNode = build.bazel.remote.execution.v2.DirectoryNode;
type SymlinkNode = build.bazel.remote.execution.v2.SymlinkNode;

export interface TreeNode {
  obj: FileNode | DirectoryNode | SymlinkNode;
  type: "file" | "dir" | "symlink";
}

function getChildCountText(childCount: Number) {
  if (childCount === 0) {
    return "empty";
  } else if (childCount === 1) {
    return "1 child";
  }
  return format.formatWithCommas(childCount) + " children";
}

export default class TreeNodeComponent extends React.Component<Props, State> {
  renderFileOrDirectoryNode(node: FileNode | DirectoryNode) {
    const digestString = node.digest?.hash ?? "";
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
          <span className="input-tree-node-label">{node.name}</span>
          {sizeInfo ? (
            <span className="input-tree-node-size">{`${format.bytes(+sizeInfo[0])} (${getChildCountText(
              sizeInfo[1]
            )})`}</span>
          ) : (
            ""
          )}
          {node.digest && <DigestComponent digest={node.digest} />}
        </div>
        {expanded && (
          <div className="input-tree-node-children">
            {this.props.treeShaToChildrenMap.get(digestString)?.map((child: TreeNode) => (
              <TreeNodeComponent
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

  renderSymlinkNode(node: SymlinkNode) {
    return (
      <div className="input-tree-node">
        <div className="input-tree-node-name">
          <span>
            <FileSymlink className="icon symlink-icon" />
          </span>{" "}
          <span className="input-tree-node-label">{node.name}</span>{" "}
          <span>
            <ArrowRight className="icon symlink-arrow-icon" />
          </span>{" "}
          <span className="input-tree-node-label">{node.target}</span>
        </div>
      </div>
    );
  }

  render() {
    return "digest" in this.props.node.obj
      ? this.renderFileOrDirectoryNode(this.props.node.obj)
      : this.renderSymlinkNode(this.props.node.obj);
  }
}
