import { ArrowRight, Download, FileSymlink, FolderMinus, FolderPlus } from "lucide-react";
import React from "react";
import { build } from "../../proto/remote_execution_ts_proto";
import DigestComponent from "../components/digest/digest";
import { TreeNode } from "../invocation/invocation_action_tree_node";
import InvocationModel from "../invocation/invocation_model";
import { findNodeByName, nodesEqual } from "./tree_utils";

interface Props {
  nodes: TreeNode[];
  otherNodes: TreeNode[];
  side: "left" | "right";
  treeA?: build.bazel.remote.execution.v2.Directory;
  treeB?: build.bazel.remote.execution.v2.Directory;
  treeShaToExpanded: Map<string, boolean>;
  treeShaToChildrenMap: Map<string, TreeNode[]>;
  handleDirectoryClicked: (node: TreeNode) => void;
  showChangesOnly: boolean;
  actionDetails?: ActionDetails;
  otherActionDetails?: ActionDetails;
  indent?: number;
}

interface ActionDetails {
  invocationId: string;
  invocationModel?: InvocationModel;
  action?: build.bazel.remote.execution.v2.Action;
}

export default class DiffTreeNodeComponent extends React.Component<Props> {
  private getNodeStyle(status: string, type: string): React.CSSProperties {
    const style: React.CSSProperties = {};

    let weight = type == "file" ? "600" : "normal";

    switch (status) {
      case "added":
        style.backgroundColor = `rgba(34, 197, 94, ${type == "file" ? "0.2" : "0.1"})`;
        style.color = "#16a34a";
        style.fontWeight = weight;
        break;
      case "removed":
        style.backgroundColor = `rgba(239, 68, 68, ${type == "file" ? "0.2" : "0.1"})`;
        style.color = "#dc2626";
        style.fontWeight = weight;
        break;
      case "modified":
        style.backgroundColor = `rgba(250, 204, 21, ${type == "file" ? "0.5" : "0.1"})`;
        style.color = "#a16207";
        style.fontWeight = weight;
        break;
    }

    return style;
  }

  private getNodeStatus(node: TreeNode, otherNode?: TreeNode): "added" | "removed" | "modified" | "unchanged" {
    if (!otherNode) {
      return this.props.side === "right" ? "added" : "removed";
    }

    if (!nodesEqual(node, otherNode)) {
      return "modified";
    }

    return "unchanged";
  }

  private handleFileClick = (node: TreeNode, otherNode?: TreeNode) => {
    if (node.type === "file") {
      const fileNode = node.obj as build.bazel.remote.execution.v2.FileNode;
      if (
        fileNode.digest &&
        this.props.actionDetails?.invocationModel &&
        this.props.otherActionDetails?.invocationModel
      ) {
        let params: Record<string, string> = {
          bytestream_url: this.props.actionDetails.invocationModel.getBytestreamURL(fileNode.digest),
          invocation_id: this.props.actionDetails.invocationModel.getInvocationId(),
          filename: fileNode.name,
        };
        const otherFileNode = otherNode?.obj as build.bazel.remote.execution.v2.FileNode;
        if (otherFileNode && otherFileNode.digest) {
          params.compare_bytestream_url = this.props.otherActionDetails.invocationModel.getBytestreamURL(
            otherFileNode.digest
          );
          params.compare_invocation_id = this.props.otherActionDetails.invocationModel.getInvocationId();
          params.compare_filename = otherFileNode.name;
        }
        let url =
          `/code/buildbuddy-io/buildbuddy/?${new URLSearchParams(params).toString()}` + (otherNode ? "#diff" : "");
        window.open(url, "_blank");
      }
    } else if (node.type === "dir") {
      this.props.handleDirectoryClicked(node);
    }
  };

  private renderNode(node: TreeNode, otherNode?: TreeNode): React.ReactNode {
    const status = this.getNodeStatus(node, otherNode);

    let className = "";
    switch (status) {
      case "added":
        className = "tree-diff-added";
        break;
      case "removed":
        className = "tree-diff-removed";
        break;
      case "modified":
        className = "tree-diff-modified";
        break;
    }

    if (node.type === "symlink") {
      const symlink = node.obj as build.bazel.remote.execution.v2.SymlinkNode;

      const symlinkStyle = this.getNodeStyle(status, node.type);

      return (
        <div key={node.obj.name} className={className}>
          <div className="tree-node-symlink" style={symlinkStyle}>
            <span>
              <FileSymlink className="icon symlink-icon" />
            </span>{" "}
            <span className="input-tree-node-label">{symlink.name}</span>{" "}
            <span>
              <ArrowRight className="icon symlink-arrow-icon" />
            </span>{" "}
            <span className="input-tree-node-label">{symlink.target}</span>
          </div>
        </div>
      );
    }

    const isDir = node.type === "dir";
    const fileOrDirNode = node.obj as
      | build.bazel.remote.execution.v2.FileNode
      | build.bazel.remote.execution.v2.DirectoryNode;
    const digestString = fileOrDirNode.digest?.hash || "";
    const expanded = isDir ? this.props.treeShaToExpanded.get(digestString) : false;

    return (
      <div key={node.obj.name} className={className}>
        <div className="input-tree-node diff-tree-node">
          <div
            className={`input-tree-node-name ${expanded ? "input-tree-node-expanded" : ""} ${!isDir ? "clickable-file" : ""}`}
            style={this.getNodeStyle(status, node.type)}
            onClick={() => this.handleFileClick(node, otherNode)}>
            <span>
              {!isDir ? (
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
            <span className="input-tree-node-label">{fileOrDirNode.name}</span>
            {fileOrDirNode.digest && <DigestComponent digest={fileOrDirNode.digest} />}
          </div>
          {expanded && isDir && this.renderChildren(node)}
        </div>
      </div>
    );
  }

  private renderChildren(parentNode: TreeNode): React.ReactNode {
    const dirNode = parentNode.obj as build.bazel.remote.execution.v2.DirectoryNode;
    const digestString = dirNode.digest?.hash || "";
    const children = this.props.treeShaToChildrenMap.get(digestString) || [];

    const otherNode = findNodeByName(this.props.otherNodes, dirNode.name || "");
    let otherChildren: TreeNode[] = [];

    if (otherNode && otherNode.type === "dir") {
      const otherDirNode = otherNode.obj as build.bazel.remote.execution.v2.DirectoryNode;
      const otherDigestString = otherDirNode.digest?.hash || "";
      otherChildren = this.props.treeShaToChildrenMap.get(otherDigestString) || [];
    }

    if (children.length === 0 && otherChildren.length === 0) {
      return null;
    }

    return (
      <div className="input-tree-node-children">
        <DiffTreeNodeComponent
          nodes={children}
          otherNodes={otherChildren}
          side={this.props.side}
          treeA={this.props.treeA}
          treeB={this.props.treeB}
          treeShaToExpanded={this.props.treeShaToExpanded}
          treeShaToChildrenMap={this.props.treeShaToChildrenMap}
          handleDirectoryClicked={this.props.handleDirectoryClicked}
          showChangesOnly={this.props.showChangesOnly}
          actionDetails={this.props.actionDetails}
          otherActionDetails={this.props.otherActionDetails}
        />
      </div>
    );
  }

  render() {
    const { nodes, otherNodes, showChangesOnly } = this.props;

    // Get all unique names from both sides
    const allNames = new Set<string>();
    nodes.forEach((node) => allNames.add(node.obj.name || ""));
    otherNodes.forEach((node) => allNames.add(node.obj.name || ""));
    const sortedNames = Array.from(allNames).sort();

    const elements: React.ReactNode[] = [];

    sortedNames.forEach((name) => {
      const node = findNodeByName(nodes, name);
      const otherNode = findNodeByName(otherNodes, name);

      // Show placeholder for missing nodes to maintain alignment
      if (!node) {
        elements.push(
          <div key={name} className="missing-node" style={{ paddingLeft: `${this.props.indent || 0}px` }}>
            —
          </div>
        );
        return;
      }

      const isDifferent = !nodesEqual(node, otherNode);
      if (showChangesOnly && !isDifferent) {
        return;
      }

      elements.push(this.renderNode(node, otherNode));
    });

    return <>{elements}</>;
  }
}
