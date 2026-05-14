import { ChevronDown, ChevronRight, File } from "lucide-react";
import React from "react";
import { workspace } from "../../../proto/workspace_ts_proto";

interface SidebarNodeProps {
  fullPath: string;
  node: workspace.Node;
  isExpanded: (fullPath: string) => boolean;
  isRenaming: (fullPath: string) => boolean;
  getChildren: (fullPath: string, sha: string) => workspace.Node[];
  changes: Map<string, workspace.Node>;
  getFiles: (originalFiles: workspace.Node[], parent: string) => workspace.Node[];
  handleFileClicked: (node: workspace.Node, path: string) => void;
  handleContextMenu: (node: workspace.Node, path: string, event: React.MouseEvent) => void;
  handleRename: (node: workspace.Node, path: string, newValue: string, alreadyExisted: boolean) => void;
  depth?: number;
}

interface SidebarNodeState {
  renameValue: string;
}

export default class SidebarNodeComponent extends React.Component<SidebarNodeProps, SidebarNodeState> {
  state: SidebarNodeState = { renameValue: "" };
  render() {
    const depth = this.props.depth || 0;
    const expanded =
      this.props.node.nodeType == workspace.NodeType.DIRECTORY && this.props.isExpanded(this.props.fullPath);
    const editing = this.props.isRenaming(this.props.fullPath) || false;
    let fileIcon = expanded ? <ChevronDown className="icon" /> : <ChevronRight className="icon" />;
    if (this.props.node.nodeType == workspace.NodeType.FILE) {
      fileIcon = <File className="icon" />;
    }
    let change = this.props.changes.get(this.props.fullPath);
    if (change?.changeType === workspace.ChangeType.DELETED) {
      return null; // Don't show deleted files in the sidebar.
    }
    return (
      <div
        className={`code-sidebar-node depth-${depth}`}
        style={{ "--depth": depth } as any}
        onContextMenu={(e) => this.props.handleContextMenu(this.props.node, this.props.fullPath, e)}>
        <div
          className="code-sidebar-node-row"
          onClick={() => this.props.handleFileClicked(this.props.node, this.props.fullPath)}
          title={this.props.node.path}>
          {fileIcon}
          <div
            className={`code-sidebar-node-name${
              change?.changeType == workspace.ChangeType.MODIFIED ||
              this.props.node.changeType == workspace.ChangeType.MODIFIED
                ? " changed"
                : ""
            }${
              change?.changeType == workspace.ChangeType.ADDED ||
              this.props.node.changeType == workspace.ChangeType.ADDED
                ? " added"
                : ""
            }`}>
            {this.props.node.path && !editing ? (
              this.props.node.path
            ) : (
              <input
                defaultValue={this.props.node.path}
                autoFocus={true}
                onChange={(e) => this.setState({ renameValue: e.target.value })}
                onBlur={() => this.handleRename()}
                onKeyDown={(e) => {
                  if (e.key == "Enter") {
                    this.handleRename();
                  }
                }}
              />
            )}
          </div>
        </div>
        {expanded && (
          <div className="code-sidebar-node-children">
            {this.props
              .getFiles(this.props.getChildren(this.props.fullPath, this.props.node.sha), this.props.fullPath)
              .sort(compareNodes)
              .map((child: any) => (
                <SidebarNodeComponent
                  node={child}
                  depth={depth + 1}
                  getFiles={this.props.getFiles}
                  changes={this.props.changes}
                  isExpanded={this.props.isExpanded}
                  isRenaming={this.props.isRenaming}
                  getChildren={this.props.getChildren}
                  handleFileClicked={this.props.handleFileClicked}
                  fullPath={this.props.fullPath + "/" + child.path}
                  handleContextMenu={this.props.handleContextMenu}
                  handleRename={this.props.handleRename}
                />
              ))}
          </div>
        )}
      </div>
    );
  }

  handleRename() {
    this.props.handleRename(
      this.props.node,
      this.props.fullPath,
      this.state.renameValue || this.props.node.path,
      this.props.isRenaming(this.props.fullPath) || false
    );
  }
}

export function compareNodes(a: workspace.Node, b: workspace.Node) {
  // Sort 'tree' type nodes after 'file' type nodes.
  const typeDiff = b.nodeType - a.nodeType;
  if (typeDiff !== 0) return typeDiff;

  return a.path.localeCompare(b.path);
}
