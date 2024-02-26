import { ChevronDown, ChevronRight, File } from "lucide-react";
import React from "react";
import { github } from "../../../proto/github_ts_proto";

interface SidebarNodeProps {
  fullPath: string;
  node: github.TreeNode;
  fullPathToExpanded: Map<string, boolean>;
  fullPathToRenaming: Map<string, boolean>;
  treeShaToChildrenMap: Map<string, any[]>;
  changes: Map<string, string | null>;
  getFiles: (originalFiles: github.TreeNode[], parent: string) => github.TreeNode[];
  handleFileClicked: (node: github.TreeNode, path: string) => void;
  handleContextMenu: (node: github.TreeNode, path: string, event: React.MouseEvent) => void;
  handleRename: (node: github.TreeNode, path: string, newValue: string, alreadyExisted: boolean) => void;
  depth?: number;
}

interface SidebarNodeState {
  renameValue: string;
}

export default class SidebarNodeComponent extends React.Component<SidebarNodeProps, SidebarNodeState> {
  state: SidebarNodeState = { renameValue: "" };
  render() {
    const depth = this.props.depth || 0;
    const expanded = this.props.fullPathToExpanded.get(this.props.fullPath);
    const editing = this.props.fullPathToRenaming.get(this.props.fullPath) || false;
    let fileIcon = expanded ? <ChevronDown className="icon" /> : <ChevronRight className="icon" />;
    if (this.props.node.type != "tree") {
      fileIcon = <File className="icon" />;
    }
    let change = this.props.changes.get(this.props.fullPath);
    if (change === null) {
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
            className={`code-sidebar-node-name${change === null ? " deleted" : ""}${
              this.props.node.sha && change !== null && change !== undefined ? " changed" : ""
            }${this.props.node.sha ? "" : " added"}`}>
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
              .getFiles(this.props.treeShaToChildrenMap.get(this.props.node.sha) || [], this.props.fullPath)
              .sort(compareNodes)
              .map((child: any) => (
                <SidebarNodeComponent
                  node={child}
                  depth={depth + 1}
                  getFiles={this.props.getFiles}
                  changes={this.props.changes}
                  fullPathToExpanded={this.props.fullPathToExpanded}
                  fullPathToRenaming={this.props.fullPathToRenaming}
                  treeShaToChildrenMap={this.props.treeShaToChildrenMap}
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
      this.props.fullPathToRenaming.get(this.props.fullPath) || false
    );
  }
}

export function compareNodes(a: any, b: any) {
  // Sort 'tree' type nodes after 'file' type nodes.
  const typeDiff = -a.type.localeCompare(b.type);
  if (typeDiff !== 0) return typeDiff;

  return a.path.localeCompare(b.path);
}
