import { ChevronDown, ChevronRight, File } from "lucide-react";
import React from "react";

class SidebarNodeProps {
  fullPath: string;
  node: any;
  treeShaToExpanded: Map<string, boolean>;
  treeShaToChildrenMap: Map<string, any[]>;
  handleFileClicked: any;
  depth?: number;
}

export default class SidebarNodeComponent extends React.Component {
  props: SidebarNodeProps;

  render() {
    const depth = this.props.depth || 0;
    const expanded = this.props.treeShaToExpanded.get(this.props.node.sha);
    let fileIcon = expanded ? <ChevronDown className="icon" /> : <ChevronRight className="icon" />;
    if (this.props.node.type != "tree") {
      fileIcon = <File className="icon" />;
    }
    return (
      <div className={`code-sidebar-node depth-${depth}`} style={{ "--depth": depth } as any}>
        <div
          className="code-sidebar-node-row"
          onClick={() => this.props.handleFileClicked(this.props.node, this.props.fullPath)}
          title={this.props.node.path}>
          {fileIcon}
          <div className="code-sidebar-node-name">{this.props.node.path}</div>
        </div>
        {expanded && (
          <div className="code-sidebar-node-children">
            {this.props.treeShaToChildrenMap
              .get(this.props.node.sha)
              .sort(compareNodes)
              .map((child: any) => (
                <SidebarNodeComponent
                  node={child}
                  depth={depth + 1}
                  treeShaToExpanded={this.props.treeShaToExpanded}
                  treeShaToChildrenMap={this.props.treeShaToChildrenMap}
                  handleFileClicked={this.props.handleFileClicked}
                  fullPath={this.props.fullPath + "/" + child.path}
                />
              ))}
          </div>
        )}
      </div>
    );
  }
}

export function compareNodes(a: any, b: any) {
  // Sort 'tree' type nodes after 'file' type nodes.
  const typeDiff = -a.type.localeCompare(b.type);
  if (typeDiff !== 0) return typeDiff;

  return a.path.localeCompare(b.path);
}
