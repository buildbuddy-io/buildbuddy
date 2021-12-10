import { ChevronDown, ChevronRight, File } from "lucide-react";
import React from "react";

class SidebarNodeProps {
  fullPath: string;
  node: any;
  treeShaToExpanded: Map<string, boolean>;
  treeShaToChildrenMap: Map<string, any[]>;
  handleFileClicked: any;
}

export default class SidebarNodeComponent extends React.Component {
  props: SidebarNodeProps;

  render() {
    const expanded = this.props.treeShaToExpanded.get(this.props.node.sha);
    let fileIcon = expanded ? <ChevronDown className="icon" /> : <ChevronRight className="icon" />;
    if (this.props.node.type != "tree") {
      fileIcon = <File className="icon" />;
    }

    return (
      <div className={`code-sidebar-node`}>
        <div
          className="code-sidebar-node-name"
          onClick={() => this.props.handleFileClicked(this.props.node, this.props.fullPath)}>
          {fileIcon}
          {this.props.node.path}
        </div>
        {expanded && (
          <div className="code-sidebar-node-children">
            {this.props.treeShaToChildrenMap.get(this.props.node.sha).map((child: any) => (
              <SidebarNodeComponent
                node={child}
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
