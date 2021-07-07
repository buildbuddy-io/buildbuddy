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
    return (
      <div className={`code-sidebar-node`}>
        <div
          className={`code-sidebar-node-name ${expanded ? "code-sidebar-node-expanded" : ""} ${
            this.props.node.type == "tree" ? "code-sidebar-folder" : "code-sidebar-file"
          }`}
          onClick={() => this.props.handleFileClicked(this.props.node, this.props.fullPath)}>
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
