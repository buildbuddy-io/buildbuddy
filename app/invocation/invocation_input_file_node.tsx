import React from "react";

class InputFileProps {
  node: any;
  treeShaToExpanded: Map<string, boolean>;
  treeShaToChildrenMap: Map<string, any[]>;
  handleFileClicked: any;
}

export default class InputFileComponent extends React.Component {
  props: InputFileProps;

  render() {
    const expanded = this.props.treeShaToExpanded.get(this.props.node.digest.hash + "/" + this.props.node.digest.size);
    return (
      <div className={`code-sidebar-node`}>
        <div
          className={`code-sidebar-node-name ${expanded ? "code-sidebar-node-expanded" : ""} ${
            this.props.node.type == "tree" ? "code-sidebar-folder" : "code-sidebar-file"
          }`}
          onClick={() => this.props.handleFileClicked(this.props.node)}>
          {this.props.node.name}
        </div>
        {expanded && (
          <div className="code-sidebar-node-children">
            {this.props.treeShaToChildrenMap.get(this.props.node.digest.hash + "/" + this.props.node.digest.size).map((child: any) => (
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