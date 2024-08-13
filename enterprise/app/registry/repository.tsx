import React from "react";
import { registry } from "../../../proto/registry_ts_proto";
import { ChevronsUpDown, File } from "lucide-react";
import { OutlinedButton } from "../../../app/components/button/button";

interface RepositoryProps {
    repository: registry.Repository;
}
  
interface RepositoryState {
}

export default class RepositoryComponent extends React.Component<RepositoryProps, RepositoryState> {
  render() {
    return (
        <div>
        <div className="registry-repo">Repository: {this.props.repository.name}</div>
        <div className="registry-tag">{this.props.repository.tags.map((tag) => <TagComponent tag={tag}></TagComponent>)}</div>
        </div>
    )
  }
}

interface TagProps {
    tag: string;
  }
  
  class TagComponent extends React.Component<TagProps> {
    render() {
      return <div className="tagName">{this.props.tag}</div>;
    }
  }
  