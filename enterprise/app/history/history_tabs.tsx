import React from "react";

interface Props {
  hash: string;
}

export default class HistoryTabsComponent extends React.Component<Props> {
  render() {
    return (
      <div className="tabs">
        <a href="#" className={`tab ${this.props.hash == "" && "selected"}`}>
          BUILDS
        </a>
        <a href="#users" className={`tab ${this.props.hash == "#users" && "selected"}`}>
          USERS
        </a>
        <a href="#hosts" className={`tab ${this.props.hash == "#hosts" && "selected"}`}>
          HOSTS
        </a>
        <a href="#repos" className={`tab ${this.props.hash == "#repos" && "selected"}`}>
          REPOS
        </a>
        <a href="#branches" className={`tab ${this.props.hash == "#branches" && "selected"}`}>
          BRANCHES
        </a>
        <a href="#commits" className={`tab ${this.props.hash == "#commits" && "selected"}`}>
          COMMITS
        </a>
      </div>
    );
  }
}
