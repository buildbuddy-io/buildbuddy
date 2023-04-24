import React from "react";

interface Props {
  tab: string;
}

export default class HistoryTabsComponent extends React.Component<Props> {
  render() {
    return (
      <div className="tabs">
        <a href="#" className={`tab ${this.props.tab == "" && "selected"}`}>
          BUILDS
        </a>
        <a href="#users" className={`tab ${this.props.tab == "#users" && "selected"}`}>
          USERS
        </a>
        <a href="#hosts" className={`tab ${this.props.tab == "#hosts" && "selected"}`}>
          HOSTS
        </a>
        <a href="#repos" className={`tab ${this.props.tab == "#repos" && "selected"}`}>
          REPOS
        </a>
        <a href="#branches" className={`tab ${this.props.tab == "#branches" && "selected"}`}>
          BRANCHES
        </a>
        <a href="#commits" className={`tab ${this.props.tab == "#commits" && "selected"}`}>
          COMMITS
        </a>
      </div>
    );
  }
}
