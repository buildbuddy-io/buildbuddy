import React from "react";

import router from "../router/router";

interface Props {
  hash: string;
  search: URLSearchParams;
}

export default class InvocationFilterComponent extends React.Component {
  props: Props;

  handleFilterChange(event: any) {
    let value = encodeURIComponent(event.target.value);
    let params = this.props.hash == "#artifacts" ? { artifactFilter: value } : { targetFilter: value };
    router.updateParams(params);
  }

  render() {
    return (
      <div className="filter">
        <img src="/image/filter.svg" />
        <input
          value={this.props.search.get(this.props.hash == "#artifacts" ? "artifactFilter" : "targetFilter") || ""}
          className="filter-input"
          placeholder="Filter..."
          onChange={this.handleFilterChange.bind(this)}
        />
      </div>
    );
  }
}
