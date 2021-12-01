import React from "react";
import { Filter } from "lucide-react";
import router from "../router/router";

interface Props {
  hash: string;
  search: URLSearchParams;
  placeholder?: string;
}

export default class InvocationFilterComponent extends React.Component {
  props: Props;

  handleFilterChange(event: any) {
    let value = event.target.value;
    let params = {};
    switch (this.filterType()) {
      case "artifactFilter":
        params = { artifactFilter: value };
        break;
      case "targetFilter":
        params = { targetFilter: value };
        break;
      case "executionFilter":
        params = { executionFilter: value };
        break;
    }
    router.replaceParams(params);
  }

  filterType() {
    switch (this.props.hash) {
      case "#artifacts":
        return "artifactFilter";
      case "#execution":
        return "executionFilter";
      case "#targets":
        return "targetFilter";
      default:
        return "";
    }
  }

  render() {
    return (
      <div className="filter">
        <Filter className="icon" />
        <input
          value={this.props.search.get(this.filterType())}
          className="filter-input"
          placeholder={this.props.placeholder ? this.props.placeholder : "Filter..."}
          onChange={this.handleFilterChange.bind(this)}
        />
      </div>
    );
  }
}
