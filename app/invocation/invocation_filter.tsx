import React from "react";
import router from "../router/router";
import { FilterInput } from "../components/filter_input/filter_input";

interface Props {
  hash: string;
  search: URLSearchParams;
  placeholder?: string;
}

export default class InvocationFilterComponent extends React.Component<Props> {
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
      <FilterInput
        className="invocation-filter"
        value={this.props.search.get(this.filterType())}
        placeholder={this.props.placeholder ? this.props.placeholder : "Filter..."}
        onChange={this.handleFilterChange.bind(this)}
      />
    );
  }
}
