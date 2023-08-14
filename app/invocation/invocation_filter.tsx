import React from "react";
import router from "../router/router";
import { FilterInput } from "../components/filter_input/filter_input";

interface Props {
  tab: string;
  search: URLSearchParams;
  placeholder?: string;
  debounceMillis?: number;
}

interface State {
  value: string;
}

export default class InvocationFilterComponent extends React.Component<Props, State> {
  state: State = {
    value: "",
  };

  private timeout?: number;

  componentDidMount() {
    this.setState({ value: this.props.search.get(this.filterType()) ?? "" });
  }

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
    this.setState({ value: value });

    // Wait a little while to apply the URL params to avoid triggering excessive
    // RPCs.
    window.clearTimeout(this.timeout);
    this.timeout = window.setTimeout(() => {
      router.replaceParams(params);
    }, this.props.debounceMillis || 0);
  }

  filterType() {
    switch (this.props.tab) {
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
        value={this.state.value}
        placeholder={this.props.placeholder ? this.props.placeholder : "Filter..."}
        onChange={this.handleFilterChange.bind(this)}
      />
    );
  }
}
