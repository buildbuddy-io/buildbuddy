import React from "react";
import rpcService from "../../../app/service/rpc_service";
import errorService from "../../../app/errors/error_service";
import { search } from "../../../proto/search_ts_proto";
import Spinner from "../../../app/components/spinner/spinner";
import { FilterInput } from "../../../app/components/filter_input/filter_input";
import TextInput from "../../../app/components/input/input";
import FilledButton from "../../../app/components/button/button";
import router from "../../../app/router/router";
import ResultComponent from "./result";
import { Bird, Search } from "lucide-react";

interface State {
  loading: boolean;
  response?: search.SearchResponse;
}

interface Props {
  path: string;
  search: URLSearchParams;
}

export default class CodeSearchComponent extends React.Component<Props, State> {
  query: string = "";
  state: State = {
    loading: false,
  };

  search() {
    router.setQueryParam("q", this.query);
    if (!this.query) {
      return;
    }
    const query = this.query;

    this.setState({ loading: true, response: undefined });
    rpcService.service
      .search(new search.SearchRequest({ query: new search.Query({ term: query }) }))
      .then((response) => {
        this.setState({ response: response });
      })
      .catch((e) => errorService.handleError(e))
      .finally(() => this.setState({ loading: false }));
  }

  handleQueryChange(event: React.ChangeEvent<HTMLInputElement>) {
    this.query = event.target.value;
  }

  componentDidMount() {
    this.query = this.props.search.get("q") ?? "";
    this.search();
  }

  renderTheRestOfTheOwl() {
    if (this.state.loading) {
      return undefined;
    }

    // Show some helpful examples if the query was empty.
    if (!this.query) {
      return (
        <div className="no-results">
          <div className="circle">
            <Search className="icon gray" />
            <h2>Empty Query</h2>
            <p>
              To see results, try entering a query. Here are some examples:
              <ul>
                <li>
                  <code className="inline-code">case:yes Hello World</code>
                </li>
                <li>
                  <code className="inline-code">lang:css padding-(left|right)</code>
                </li>
                <li>
                  <code className="inline-code">lang:go flag.String</code>
                </li>
              </ul>
            </p>
          </div>
        </div>
      );
    }

    if (!this.state.response) {
      return undefined;
    }

    if (this.state.response.results.length === 0) {
      return (
        <div className="no-results">
          <div className="circle">
            <Bird className="icon gray" />
            <h2>No results found</h2>
          </div>
        </div>
      );
    }

    const parsedQuery = this.state.response.parsedQuery?.parsedQuery ?? "";
    const highlight = new RegExp(parsedQuery, "igmd");
    return (
      <div>
        {this.state.response.results.map((result) => (
          <ResultComponent result={result} highlight={highlight}></ResultComponent>
        ))}
      </div>
    );
  }

  render() {
    return (
      <div className="code-search">
        <div className="shelf">
          <div className="title-bar">
            <div className="cs-title">Code search</div>
            <form
              className="form-bs"
              onSubmit={(e) => {
                e.preventDefault();
                this.search();
              }}>
              <input
                type="text"
                className="searchbox"
                defaultValue={this.props.search.get("q") ?? ""}
                onChange={this.handleQueryChange.bind(this)}
              />
              <FilledButton type="submit">SEARCH</FilledButton>
            </form>
          </div>
        </div>
        <div>
          {this.state.loading && (
            <div className="spinner-center">
              <Spinner></Spinner>
            </div>
          )}
          {!this.state.loading && this.renderTheRestOfTheOwl()}
        </div>
      </div>
    );
  }
}
