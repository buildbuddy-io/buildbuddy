import React from "react";
import rpcService from "../../../app/service/rpc_service";
import errorService from "../../../app/errors/error_service";
import { search } from "../../../proto/search_ts_proto";
import Spinner from "../../../app/components/spinner/spinner";
import { FilterInput } from "../../../app/components/filter_input/filter_input";
import TextInput from "../../../app/components/input/input";
import FilledButton from "../../../app/components/button/button";
import router from "../../../app/router/router";
import shortcuts, { KeyCombo } from "../../../app/shortcuts/shortcuts";
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
  inputText: string = "";
  keyboardShortcutHandle: string = "";
  state: State = {
    loading: false,
  };

  getQuery() {
    return this.props.search.get("q") || "";
  }

  search() {
    if (!this.getQuery()) {
      return;
    }

    this.setState({ loading: true, response: undefined });
    rpcService.service
      .search(new search.SearchRequest({ query: new search.Query({ term: this.getQuery() }) }))
      .then((response) => {
        this.setState({ response: response });
      })
      .catch((e) => errorService.handleError(e))
      .finally(() => this.setState({ loading: false }));
  }

  componentDidUpdate(prevProps: Props) {
    if (this.props.search.get("q") != prevProps.search.get("q")) {
      this.search();
    }
  }

  handleInputChange(event: React.ChangeEvent<HTMLInputElement>) {
    this.inputText = event.target.value;
  }

  componentDidMount() {
    this.keyboardShortcutHandle = shortcuts.register(KeyCombo.slash, () => {
      this.focusSearchBox();
    });
    this.search();
  }

  componentWillUnmount() {
    shortcuts.deregister(this.keyboardShortcutHandle);
  }

  renderTheRestOfTheOwl() {
    if (this.state.loading) {
      return undefined;
    }

    // Show some helpful examples if the query was empty.
    if (!this.getQuery()) {
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

    let parsedQuery = this.state.response.parsedQuery?.parsedQuery ?? "";
    const highlight = new RegExp(parsedQuery.replace(/\(\?[imsU]+\)/g, ""), "igmd");
    return (
      <div>
        {this.state.response.results.map((result) => (
          <ResultComponent result={result} highlight={highlight}></ResultComponent>
        ))}
      </div>
    );
  }

  focusSearchBox() {
    (document.querySelector(".searchbox") as HTMLElement | undefined)?.focus();
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
                router.updateParams({ q: this.inputText });
              }}>
              <input
                type="text"
                className="searchbox"
                defaultValue={this.props.search.get("q") ?? ""}
                onChange={this.handleInputChange.bind(this)}
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
