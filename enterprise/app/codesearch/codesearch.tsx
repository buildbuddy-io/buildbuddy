import { Bird, Search, XCircle } from "lucide-react";
import React from "react";
import FilledButton from "../../../app/components/button/button";
import Spinner from "../../../app/components/spinner/spinner";
import errorService from "../../../app/errors/error_service";
import router from "../../../app/router/router";
import rpcService from "../../../app/service/rpc_service";
import shortcuts, { KeyCombo } from "../../../app/shortcuts/shortcuts";
import { BuildBuddyError } from "../../../app/util/errors";
import { search } from "../../../proto/search_ts_proto";
import ResultComponent from "./result";

interface State {
  loading: boolean;
  response?: search.SearchResponse;
  inputText: string;
  errorMessage?: string;
}

interface Props {
  path: string;
  search: URLSearchParams;
}

export default class CodeSearchComponent extends React.Component<Props, State> {
  keyboardShortcutHandle: string = "";
  state: State = {
    loading: false,
    inputText: this.getQuery(),
  };

  hasQuery() {
    return this.props.search.has("q");
  }

  getQuery() {
    return (this.props.search.get("q") || "").trim();
  }

  search() {
    if (!this.getQuery()) {
      return;
    }

    this.setState({ loading: true, response: undefined, inputText: this.getQuery() });
    rpcService.service
      .search(new search.SearchRequest({ query: new search.Query({ term: this.getQuery() }) }))
      .then((response) => {
        this.setState({ response: response });
      })
      .catch((e) => {
        const parsedError = BuildBuddyError.parse(e);
        if (parsedError.code == "InvalidArgument") {
          this.setState({ errorMessage: String(parsedError.description) });
        } else {
          errorService.handleError(e);
        }
      })
      .finally(() => this.setState({ loading: false }));
  }

  componentDidUpdate(prevProps: Props) {
    if (this.props.search.get("q") != prevProps.search.get("q")) {
      this.search();
    }
  }

  componentDidMount() {
    this.keyboardShortcutHandle = shortcuts.register(KeyCombo.slash, () => {
      this.focusSearchBox();
    });
    this.focusSearchBox();
    this.search();
  }

  componentWillUnmount() {
    shortcuts.deregister(this.keyboardShortcutHandle);
  }

  getStat(name: string): number {
    if (!this.state.response?.performanceMetrics) {
      return 0;
    }
    let match = this.state.response.performanceMetrics.metrics.find((metric) => metric.name == name);
    return +(match?.value || 0);
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
            <Search className="big-icon gray" />
            <h2>Empty Query</h2>
            <p>To see results, try entering a query. Here are some examples:</p>
            {this.renderExamples()}
          </div>
        </div>
      );
    }

    if (this.state.errorMessage) {
      return (
        <div className="no-results">
          <div className="circle">
            <XCircle className="big-icon gray" />
            <h2>Invalid Search Query</h2>
            <p>{this.state.errorMessage}</p>
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
            <Bird className="big-icon gray" />
            <h2>No results found</h2>
          </div>
        </div>
      );
    }

    let parsedQuery = this.state.response.parsedQuery?.parsedQuery ?? "";
    const highlight = new RegExp(parsedQuery.replace(/\(\?[imsU]+\)/g, ""), "igmd");
    return (
      <div>
        <div>
          {this.state.response.results.map((result) => (
            <ResultComponent result={result} highlight={highlight}></ResultComponent>
          ))}
        </div>
        <div className="statsForNerds">
          <span>
            Found {this.getStat("TOTAL_DOCS_SCORED_COUNT")} results (
            {(this.getStat("TOTAL_SEARCH_DURATION") / 1e6).toFixed(2)}ms)
          </span>
        </div>
      </div>
    );
  }

  focusSearchBox() {
    (document.querySelector(".searchbox") as HTMLElement | undefined)?.focus();
  }

  renderExamples() {
    return (
      <div className="examples">
        <p>Try:</p>
        <ul className="examples">
          <li>
            <a href={`/search/?q=${encodeURIComponent("case:yes Hello World")}`}>
              <code className="inline-code">case:yes Hello World</code>
            </a>
          </li>
          <li>
            <a href={`/search/?q=${encodeURIComponent("lang:css padding-(left|right)")}`}>
              <code className="inline-code">lang:css padding-(left|right)</code>
            </a>
          </li>
          <li>
            <a href={`/search/?q=${encodeURIComponent("lang:go flag.String")}`}>
              <code className="inline-code">lang:go flag.String</code>
            </a>
          </li>
        </ul>
      </div>
    );
  }

  renderSearchBox() {
    return (
      <form
        className="form-bs"
        onSubmit={(e) => {
          e.preventDefault();
          router.updateParams({ q: this.state.inputText });
        }}>
        <input
          type="text"
          className="searchbox"
          value={this.state.inputText}
          onChange={(e) => {
            this.setState({ inputText: e.target.value });
          }}
        />
        <Search />
      </form>
    );
  }

  render() {
    if (!this.hasQuery()) {
      // Show nice search box, centered within the page
      return (
        <div className="code-search landing">
          <h1 className="cs-title">Code search</h1>
          {this.renderSearchBox()}
          {this.renderExamples()}
        </div>
      );
    }

    return (
      <div className="code-search">
        <div className="shelf">
          <div className="title-bar">
            <h1 className="cs-title">Code search</h1>
            {this.renderSearchBox()}
          </div>
        </div>
        <div>
          {this.state.loading && (
            <div className="spinner-center">
              <Spinner />
            </div>
          )}
          {!this.state.loading && this.renderTheRestOfTheOwl()}
        </div>
      </div>
    );
  }
}
