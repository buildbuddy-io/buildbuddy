import React from "react";
import rpcService from "../../../app/service/rpc_service";
import { search } from "../../../proto/search_ts_proto";
import Spinner from "../../../app/components/spinner/spinner";
import { File } from "lucide-react";
import TextInput from "../../../app/components/input/input";
import FilledButton from "../../../app/components/button/button";

interface State {
  lastQuery: string;
  loading: boolean;
  response?: search.SearchResponse;
}

interface Props {
  path: string;
}

export default class CodeSearchComponent extends React.Component<Props, State> {
  searchBoxRef = React.createRef<HTMLInputElement>();

  state: State = {
    lastQuery: "",
    loading: false,
  };

  search() {
    if (!this.searchBoxRef.current?.value) {
      return;
    }
    const query = this.searchBoxRef.current.value;
    this.setState({ loading: true, response: undefined, lastQuery: query });

    // XXX: Request...
    setTimeout(() => {
      this.setState({
        loading: false,
        response: new search.SearchResponse({
          results: [
            new search.Result({
              filename: "//a/b/c.txt",
              matchCount: 2,
              repo: "buildbuddy-io/buildbuddy",
              snippets: [
                new search.Snippet({ lines: "aadjfhsd asdfhjs dsfj\nahsdffdhs shdfs hds\n" }),
                new search.Snippet({ lines: "\tqqq asdfhjs dsfj\n\tahsdffdhs shdfs hds\n" }),
              ],
            }),
            new search.Result({
              filename: "//a/b/ddddd.txt",
              matchCount: 6,
              repo: "buildbuddy-io/buildbuddy",
              snippets: [new search.Snippet({ lines: "aadjfbbbbb\nahsdffdhbbbbbbs hds\n" })],
            }),
          ],
        }),
      });
    }, 1000);
  }

  componentDidMount() {
    return;
  }

  renderLine(line: string) {
    const splitOnQuery = line.split(this.state.lastQuery);
    let out: JSX.Element[] = [];
    for (let i = 0; i < splitOnQuery.length; i++) {
      out.push(<span>{splitOnQuery[i]}</span>);
      if (i < splitOnQuery.length - 1) {
        out.push(<span className="highlight">{this.state.lastQuery}</span>);
      }
    }

    return <pre className="code-line">{out}</pre>;
  }

  renderSnippet(snippet: search.Snippet) {
    const lines = snippet.lines.split("\n");
    console.log("ello");
    return <div className="snippet">{lines.map((line) => this.renderLine(line))}</div>;
  }

  renderResult(result: search.Result) {
    return (
      <div className="result">
        <div className="result-title-bar">
          <File size={16}></File>
          <div className="repo-name">[{result.repo}]</div>
          <div className="filename">{result.filename}</div>
        </div>
        {result.snippets.map((snippet) => {
          return this.renderSnippet(snippet);
        })}
      </div>
    );
  }

  renderTheRestOfTheOwl() {
    if (this.state.loading) {
      return undefined;
    }
    if (!this.state.response) {
      return undefined;
    }
    if (this.state.response.results.length === 0) {
      return <div>NO RESULTS.</div>;
    }
    return <div>{this.state.response.results.map((result) => this.renderResult(result))}</div>;
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
              <TextInput className="search-input" ref={this.searchBoxRef} defaultValue="" autoFocus={true}></TextInput>
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
