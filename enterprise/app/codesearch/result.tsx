import { ChevronsUpDown, File } from "lucide-react";
import React from "react";
import { search } from "../../../proto/search_ts_proto";

interface SnippetProps {
  result: search.Result;
  highlight: RegExp;
  snippet: search.Snippet;
}

class SnippetComponent extends React.Component<SnippetProps> {
  getFileAndLineURL(lineNumber: number): string {
    let ownerRepo = this.props.result.owner + "/" + this.props.result.repo;
    let filename = this.props.result.filename;
    let parsedQuery = this.props.highlight.source;
    let sha = this.props.result.sha;
    return `/code/${ownerRepo}/${filename}?commit=${sha}&pq=${parsedQuery}#L${lineNumber}`;
  }

  renderLine(line: string): JSX.Element {
    let lineNumber = 1;
    let lineNumberMatch = line.match(/\d+:/g);
    if (lineNumberMatch) {
      lineNumber = parseInt(lineNumberMatch[0], 10);
    }
    let regionsToHighlight = [...line.matchAll(this.props.highlight)];
    let out: JSX.Element[] = [];
    let start = 0;
    while (start < line.length) {
      if (regionsToHighlight.length) {
        let region = regionsToHighlight.shift()!;
        let regionStart = region.index!;
        let regionEnd = regionStart + region[0]!.length;

        // Append anything leading up to this highlighted region.
        if (regionStart != start) {
          out.push(<span>{line.slice(start, regionStart)}</span>);
        }
        // Append this highlighted region.
        out.push(<span className="highlight">{line.slice(regionStart, regionEnd)}</span>);
        start = regionEnd;
      } else {
        // Append anything left outside of highlighted regions.
        out.push(<span>{line.slice(start, line.length)}</span>);
        start = line.length;
      }
    }

    return (
      <a href={this.getFileAndLineURL(lineNumber)}>
        <pre className="code-line">{out}</pre>
      </a>
    );
  }

  render(): React.ReactNode {
    const lines = this.props.snippet.lines.split("\n");
    return <div className="snippet">{lines.map((line) => this.renderLine(line))}</div>;
  }
}

interface ResultProps {
  highlight: RegExp;
  result: search.Result;
}

interface ResultState {
  limit: number;
}

export default class ResultComponent extends React.Component<ResultProps, ResultState> {
  state: ResultState = {
    limit: 3,
  };

  getFileOnlyURL(): string {
    let ownerRepo = this.props.result.owner + "/" + this.props.result.repo;
    let filename = this.props.result.filename;
    let parsedQuery = this.props.highlight.source;
    let sha = this.props.result.sha;
    return `/code/${ownerRepo}/${filename}?commit=${sha}&pq=${parsedQuery}`;
  }

  handleMoreClicked(): void {
    this.setState({ limit: Number.MAX_SAFE_INTEGER });
  }

  render(): React.ReactNode {
    let additionalMatchCount = this.props.result.snippets.length - this.state.limit;
    return (
      <div className="result">
        <div className="result-title-bar">
          <File size={16}></File>
          <div className="repo-name">[{this.props.result.repo}]</div>
          <div className="filename">
            <a href={this.getFileOnlyURL()}>{this.props.result.filename}</a>
          </div>
        </div>
        {this.props.result.snippets.slice(0, this.state.limit).map((snippet) => {
          return (
            <SnippetComponent
              snippet={snippet}
              highlight={this.props.highlight}
              result={this.props.result}></SnippetComponent>
          );
        })}
        {this.props.result.snippets.length > this.state.limit && (
          <div className="more-button" onClick={this.handleMoreClicked.bind(this)}>
            <ChevronsUpDown /> Show {additionalMatchCount} more match{additionalMatchCount > 1 && "es"}
          </div>
        )}
      </div>
    );
  }
}
