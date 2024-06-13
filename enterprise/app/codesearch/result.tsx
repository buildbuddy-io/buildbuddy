import React from "react";
import { search } from "../../../proto/search_ts_proto";
import { File } from "lucide-react";

interface SnippetProps {
  result: search.Result;
  highlight: RegExp;
  snippet: search.Snippet;
}

class SnippetComponent extends React.Component<SnippetProps> {
  getFileAndLineURL(lineNumber: number) {
    let ownerRepo = this.props.result.repo;
    let filename = this.props.result.filename;
    let parsedQuery = this.props.highlight.source;
    let sha = this.props.result.sha;
    return `/code/${ownerRepo}/${filename}?commit=${sha}&pq=${parsedQuery}#L${lineNumber}`;
  }

  renderLine(line: string) {
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

  render() {
    const lines = this.props.snippet.lines.split("\n");
    return <div className="snippet">{lines.map((line) => this.renderLine(line))}</div>;
  }
}

interface ResultProps {
  highlight: RegExp;
  result: search.Result;
}

export default class ResultComponent extends React.Component<ResultProps> {
  getFileOnlyURL() {
    let ownerRepo = this.props.result.repo;
    let filename = this.props.result.filename;
    let parsedQuery = this.props.highlight.source;
    let sha = this.props.result.sha;
    return `/code/${ownerRepo}/${filename}?commit=${sha}&pq=${parsedQuery}`;
  }
  render() {
    return (
      <div className="result">
        <div className="result-title-bar">
          <File size={16}></File>
          <div className="repo-name">[{this.props.result.repo}]</div>
          <div className="filename">
            <a href={this.getFileOnlyURL()}>{this.props.result.filename}</a>
          </div>
        </div>
        {this.props.result.snippets.map((snippet) => {
          return (
            <SnippetComponent
              snippet={snippet}
              highlight={this.props.highlight}
              result={this.props.result}></SnippetComponent>
          );
        })}
      </div>
    );
  }
}
