import React from "react";
import { search } from "../../../proto/search_ts_proto";
import { File } from "lucide-react";

interface SnippetProps {
  highlight: RegExp;
  snippet: search.Snippet;
}

class SnippetComponent extends React.Component<SnippetProps> {
  renderLine(line: string) {
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

    return <pre className="code-line">{out}</pre>;
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
  render() {
    return (
      <div className="result">
        <div className="result-title-bar">
          <File size={16}></File>
          <div className="repo-name">[{this.props.result.repo}]</div>
          <div className="filename">{this.props.result.filename}</div>
        </div>
        {this.props.result.snippets.map((snippet) => {
          return <SnippetComponent snippet={snippet} highlight={this.props.highlight}></SnippetComponent>;
        })}
      </div>
    );
  }
}
