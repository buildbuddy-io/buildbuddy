import React from "react";
import JsDiff from "diff";
import { OutlinedButton } from "../components/button/button";

export type DiffChunkProps = {
  change: JsDiff.Change;
  defaultExpanded: boolean;
};

type DiffChunkState = {
  expanded: boolean;
};

export default class DiffChunk extends React.Component<DiffChunkProps, DiffChunkState> {
  state = { expanded: this.props.defaultExpanded };

  componentDidUpdate(prevProps: DiffChunkProps) {
    if (prevProps.defaultExpanded !== this.props.defaultExpanded) {
      this.setState({ expanded: this.props.defaultExpanded });
    }
  }

  private onExpand() {
    this.setState({ expanded: true });
  }

  render() {
    const {
      change: { value, added, removed },
    } = this.props;

    const isUnchanged = !added && !removed;

    const lines = value.trimEnd().split("\n");

    const nodes = lines.map((line: string, index: number) => (
      <div key={`${index}`} className={`diff-line ${added ? "added" : ""} ${removed ? "removed" : ""}`}>
        <div className="plus-minus-cell">
          {added && "+"}
          {removed && "-"}
        </div>
        <div className="diff-line-content">{line}</div>
      </div>
    ));

    if (nodes.length >= 10 && isUnchanged && !this.state.expanded) {
      return [
        nodes[0],
        <OutlinedButton className="diff-line collapsed" onClick={this.onExpand.bind(this)}>
          <div className="plus-minus-cell">
            <img className="maximize-icon" src="/image/maximize-2.svg" />
          </div>
          <pre>Show {nodes.length - 2} identical lines</pre>
        </OutlinedButton>,
        nodes[nodes.length - 1],
      ];
    }

    return nodes;
  }
}
