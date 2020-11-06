import DiffMatchPatch from "diff-match-patch";
import React from "react";
import { OutlinedButton } from "../components/button/button";

export type DiffChunkProps = {
  change: DiffMatchPatch.Diff;
  defaultExpanded: boolean;
};

type DiffChunkState = {
  expanded: boolean;
};

const MIN_NUM_HIDDEN_LINES = 12;
// Number of lines to show before and after a long segment of identical lines.
const NUM_LINES_OF_CONTEXT = 4;

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
      change: [op, data],
    } = this.props;
    const text = data.replace(/&/g, "&amp;").replace(/</g, "&lt;").replace(/>/g, "&gt;");

    switch (op) {
      case +1:
        return <ins className="added">{text}</ins>;
      case -1:
        return (
          <del className="removed">
            <span>{text}</span>
          </del>
        );
      case 0:
        if (!this.state.expanded) {
          const lines = text.trimEnd().split("\n");
          if (lines.length > NUM_LINES_OF_CONTEXT * 2 + MIN_NUM_HIDDEN_LINES) {
            const contextBefore = [];
            for (let i = 0; i < NUM_LINES_OF_CONTEXT; i++) {
              contextBefore.push(
                <>
                  {lines[i]}
                  <br />
                </>
              );
            }
            const contextAfter = [];
            for (let i = lines.length - NUM_LINES_OF_CONTEXT; i < lines.length - 1; i++) {
              contextAfter.push(
                <>
                  {lines[i]}
                  <br />
                </>
              );
            }
            contextAfter.push(lines[lines.length - 1]);
            if (text.endsWith("\n")) {
              contextAfter.push(<br />);
            }

            return (
              <>
                {contextBefore}
                <OutlinedButton className="diff-line collapsed" onClick={this.onExpand.bind(this)}>
                  <div className="plus-minus-cell">
                    <img className="maximize-icon" src="/image/maximize-2.svg" />
                  </div>
                  <pre>Show {lines.length - NUM_LINES_OF_CONTEXT * 2} identical lines</pre>
                </OutlinedButton>
                {contextAfter}
              </>
            );
          }
        }

        return <>{text}</>;
    }
  }
}
