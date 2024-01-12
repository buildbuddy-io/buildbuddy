import Long from "long";
import React, { ReactNode } from "react";
import format from "../../../app/format/format";
import rpc_service from "../../../app/service/rpc_service";
import { github } from "../../../proto/github_ts_proto";
import { Github, MessageCircle } from "lucide-react";
import error_service from "../../../app/errors/error_service";

interface ViewPullRequestComponentProps {
  owner: string;
  repo: string;
  pull: number;
  path: string;
}

interface State {
  response?: github.GetGithubPullRequestDetailsResponse;
  displayedDiffs: string[];
}

interface DiffLineInfo {
  startLine: number;
  lineCount: number;
}

type SourceLine = string | undefined;

interface DiffLinePair {
  left: SourceLine;
  right: SourceLine;
}

interface Hunk {
  header: string;
  lines: DiffLinePair[];
}

export default class ViewPullRequestComponent extends React.Component<ViewPullRequestComponentProps, State> {
  state: State = {
    displayedDiffs: [],
  };

  componentWillMount() {
    document.title = `Change #${this.props.pull} in ${this.props.owner}/${this.props.repo} | BuildBuddy`;
    rpc_service.service
      .getGithubPullRequestDetails({
        owner: this.props.owner,
        repo: this.props.repo,
        pull: Long.fromInt(this.props.pull),
      })
      .then((r) => {
        console.log(r);
        this.setState({ response: r });
      })
      .catch((e) => error_service.handleError(e));
  }

  renderSingleReviewer(reviewer: github.Reviewer) {
    return (
      <span className={"reviewer " + (reviewer.attention ? "strong " : "") + (reviewer.approved ? "approved" : "")}>
        {reviewer.login}
      </span>
    );
  }

  renderReviewers(reviewers: github.Reviewer[]) {
    reviewers.sort((a, b) => (a.login.toLowerCase() < b.login.toLowerCase() ? -1 : 1));
    return (
      <>
        {this.joinReactNodes(
          reviewers.map((r) => this.renderSingleReviewer(r)),
          ", "
        )}
      </>
    );
  }

  joinReactNodes(nodes: React.ReactNode[], joiner: React.ReactNode): React.ReactNode[] {
    const joined: React.ReactNode[] = [];
    for (let i = 0; i < nodes.length; i++) {
      joined.push(nodes[i]);
      // If the next element exists, append the joiner node.
      if (i + 1 < nodes.length) {
        joined.push(joiner);
      }
    }
    return joined;
  }

  renderFileHeader() {
    return (
      <tr className="file-list-header">
        <td></td>
        <td className="diff-file-name">File</td>
        <td>Comments</td>
        <td>Inline</td>
        <td>Delta</td>
        <td></td>
      </tr>
    );
  }

  renderDiffBar(additions: number, deletions: number, max: number) {
    // TODO(jdhollen): render cute little green/red diff stats.
    return "";
  }

  renderDiffHunk(hunk: Hunk) {
    return (
      <>
        <pre className="diff-header">{hunk.header}</pre>
        {hunk.lines.map((v) => {
          let leftClasses =
            "source-line left" + (v.right === undefined ? " new" : "") + (v.left === undefined ? " empty" : "");
          let rightClasses =
            "source-line right" + (v.left === undefined ? " new" : "") + (v.right === undefined ? " empty" : "");
          if (v.left !== undefined && v.right !== undefined && v.left !== v.right) {
            leftClasses += " modified";
            rightClasses += " modified";
          }
          return (
            <div className="source-line-pair">
              <pre className={leftClasses}>{v.left ?? ""}</pre>
              <pre className={rightClasses}>{v.right ?? ""}</pre>
            </div>
          );
        })}
      </>
    );
  }

  getDiffLines(patch: string): JSX.Element[] {
    const out: JSX.Element[] = [];

    const patchLines = patch.split("\n");
    let currentIndex = 0;
    while (currentIndex < patchLines.length) {
      let hunk: Hunk;
      if (patchLines[currentIndex].startsWith("@@")) {
        [hunk, currentIndex] = readNextHunk(patchLines, currentIndex);
        out.push(this.renderDiffHunk(hunk));
      }
    }

    return out;
  }

  renderFileDiffs(patch: string) {
    const out = this.getDiffLines(patch);

    return (
      <tr className="file-list-diff">
        <td colSpan={6}>{out}</td>
      </tr>
    );
  }

  handleDiffClicked(name: string) {
    const newValue = [...this.state.displayedDiffs];
    const index = newValue.indexOf(name);
    if (index === -1) {
      newValue.push(name);
    } else {
      newValue.splice(index, 1);
    }
    this.setState({ displayedDiffs: newValue });
  }

  renderFileRow(file: github.FileSummary) {
    return (
      <>
        <tr className="file-list-row" onClick={this.handleDiffClicked.bind(this, file.name)}>
          <td className="viewed">
            <input type="checkbox"></input>
          </td>
          <td className="diff-file-name">{file.name}</td>
          <td>{file.comments}</td>
          <td>Diff</td>
          <td>{+file.additions + +file.deletions}</td>
          <td>{this.renderDiffBar(+file.additions, +file.deletions, 0)}</td>
        </tr>
        {this.state.displayedDiffs.indexOf(file.name) !== -1 && this.renderFileDiffs(file.patch)}
      </>
    );
  }

  renderAnalysisResults(statuses: github.ActionStatus[]) {
    const done = statuses
      .filter((v) => v.status === "success" || v.status === "failure")
      .sort((a, b) => (a.status === b.status ? 0 : a.status === "failure" ? -1 : 1))
      .map((v) => (
        <a href={v.url} target="_blank" className={"action-status " + v.status}>
          {v.name}
        </a>
      ));
    const pending = statuses
      .filter((v) => v.status !== "success" && v.status !== "failure")
      .map((v) => (
        <a href={v.url} target="_blank" className={"action-status " + v.status}>
          {v.name}
        </a>
      ));

    return (
      <>
        {pending.length > 0 && <div>Pending: {pending}</div>}
        {done.length > 0 && <div>Done: {done}</div>}
      </>
    );
  }

  getPrStatusClass(r?: github.GetGithubPullRequestDetailsResponse) {
    if (!r) {
      return "pending";
    }
    if (r.submitted) {
      return "submitted";
    } else if (r.mergeable) {
      return "approved";
    } else {
      return "pending";
    }
  }

  getPrStatusString(r: github.GetGithubPullRequestDetailsResponse) {
    if (r.submitted) {
      return "Merged";
    } else if (r.mergeable) {
      return "Ready to merge";
    } else {
      return "Pending";
    }
  }

  render() {
    return (
      <div className={"pr-view " + this.getPrStatusClass(this.state.response)}>
        {this.state.response !== undefined && (
          <>
            <div className="summary-section">
              <div className="review-header">
                <MessageCircle size="36" className="icon" />
                <span className="review-title">
                  <span className="review-number">Change #{this.state.response.pull}&nbsp;</span>
                  <span className="review-author">
                    by {this.state.response.author} in {this.state.response.owner}/{this.state.response.repo}
                  </span>
                  <a href={this.state.response.githubUrl} className="review-gh-link">
                    <Github size="16" className="icon" />
                  </a>
                </span>
              </div>
              <div className="header-separator"></div>
              <div className="review-cell">
                <div className="attr-grid">
                  <div>Reviewers</div>
                  <div>{this.renderReviewers(this.state.response.reviewers)}</div>
                  <div>Issues</div>
                  <div></div>
                  <div>Mentions</div>
                  <div></div>
                </div>
              </div>
              <div className="review-cell">
                <div className="description">
                  {this.state.response.title}
                  <br />
                  <br />
                  {this.state.response.body}
                </div>
              </div>
              <div className="review-cell">
                <div className="attr-grid">
                  <div>Created</div>
                  <div>{format.formatTimestampUsec(this.state.response.createdAtUsec)}</div>
                  <div>Modified</div>
                  <div>{format.formatTimestampUsec(this.state.response.updatedAtUsec)}</div>
                  <div>Branch</div>
                  <div>{this.state.response.branch}</div>
                </div>
              </div>
              <div className="review-cell">
                <div className="attr-grid">
                  <div>Status</div>
                  <div>{this.getPrStatusString(this.state.response)}</div>
                  <div>Analysis</div>
                  <div>{this.renderAnalysisResults(this.state.response.actionStatuses)}</div>
                </div>
              </div>
              <div className="review-cell blue">Files</div>
              <div className="review-cell blue"></div>
            </div>
            <div className="file-section">
              <table>
                {this.renderFileHeader()}
                {this.state.response.files.map((f) => this.renderFileRow(f))}
              </table>
            </div>
          </>
        )}
      </div>
    );
  }
}

function getDiffLineInfo(input: string): DiffLineInfo {
  const breakdown = input.slice(1).split(",");
  return { startLine: Number(breakdown[0]) || 0, lineCount: Number(breakdown[1]) || 0 };
}

function readNextHunk(patchLines: string[], startIndex: number): [Hunk, number] {
  // Parse the hunk start line.
  const hunkSummary = patchLines[startIndex].split("@@");
  if (hunkSummary.length < 3) {
    return [{ header: "", lines: [] }, startIndex];
  }
  const diffLineInfo = hunkSummary[1].trim().split(" ");
  let leftInfo: DiffLineInfo | undefined, rightInfo: DiffLineInfo | undefined;
  for (let i = 0; i < diffLineInfo.length; i++) {
    const value = diffLineInfo[i];
    if (value.length < 4) {
      continue;
    }
    if (value[0] === "+") {
      rightInfo = getDiffLineInfo(value);
    } else if (value[0] === "-") {
      leftInfo = getDiffLineInfo(value);
    }
  }

  let leftLinesRead = 0;
  let rightLinesRead = 0;
  let currentLineOffset = 0;
  let currentIndex = startIndex + 1;

  let leftLines: SourceLine[] = [];
  let rightLines: SourceLine[] = [];
  while (
    leftLinesRead < (leftInfo?.lineCount ?? 0) &&
    rightLinesRead < (rightInfo?.lineCount ?? 0) &&
    currentIndex < patchLines.length
  ) {
    let line = patchLines[currentIndex];
    if (line[0] === "+") {
      rightLinesRead += 1;
      currentLineOffset += 1;
      rightLines.push(line.slice(1));
    } else if (line[0] === "-") {
      leftLinesRead += 1;
      currentLineOffset -= 1;
      leftLines.push(line.slice(1));
    } else {
      leftLinesRead += 1;
      rightLinesRead += 1;
      rightLines.push(line.slice(1));
      leftLines.push(line.slice(1));
      const arrayToGrow = currentLineOffset < 0 ? rightLines : leftLines;
      for (let i = 0; i < Math.abs(currentLineOffset); i++) {
        arrayToGrow.push(undefined);
      }
      currentLineOffset = 0;
    }
    currentIndex++;
  }
  const finalOffset = rightLines.length - leftLines.length;
  if (finalOffset !== 0) {
    const arrayToGrow = finalOffset < 0 ? rightLines : leftLines;
    for (let i = 0; i < Math.abs(finalOffset); i++) {
      arrayToGrow.push(undefined);
    }
  }

  let output: DiffLinePair[] = [];
  for (let i = rightLines.length - 1; i >= 0; i--) {
    if (leftLines[i] === undefined) {
      let j = i - 1;
      while (j >= 0) {
        if (leftLines[j] !== undefined) {
          if (leftLines[j] === rightLines[i]) {
            leftLines[i] = leftLines[j];
            leftLines[j] = undefined;
          }
          break;
        }
        j--;
      }
    }
  }
  for (let i = 0; i < rightLines.length; i++) {
    output.push({ left: leftLines[i], right: rightLines[i] });
  }

  return [{ header: patchLines[startIndex], lines: output }, currentIndex];
}
