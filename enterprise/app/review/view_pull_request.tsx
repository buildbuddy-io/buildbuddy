import Long from "long";
import React, { ReactNode } from "react";
import format from "../../../app/format/format";
import rpc_service from "../../../app/service/rpc_service";
import { github } from "../../../proto/github_ts_proto";
import { Github, MessageCircle } from "lucide-react";
import error_service from "../../../app/errors/error_service";
import moment from "moment";

interface ViewPullRequestComponentProps {
  owner: string;
  repo: string;
  pull: number;
  path: string;
}

interface commentDraft {
  text: string;
  path?: string;
  commitSha?: string;
  side?: string;
  line?: number;
  commentId?: string;
  threadId?: string;
  defaultResolved?: boolean;
}

interface State {
  response?: github.GetGithubPullRequestDetailsResponse;
  displayedDiffs: string[];
  inProgressComment?: commentDraft;
  pendingRequest: boolean;
}

interface DiffLineInfo {
  startLine: number;
  lineCount: number;
}

interface SourceLine {
  source?: string;
  lineNumber?: number;
}

interface DiffLinePair {
  left: SourceLine;
  right: SourceLine;
}

interface Hunk {
  header: string;
  lines: DiffLinePair[];
}

export default class ViewPullRequestComponent extends React.Component<ViewPullRequestComponentProps, State> {
  commentTextRef: React.RefObject<HTMLTextAreaElement> = React.createRef();
  commentResolvedRef: React.RefObject<HTMLInputElement> = React.createRef();
  state: State = {
    displayedDiffs: [],
    pendingRequest: false,
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

  findComment(threadId: string): github.Comment | undefined {
    return this.state.response?.comments.find((c) => c.threadId === threadId);
  }

  findPathFromThreadId(threadId: string): string {
    return this.findComment(threadId)?.path || "";
  }

  findPositionFromThreadId(threadId: string): github.CommentPosition | undefined {
    return this.findComment(threadId)?.position ?? undefined;
  }

  submitComment(c: commentDraft | undefined, submitAsBot: boolean, resolved: boolean) {
    if (this.state.pendingRequest) {
      return;
    }
    if (c === undefined) {
      return;
    }
    const commentText = this.commentTextRef.current?.value ?? c.text;
    const req = new github.PostGithubPullRequestCommentRequest({
      owner: this.props.owner,
      repo: this.props.repo,
      pull: Long.fromInt(this.props.pull),
      path: c.path,
      body: commentText,
      commitSha: c.commitSha,
      submitAsBot: submitAsBot,
      threadId: c.threadId,
      pullId: this.state.response?.pullId,
      reviewId: this.state.response?.draftReviewId,
      commentId: c.commentId,
      // XXX: Set review id, set to pending..
      markAsResolved: resolved,
    });
    if (c.commentId && c.commentId != "") {
      req.commentId = c.commentId;
    } else if (c.threadId) {
      req.threadId = c.threadId;
    } else if (c.line && c.side) {
      req.line = Long.fromNumber(c.line);
      req.side = c.side;
    } else {
      return;
    }
    console.log(req);
    this.setState({ pendingRequest: true });
    rpc_service.service
      .postGithubPullRequestComment(req)
      .then((r) => {
        console.log(r);
        error_service.handleError("posted!");
        const comment = new github.Comment({
          id: r.commentId,
          body: req.body,
          path: req.path || this.findPathFromThreadId(req.threadId),
          commitSha: req.commitSha,
          position: c.line
            ? new github.CommentPosition({
                startLine: Long.fromNumber(c.line || 0),
                endLine: Long.fromNumber(c.line || 0),
                leftSide: c.side === "LEFT",
              })
            : this.findPositionFromThreadId(req.threadId),
          commenter: new github.ReviewUser({
            login: "jdhollen",
            bot: false,
          }),
          reviewId: req.reviewId,
          createdAtUsec: Long.fromNumber(Date.now() * 1000),
          threadId: req.threadId,
          isResolved: req.markAsResolved,
        });
        if (req.commentId) {
          if (this.state.response) {
            const index = this.state.response.comments.findIndex((c) => c.id === req.commentId);
            this.state.response.comments.splice(index, 1, comment);
          }
        } else {
          this.state.response?.comments.push(comment);
        }
        console.log(this.state.response);
        this.setState({ pendingRequest: false, inProgressComment: undefined });
      })
      .catch((e) => {
        error_service.handleError(e);

        this.setState({ pendingRequest: false, inProgressComment: undefined });
      });
  }

  cancelComment() {
    if (this.state.pendingRequest) {
      return;
    }
    this.setState({ inProgressComment: undefined });
  }

  deleteComment(id: string) {
    if (this.state.pendingRequest || id === "") {
      return;
    }
    const req = new github.PostGithubPullRequestCommentRequest({
      commentId: id,
      delete: true,
    });
    console.log(req);
    this.setState({ pendingRequest: true });
    rpc_service.service
      .postGithubPullRequestComment(req)
      .then((r) => {
        console.log(r);
        const comment = this.state.response?.comments.findIndex((c) => c.id === id);
        if (comment && this.state.response) {
          this.state.response.comments.splice(comment, 1);
        }
        this.setState({ pendingRequest: false, inProgressComment: undefined });
      })
      .catch((e) => {
        error_service.handleError(e);

        this.setState({ pendingRequest: false, inProgressComment: undefined });
      });
  }

  renderThread(comments: github.Comment[], forNewComment?: boolean) {
    if (comments.length < 1 && !forNewComment) {
      return <></>;
    }
    const leftSide = forNewComment
      ? this.state.inProgressComment?.side === "LEFT"
      : comments[0]?.position?.leftSide ?? false;
    const replying = forNewComment || comments.find((c) => c.threadId === this.state.inProgressComment?.threadId);
    const resolved =
      (replying && this.state.inProgressComment?.defaultResolved) || (!replying && comments[0].isResolved);
    const isBot = comments.length > 0 && !comments.find((c) => !c.commenter?.bot);
    const threadId = comments.length > 0 ? comments[0].threadId : "";
    const draftCommentIndex = comments.findIndex((c) => c.reviewId === this.state.response?.draftReviewId);
    const hasDraft = draftCommentIndex >= 0;
    if (hasDraft) {
      comments.push(comments.splice(draftCommentIndex, 1)[0]);
    }
    console.log(this.state.inProgressComment);
    return (
      <div className="thread-block">
        {!leftSide ? (
          <>
            <pre className="thread-line-number-space"> </pre>
            <div className="thread-empty-side"> </div>
          </>
        ) : undefined}
        <>
          <pre className="thread-line-number-space"> </pre>
          <div className="thread-container">
            <div className={`thread${resolved || isBot ? " resolved" : ""}`}>
              {comments.map((c) => {
                if (replying && hasDraft && c.id === comments[draftCommentIndex].id) {
                  return undefined;
                }
                return (
                  <>
                    <div className="thread-comment">
                      <div className="comment-author">
                        <div className="comment-author-text">
                          {c.commenter?.login} {c.position?.leftSide}
                        </div>
                        <div className="comment-timestamp">
                          {moment(+c.createdAtUsec / 1000).format("HH:mm, MMM DD")}
                        </div>
                      </div>
                      <div className="comment-body">{c.body}</div>
                    </div>
                    <div className="comment-divider"></div>
                  </>
                );
              })}
              {replying && (
                <div className="thread-reply">
                  <div className="thread-comment">
                    <div className="comment-author">
                      <div className="comment-author-text">{this.state.response?.currentUser || "you"}</div>
                      <div className="comment-timestamp">Draft</div>
                    </div>
                    <div className="comment-body">
                      <textarea
                        disabled={this.state.pendingRequest}
                        autoFocus
                        ref={this.commentTextRef}
                        className="comment-input"
                        defaultValue={this.state.inProgressComment?.text ?? ""}></textarea>
                    </div>
                  </div>
                </div>
              )}
              <div className="reply-bar">
                {replying && (
                  <>
                    {!forNewComment && (
                      <>
                        <input
                          id="pr-view-resolved-check"
                          className="resolved-check"
                          type="checkbox"
                          ref={this.commentResolvedRef}
                          defaultChecked={this.state.inProgressComment?.defaultResolved}></input>
                        <label className="resolved-label" htmlFor="pr-view-resolved-check">
                          {replying ? "Resolved" : "No action required"}
                        </label>
                      </>
                    )}
                    <span
                      className="reply-fake-link"
                      onClick={() =>
                        this.submitComment(
                          this.state.inProgressComment,
                          false,
                          this.commentResolvedRef.current?.checked ??
                            this.state.inProgressComment?.defaultResolved ??
                            false
                        )
                      }>
                      Save draft
                    </span>
                    <span className="reply-fake-link" onClick={() => this.cancelComment()}>
                      Cancel
                    </span>
                  </>
                )}
                {!replying && (
                  <>
                    <span className="resolution-pill-box">
                      <span className={resolved || isBot ? "resolution-pill resolved" : "resolution-pill unresolved"}>
                        {isBot ? "Automated" : hasDraft ? "Draft" : resolved ? "Resolved" : "Unresolved"}
                      </span>
                    </span>
                    {isBot && (
                      <>
                        <span
                          className="reply-fake-link"
                          onClick={() =>
                            // XXX: draft text
                            this.startReply(
                              hasDraft ? comments[draftCommentIndex].body : "",
                              hasDraft ? comments[draftCommentIndex].id : "",
                              threadId,
                              resolved
                            )
                          }>
                          {hasDraft ? "Edit" : "Reply"}
                        </span>
                        {!hasDraft && (
                          <span
                            className="reply-fake-link"
                            onClick={() => this.startPleaseFixReply("", threadId, comments[comments.length - 1].body)}>
                            Please fix
                          </span>
                        )}
                      </>
                    )}
                    {!isBot && (
                      <>
                        <span
                          className="reply-fake-link"
                          onClick={() =>
                            // XXX: draft text
                            this.startReply(
                              hasDraft ? comments[draftCommentIndex].body : "",
                              hasDraft ? comments[draftCommentIndex].id : "",
                              threadId,
                              resolved
                            )
                          }>
                          {hasDraft ? "Edit" : "Reply"}
                        </span>
                        {!hasDraft && (
                          <>
                            <span
                              className="reply-fake-link"
                              onClick={() => this.markDone(comments[comments.length - 1].id, threadId, resolved)}>
                              Done
                            </span>
                            <span
                              className="reply-fake-link"
                              onClick={() => this.ack(comments[comments.length - 1].id, threadId, resolved)}>
                              Ack
                            </span>
                          </>
                        )}
                        {hasDraft && (
                          <>
                            <span
                              className="reply-fake-link"
                              onClick={() => this.deleteComment(comments[comments.length - 1].id)}>
                              Discard
                            </span>
                          </>
                        )}
                      </>
                    )}
                  </>
                )}
              </div>
            </div>
          </div>
        </>
        {leftSide ? (
          <>
            <pre className="thread-line-number-space"> </pre>
            <div className="thread-empty-side"> </div>
          </>
        ) : undefined}
      </div>
    );
  }

  renderComments(path: string, comments: github.Comment[], leftLine: number, rightLine: number) {
    const threads: Map<String, github.Comment[]> = new Map();
    comments.forEach((c) => {
      const thread = c.threadId;
      let commentList = threads.get(thread);
      if (!commentList) {
        commentList = [];
        threads.set(thread, commentList);
      }
      commentList.push(c);
    });
    const outs: JSX.Element[] = [];
    const newComment =
      !this.state.inProgressComment?.threadId &&
      this.state.inProgressComment?.path === path &&
      ((this.state.inProgressComment?.side === "LEFT" && leftLine === this.state.inProgressComment?.line) ||
        (this.state.inProgressComment?.side === "RIGHT" && rightLine === this.state.inProgressComment?.line));
    if (newComment) {
      outs.push(
        this.renderThread([], true)
        // <>
        //   {newComment && this.state.inProgressComment?.side === "RIGHT" && (
        //     <div className="comment-area no-comment"></div>
        //   )}
        //   {newComment && (
        //     <div className="comment-area">
        //       <textarea ref={this.commentTextRef} className="comment-input"></textarea>
        //       <button onClick={() => this.submitComment(this.state.inProgressComment, false)}>SUBMIT</button>
        //       <button onClick={() => this.submitComment(this.state.inProgressComment, true)}>SUBMIT AS BOT</button>
        //       <button onClick={() => this.cancelComment()}>CANCEL</button>
        //     </div>
        //   )}
        //   {newComment && this.state.inProgressComment?.side === "LEFT" && (
        //     <div className="comment-area no-comment"></div>
        //   )}
        // </>
      );
    }
    threads.forEach((comments, threadId) => {
      outs.push(this.renderThread(comments));
    });
    return <>{outs}</>;
  }

  startReply(text: string, commentId: string, threadId: string, defaultResolved: boolean) {
    if (this.state.pendingRequest) {
      return;
    }
    this.setState({ inProgressComment: { text, commentId, defaultResolved, threadId } });
  }

  startPleaseFixReply(id: string, threadId: string, fixText: string) {
    if (this.state.pendingRequest) {
      return;
    }
    const text =
      fixText
        .split("\n")
        .map((v) => "> " + v)
        .reduce((o, v) => o + v + "\n", "") + "\nPlease fix.";
    this.setState({ inProgressComment: { text, commentId: "", defaultResolved: false, threadId } });
  }

  markDone(id: string, threadId: string, defaultResolved: boolean) {
    if (this.state.pendingRequest) {
      return;
    }
    this.submitComment({ text: "Done.", commentId: "", defaultResolved: true, threadId }, false, true);
  }

  ack(id: string, threadId: string, defaultResolved: boolean) {
    if (this.state.pendingRequest) {
      return;
    }
    this.submitComment({ text: "Acknowledged.", commentId: "", defaultResolved, threadId }, false, defaultResolved);
  }

  startComment(side: string, path: string, commitSha: string, lineNumber?: number) {
    if (this.state.pendingRequest) {
      return;
    }
    if (!lineNumber) {
      return;
    }
    this.setState({ inProgressComment: { text: "", line: lineNumber, side, path, commitSha } });
  }

  renderDiffHunk(hunk: Hunk, path: string, commitSha: string, comments: github.Comment[]) {
    return (
      <>
        <pre className="diff-header">{hunk.header}</pre>
        {hunk.lines.map((v) => {
          let leftClasses =
            "source-line left" +
            (v.right.source === undefined ? " new" : "") +
            (v.left.source === undefined ? " empty" : "");
          let rightClasses =
            "source-line right" +
            (v.left.source === undefined ? " new" : "") +
            (v.right.source === undefined ? " empty" : "");
          if (v.left.source !== undefined && v.right.source !== undefined && v.left.source !== v.right.source) {
            leftClasses += " modified";
            rightClasses += " modified";
          }
          return (
            <>
              <div className="source-line-pair">
                <pre className="source-line-number">{v.left.lineNumber ?? " "}</pre>
                <pre
                  className={leftClasses}
                  onClick={this.startComment.bind(this, "LEFT", path, commitSha, v.left.lineNumber)}>
                  {v.left.source ?? ""}
                </pre>
                <pre className="source-line-number">{v.right.lineNumber ?? " "}</pre>
                <pre
                  className={rightClasses}
                  onClick={this.startComment.bind(this, "RIGHT", path, commitSha, v.right.lineNumber)}>
                  {v.right.source ?? ""}
                </pre>
              </div>
              <div className="threads">
                {this.renderComments(
                  path,
                  comments.filter((c) => {
                    return (
                      +(c.position?.startLine || c.position?.endLine || 0) ===
                      (c.position?.leftSide ? v.left.lineNumber : v.right.lineNumber)
                    );
                  }),
                  v.left.lineNumber ?? -1,
                  v.right.lineNumber ?? -1
                )}
              </div>
            </>
          );
        })}
      </>
    );
  }

  getDiffLines(patch: string, path: string, commitSha: string, comments: github.Comment[]): JSX.Element[] {
    const out: JSX.Element[] = [];

    const patchLines = patch.split("\n");
    let currentIndex = 0;
    while (currentIndex < patchLines.length) {
      let hunk: Hunk;
      if (patchLines[currentIndex].startsWith("@@")) {
        [hunk, currentIndex] = readNextHunk(patchLines, currentIndex);
        out.push(this.renderDiffHunk(hunk, path, commitSha, comments));
      }
    }

    return out;
  }

  renderFileDiffs(patch: string, filename: string, commitSha: string) {
    // XXX: Need to check commit sha.
    const fileComments = this.state.response!.comments.filter((v) => v.path === filename);
    const out = this.getDiffLines(patch, filename, commitSha, fileComments);

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
        {this.state.displayedDiffs.indexOf(file.name) !== -1 &&
          this.renderFileDiffs(file.patch, file.name, file.commitSha)}
      </>
    );
  }

  renderAnalysisResults(statuses: github.ActionStatus[]) {
    const done = statuses
      .filter((v) => v.status === "SUCCESS" || v.status === "FAILURE")
      .sort((a, b) => (a.status === b.status ? 0 : a.status === "FAILURE" ? -1 : 1))
      .map((v) => (
        <a href={v.url} target="_blank" className={"action-status " + v.status}>
          {v.name}
        </a>
      ));
    const pending = statuses
      .filter((v) => v.status !== "SUCCESS" && v.status !== "FAILURE")
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

  approve() {
    rpc_service.service
      .approveGithubPullRequest({
        owner: this.props.owner,
        repo: this.props.repo,
        pull: Long.fromInt(this.props.pull),
      })
      .then((r) => {
        console.log(r);
        window.location.reload();
      })
      .catch((e) => error_service.handleError(e));
  }

  reply() {
    if (!this.state.response?.draftReviewId) {
      return;
    }
    rpc_service.service
      .sendGithubPullRequestReview({
        reviewId: this.state.response.draftReviewId,
      })
      .then((r) => {
        console.log(r);
        window.location.reload();
      })
      .catch((e) => error_service.handleError(e));
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
                  <div>
                    <button onClick={() => this.approve()}>APPROVE</button>
                    <button onClick={() => this.reply()}>REPLY</button>
                  </div>
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
    leftLinesRead < (leftInfo?.lineCount || 1) &&
    rightLinesRead < (rightInfo?.lineCount || 1) &&
    currentIndex < patchLines.length
  ) {
    let line = patchLines[currentIndex];
    if (line[0] === "+") {
      rightLines.push({ source: line.slice(1), lineNumber: (rightInfo?.startLine ?? 0) + rightLinesRead });
      rightLinesRead += 1;
      currentLineOffset += 1;
    } else if (line[0] === "-") {
      leftLines.push({ source: line.slice(1), lineNumber: (leftInfo?.startLine ?? 0) + leftLinesRead });
      leftLinesRead += 1;
      currentLineOffset -= 1;
    } else {
      rightLines.push({ source: line.slice(1), lineNumber: (rightInfo?.startLine ?? 0) + rightLinesRead });
      leftLines.push({ source: line.slice(1), lineNumber: (leftInfo?.startLine ?? 0) + leftLinesRead });
      leftLinesRead += 1;
      rightLinesRead += 1;
      const arrayToGrow = currentLineOffset < 0 ? rightLines : leftLines;
      for (let i = 0; i < Math.abs(currentLineOffset); i++) {
        arrayToGrow.push({});
      }
      currentLineOffset = 0;
    }
    currentIndex++;
  }
  const finalOffset = rightLines.length - leftLines.length;
  if (finalOffset !== 0) {
    const arrayToGrow = finalOffset < 0 ? rightLines : leftLines;
    for (let i = 0; i < Math.abs(finalOffset); i++) {
      arrayToGrow.push({});
    }
  }

  let output: DiffLinePair[] = [];
  for (let i = rightLines.length - 1; i >= 0; i--) {
    if (leftLines[i].source === undefined) {
      let j = i - 1;
      while (j >= 0) {
        if (leftLines[j].source !== undefined) {
          if (leftLines[j].source === rightLines[i].source) {
            leftLines[i] = leftLines[j];
            leftLines[j] = {};
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
