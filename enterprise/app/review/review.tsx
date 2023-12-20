import Long from "long";
import React, { ReactNode } from "react";
import { User } from "../../../app/auth/user";
import format from "../../../app/format/format";
import rpc_service from "../../../app/service/rpc_service";
import { github } from "../../../proto/github_ts_proto";

interface CodeReviewComponentProps {
  user?: User;
}
interface State {
  response?: github.GetGithubPullRequestResponse;
}

export default class CodeReviewComponent extends React.Component<CodeReviewComponentProps, State> {
  state: State = {};

  componentWillMount() {
    rpc_service.service.getGithubPullRequest({}).then((r) => this.setState({ response: r }));
  }

  render() {
    let needsAttention: github.PullRequest[] = [];
    for (let p of this.state.response?.incoming || []) {
      if (!incomingNeedsAttention(p)) continue;
      needsAttention.push(p);
    }
    for (let p of this.state.response?.outgoing || []) {
      if (!outgoingNeedsAttention(p)) continue;
      needsAttention.push(p);
    }

    console.log(this.state.response);
    return (
      <div className="reviews">
        <div className="reviews-section">
          <div className="pr-list">
            <div className="reviews-title">
              Needs attention{" "}
              <span className="change-count">
                {needsAttention.length == 1 ? "1 Change" : `${needsAttention.length} Changes`}
              </span>
            </div>
            {this.prHeader()}
            {needsAttention.map((pr) => (
              <PR pr={pr} />
            ))}
            {needsAttention.length % 2 != 0 && <div className="pr-spacer"></div>}
            <div className="reviews-title">
              Incoming reviews{" "}
              <span className="change-count">
                {this.state.response?.incoming.length == 1
                  ? "1 Change"
                  : `${this.state.response?.incoming.length} Changes`}
              </span>
            </div>
            {this.prHeader()}
            {this.state.response?.incoming.map((pr) => (
              <PR pr={pr} />
            ))}
            {this.state.response && this.state.response.incoming.length % 2 != 0 && <div className="pr-spacer"></div>}
            <div className="reviews-title">
              Outgoing reviews{" "}
              <span className="change-count">
                {this.state.response?.outgoing.length == 1
                  ? "1 Change"
                  : `${this.state.response?.outgoing.length} Changes`}
              </span>
            </div>
            {this.prHeader()}
            {this.state.response?.outgoing.map((pr) => (
              <PR pr={pr} />
            ))}
          </div>
        </div>
      </div>
    );
  }

  prHeader() {
    return (
      <div className="pr pr-header">
        <div>Change</div>
        <div>Author</div>
        <div>Status</div>
        <div>Last action</div>
        <div>Reviewers</div>
        <div>Size</div>
        <div>Description</div>
      </div>
    );
  }
}

function incomingNeedsAttention(pr: github.PullRequest) {
  let latestCurrentUserReview = new Long(0);
  let latestNonCurrentUserReview = new Long(0);
  for (let [author, review] of Object.entries(pr.reviews)) {
    if (review.isCurrentUser && review.requested) {
      return true;
    }

    if (review.isCurrentUser) {
      latestCurrentUserReview = review.submittedAtUsec;
    } else if (+review.submittedAtUsec > +latestNonCurrentUserReview) {
      latestNonCurrentUserReview = review.submittedAtUsec;
    }
  }
  return latestNonCurrentUserReview > latestCurrentUserReview;
}

function outgoingNeedsAttention(pr: github.PullRequest) {
  let latestCurrentUserReview = new Long(0);
  let latestNonCurrentUserReview = new Long(0);
  for (let [author, review] of Object.entries(pr.reviews)) {
    if (review.isCurrentUser) {
      latestCurrentUserReview = review.submittedAtUsec;
    } else if (+review.submittedAtUsec > +latestNonCurrentUserReview) {
      latestNonCurrentUserReview = review.submittedAtUsec;
    }
  }
  return latestNonCurrentUserReview > latestCurrentUserReview;
}

function size(pr: github.PullRequest) {
  let linesChanged = +pr.additions + +pr.deletions;
  if (linesChanged < 10) {
    return "XS";
  }
  if (linesChanged < 50) {
    return "S";
  }
  if (linesChanged < 100) {
    return "M";
  }
  if (linesChanged < 500) {
    return "L";
  }
  return "XL";
}

interface PRProps {
  pr: github.PullRequest;
}

class PR extends React.Component<PRProps> {
  render() {
    let reviewers: React.ReactNode[] = [];
    let status = "pending";
    for (let [author, review] of Object.entries(this.props.pr.reviews).sort((a, b) => a[0].localeCompare(b[0]))) {
      if (author == this.props.pr.author) continue;
      reviewers.push(
        <span className={`pr-review pr-review-${review.status} ${review.requested ? "pr-review-requested" : ""}`}>
          {author}
        </span>
      );
      if (review.status == "approved") {
        status = review.status;
      }
    }

    return (
      <a className="pr" href={this.props.pr.url} target="_blank">
        <div>{this.props.pr.number}</div>
        <div>{this.props.pr.author}</div>
        <div>
          <div className={`pr-status pr-status-${status}`}>{status}</div>
        </div>
        <div>{format.formatDateFromUsec(this.props.pr.updatedAtUsec)}</div>
        <div>{reviewers.reduce((prev, curr) => [prev, ", ", curr])}</div>
        <div>{size(this.props.pr)}</div>
        <div>{this.props.pr.title}</div>
      </a>
    );
  }
}
