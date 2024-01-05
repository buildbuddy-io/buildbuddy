import Long from "long";
import React from "react";
import { User } from "../../../app/auth/user";
import format from "../../../app/format/format";
import rpc_service from "../../../app/service/rpc_service";
import { joinReactNodes } from "../../../app/util/react";
import { github } from "../../../proto/github_ts_proto";

interface ReviewListComponentProps {
  user?: User;
}

interface State {
  response?: github.GetGithubPullRequestResponse;
}

export default class ReviewListComponent extends React.Component<ReviewListComponentProps, State> {
  state: State = {};

  componentWillMount() {
    document.title = "Reviews | Buildbuddy";
    rpc_service.service.getGithubPullRequest({}).then((r) => {
      console.log(r);
      this.setState({ response: r });
    });
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
        <PRSection
          title="Needs attention"
          emptyMessage="No changes need your attention, you can relax!"
          loading={!this.state.response}
          prs={needsAttention}
        />
        <PRSection
          title="Incoming reviews"
          emptyMessage="No incoming changes!"
          loading={!this.state.response}
          prs={this.state.response?.incoming ?? []}
        />
        <PRSection
          title="Outgoing reviews"
          emptyMessage="No outgoing changes!"
          loading={!this.state.response}
          prs={this.state.response?.outgoing ?? []}
        />
      </div>
    );
  }
}

interface PRSectionProps {
  title: string;
  emptyMessage: string;
  loading: boolean;
  prs: github.PullRequest[];
}

function PRSection({ title, emptyMessage, loading, prs }: PRSectionProps) {
  return (
    <>
      <div className="reviews-title">
        {title}
        {!loading && <span className="change-count">{prs.length === 1 ? "1 Change" : `${prs.length} Changes`}</span>}
      </div>
      {loading && (
        <div className="empty-state">
          <div className="spinner" />
        </div>
      )}
      {!loading && prs.length === 0 && <div className="empty-state">{emptyMessage}</div>}
      {prs.length > 0 && (
        <>
          <div className="pr pr-header">
            <div>PR</div>
            <div>Author</div>
            <div>Status</div>
            <div>Updated</div>
            <div>Reviewers</div>
            <div>Size</div>
            <div>Description</div>
          </div>
          <div className="pr-rows">
            {prs.map((pr) => (
              <PR pr={pr} />
            ))}
          </div>
        </>
      )}
    </>
  );
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
    let unresolved = false;
    let approved = false;
    for (let [author, review] of Object.entries(this.props.pr.reviews).sort((a, b) => a[0].localeCompare(b[0]))) {
      if (author == this.props.pr.author) continue;
      reviewers.push(
        <span className={`pr-review pr-review-${review.status} ${review.requested ? "pr-review-requested" : ""}`}>
          {author}
        </span>
      );
      if (review.status == "approved") {
        approved = true;
      }
      if (review.status == "changes_requested") {
        unresolved = true;
      }
    }

    let status = "pending";
    if (unresolved) {
      status = "unresolved";
    } else if (approved) {
      status = "approved";
    }

    return (
      <a className="pr" href={this.props.pr.url} target="_blank">
        <div>{this.props.pr.number}</div>
        <div>{this.props.pr.author}</div>
        <div>
          <div className={`pr-status pr-status-${status}`}>{status}</div>
        </div>
        <div>{format.formatDateFromUsec(this.props.pr.updatedAtUsec, { compact: true })}</div>
        <div>{joinReactNodes(reviewers, ", ")}</div>
        <div>{size(this.props.pr)}</div>
        <div>{this.props.pr.title}</div>
      </a>
    );
  }
}
