import React from "react";
import { ReviewModel } from "./review_model";
import FilledButton, { OutlinedButton } from "../../../app/components/button/button";
import { Github } from "lucide-react";
import { github } from "../../../proto/github_ts_proto";
import { ReviewController } from "./review_controller";
import Link from "../../../app/components/link/link";
import router from "../../../app/router/router";

interface PullRequestHeaderComponentProps {
  reviewModel: ReviewModel;
  controller: ReviewController;
  path: string;
}

export default class PullRequestHeaderComponent extends React.Component<PullRequestHeaderComponentProps> {
  render() {
    const model = this.props.reviewModel;
    const userIsPrAuthor = model.viewerIsPrAuthor();
    return (
      <div>
        <div className="review-header">
          <span className="review-title">
            <span className="review-number">
              <Link href={router.getReviewUrl(model.getOwner(), model.getRepo(), model.getPullNumber())}>
                Change #{model.getPullNumber()}
              </Link>
              &nbsp;
            </span>
            <span className="review-details">
              by <span className="review-author">{model.getAuthor()}</span> in{" "}
              <span className="review-repo">
                {model.getOwner()}/{model.getRepo()}
              </span>
            </span>
            <a href={model.getGithubUrl()} className="review-gh-link">
              <Github size="16" className="icon" />
            </a>
          </span>
          <div className="review-actions">
            {userIsPrAuthor && !model.isSubmitted() && (
              <OutlinedButton disabled={!model.isMergeable()} onClick={() => this.props.controller.submitPr()}>
                Submit
              </OutlinedButton>
            )}
            {!userIsPrAuthor && (
              <OutlinedButton onClick={() => this.props.controller.startReviewReply(true)}>Approve</OutlinedButton>
            )}
            <FilledButton onClick={() => this.props.controller.startReviewReply(false)}>Reply</FilledButton>
          </div>
        </div>
        <div className="header-separator"></div>
      </div>
    );
  }
}
