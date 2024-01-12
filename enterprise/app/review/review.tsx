import React from "react";
import ReviewListComponent from "./review_list";
import ViewPullRequestComponent from "./view_pull_request";
import { Path } from "../../../app/router/router";

interface CodeReviewComponentProps {
  path: string;
}

export default class CodeReviewComponent extends React.Component<CodeReviewComponentProps> {
  render() {
    const route = this.props.path.substring(Path.reviewsPath.length).split("/");
    // Checking route.length here because a code review path is "org/repo/pull" (3 parts)
    if (route.length === 3) {
      return (
        <ViewPullRequestComponent
          path={this.props.path}
          owner={route[0]}
          repo={route[1]}
          pull={Number(route[2])}></ViewPullRequestComponent>
      );
    }
    if (route.length === 2 && route[0] === "user" && route[1]) {
      return <ReviewListComponent user={route[1]}></ReviewListComponent>;
    }
    return <ReviewListComponent></ReviewListComponent>;
  }
}
