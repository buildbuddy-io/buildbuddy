import React from "react";

export default class FooterComponent extends React.Component {
  render() {
    return (
      <div className="footer">
        <div className="footer-icon-links">
          <a href="https://github.com/buildbuddy-io/buildbuddy/issues/new" target="_blank">
            <img src="/image/alert-triangle-white.svg" /> Report an issue
          </a>
          <a href="https://slack.buildbuddy.io" target="_blank">
            <img src="/image/slack-white.svg" /> BuildBuddy Slack
          </a>
          <a href="https://twitter.com/buildbuddy_io" target="_blank">
            <img src="/image/twitter-white.svg" /> Twitter
          </a>
          <a href="https://github.com/buildbuddy-io/buildbuddy/" target="_blank">
            <img src="/image/github-white.svg" /> Github repo
          </a>
          <a href="mailto:hello@buildbuddy.io" target="_blank">
            <img src="/image/message-circle-white.svg" /> Contact us
          </a>
        </div>
        <div>
          <a href="https://buildbuddy.io/terms" target="_blank">
            Terms
          </a>{" "}
          |{" "}
          <a href="https://buildbuddy.io/privacy" target="_blank">
            Privacy
          </a>{" "}
          |{" "}
          <a href="https://buildbuddy.io" target="_blank">
            BuildBuddy
          </a>{" "}
          | &copy; 2020 Iteration, Inc.
        </div>
      </div>
    );
  }
}
