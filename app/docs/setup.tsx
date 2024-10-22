import React from "react";
import capabilities from "../capabilities/capabilities";
import SetupCodeComponent from "./setup_code";
import { User } from "../auth/user";

interface Props {
  user?: User;
}

interface State {
  menuExpanded: boolean;
}

export default class SetupComponent extends React.Component<Props> {
  state: State = {
    menuExpanded: false,
  };

  componentWillMount() {
    document.title = `Setup | BuildBuddy`;
  }

  render() {
    let bbURL = this.props.user?.selectedGroup.url || `${window.location.protocol}//${window.location.host}`;

    return (
      <div className="home">
        <div className="container narrow">
          <div className="title">Quickstart</div>
          {this.props.children}
          To get started, select options below then copy the results to your <b>.bazelrc</b> file.
          <br />
          <br />
          If you don't have a <b>.bazelrc</b> file - create one in the same directory as your Bazel <b>WORKSPACE</b>{" "}
          file.
          <h2>1. Configure your .bazelrc</h2>
          <SetupCodeComponent />
          <h2>2. Verify your installation</h2>
          Once you've added those lines to your <b>.bazelrc</b>, kick off a bazel build.
          <br />
          <br />
          You'll get a BuildBuddy URL printed at the beginning and the end of every Bazel invocation like this:
          <code>
            bazel build //...
            <br />
            INFO: Streaming build results to: {bbURL}/invocation/7bedd84e-525e-4b93-a5f5-53517d57752b
            <br />
            ...
          </code>
          Now you can ⌘ click / double click on these urls to see the results of your build!
          <br />
          <br />
          {capabilities.enterprise && (
            <>
              Visit your <a href="/">build history</a> to make sure that your builds are associated with your account.
              <br />
              <br />
              {capabilities.anonymous && (
                <span>
                  Note: Builds using the <b>No auth</b> option will not appear in your history, as they can't be
                  associated with your account.
                </span>
              )}
            </>
          )}
          {!capabilities.enterprise && (
            <div>
              <h2>Enterprise BuildBuddy</h2>
              Want enterprise features like SSO, organization build history, trends, remote build execution and more?
              <br />
              <br />
              <b>
                <a target="_blank" href="https://buildbuddy.typeform.com/to/wIXFIA">
                  Click here
                </a>
              </b>{" "}
              to upgrade to enterprise BuildBuddy.
            </div>
          )}
          <h2>Documentation</h2>
          Visit our <a href="https://www.buildbuddy.io/docs/introduction">documentation</a> for more information on
          setting up, configuring, and using BuildBuddy.
          <h2>Get in touch!</h2>
          {capabilities.config.communityLinksEnabled ? (
            <>
              Join our <a href="https://community.buildbuddy.io">Slack channel</a> or email us at{" "}
              <a href="mailto:hello@buildbuddy.io">hello@buildbuddy.io</a> if you have any questions or feature
              requests!
            </>
          ) : (
            <>
              Email us at <a href="mailto:hello@buildbuddy.io">hello@buildbuddy.io</a> if you have any questions or
              feature requests!
            </>
          )}
        </div>
      </div>
    );
  }
}
