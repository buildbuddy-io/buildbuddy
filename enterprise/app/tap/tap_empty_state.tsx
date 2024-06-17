import { ChevronRight } from "lucide-react";
import React from "react";

interface Props {
  title: string;
  message: string;
  showV2Instructions: boolean;
}

export default class TapEmptyStateComponent extends React.Component<Props> {
  render() {
    return (
      <div className="container narrow">
        <div className="empty-state history">
          <h2>{this.props.title}</h2>
          {this.props.showV2Instructions ? (
            <p>
              {this.props.message}
              <ul>
                <li>
                  Add <code className="inline-code">--build_metadata=ROLE=CI</code> to your CI bazel test command.
                </li>
                <li>
                  Provide a{" "}
                  <a target="_blank" href="https://www.buildbuddy.io/docs/guide-metadata/#commit-sha">
                    commit SHA
                  </a>{" "}
                  and{" "}
                  <a target="_blank" href="https://www.buildbuddy.io/docs/guide-metadata/#repository-url">
                    repository URL
                  </a>
                  .
                </li>
              </ul>
            </p>
          ) : (
            <p>
              Seems like you haven't done any builds marked as CI. Add <b>--build_metadata=ROLE=CI</b> to your builds to
              see a CI test grid. You'll likely also want to provide a commit SHA and optionally a git repo url.
            </p>
          )}
          <p>Check out the Build Metadata Guide below for more information on configuring these.</p>
          <p>
            <a className="button" target="_blank" href="https://buildbuddy.io/docs/guide-metadata">
              Build Metadata Guide <ChevronRight />
            </a>
          </p>
        </div>
      </div>
    );
  }
}
