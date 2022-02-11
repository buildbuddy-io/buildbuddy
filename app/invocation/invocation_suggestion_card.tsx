import React from "react";
import { Radio } from "lucide-react";

interface Props {
  buildLogs: string;
}

export default class SuggestionCardComponent extends React.Component<Props> {
  shouldComponentUpdate(nextProps: Readonly<Props>): boolean {
    return nextProps.buildLogs !== this.props.buildLogs;
  }

  // TODO(siggisim): server side suggestion storing, parsing, and fetching.
  suggestionMap = [
    {
      regex: /stat \/usr\/bin\/gcc: no such file or directory/,
      message: (
        <>
          It looks like the C toolchains aren't configured properly for this invocation, and remote build execution is
          enabled. For more information about configuring toolchains, see the{" "}
          <a href="https://www.buildbuddy.io/docs/rbe-setup" target="_blank">
            BuildBuddy RBE Setup documentation
          </a>
          .
        </>
      ),
    },
    {
      regex: /exec user process caused "exec format error"/,
      message: (
        <>
          It looks like the architecture of the host machine does not match the architecture of the remote execution
          workers. This is likely because Bazel is running on a Mac, but only Linux remote executors are registered in
          the remote execution cluster. Try running Bazel on a Linux host or in a Docker container.
        </>
      ),
    },
    {
      regex: /rpc error: code = Unavailable desc = No registered executors./,
      message: (
        <>
          It looks like no executors are registered for the configured platform. This is likely because Bazel is running
          on a Mac, but only Linux remote executors are registered in the remote execution cluster. Try running Bazel on
          a Linux host or in a Docker container.
        </>
      ),
    },
  ];

  getSuggestion() {
    if (!this.props.buildLogs) return null;

    for (let potentialSuggestion of this.suggestionMap) {
      let matches = this.props.buildLogs.match(potentialSuggestion.regex);
      if (matches) {
        return {
          message: potentialSuggestion.message,
          reason: <>Shown because your build log contains "{matches[0]}"</>,
        };
      }
    }

    return null;
  }

  render() {
    const suggestion = this.getSuggestion();
    if (!suggestion) return null;

    return (
      <div className="card card-suggestion">
        <Radio className="icon white" />
        <div className="content">
          <div className="title">Suggestion from the BuildBuddy Team</div>
          <div className="details">
            <div className="card-suggestion-message">{suggestion.message}</div>
            <div className="card-suggestion-reason">{suggestion.reason}</div>
          </div>
        </div>
      </div>
    );
  }
}
