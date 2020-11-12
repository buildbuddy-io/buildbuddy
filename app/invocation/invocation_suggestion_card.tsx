import React from "react";
import InvocationModel from "./invocation_model";

interface Props {
  model: InvocationModel;
}

interface State {
  suggestion: React.ReactElement;
  reason: React.ReactElement;
}

export default class SuggestionCardComponent extends React.Component {
  props: Props;

  state: State = {
    suggestion: undefined,
    reason: undefined,
  };

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

  componentDidMount() {
    this.updateSuggestion();
  }

  componentDidUpdate(prevProps: Props) {
    if (prevProps.model !== this.props.model) {
      this.updateSuggestion();
    }
  }

  updateSuggestion() {
    if (!this.props.model.consoleBuffer) return;

    for (let potentialSuggestion of this.suggestionMap) {
      let matches = this.props.model.consoleBuffer.match(potentialSuggestion.regex);
      if (matches) {
        this.setState({
          suggestion: potentialSuggestion.message,
          reason: <>Shown because your build log contains "{matches[0]}"</>,
        });
        break;
      }
    }
  }

  render() {
    if (!this.state.suggestion) {
      return <></>;
    }
    return (
      <div className="card card-suggestion">
        <img className="icon" src="/image/radio-white.svg" />
        <div className="content">
          <div className="title">Suggestion from the BuildBuddy Team</div>
          <div className="details">
            <div className="card-suggestion-message">{this.state.suggestion}</div>
            <div className="card-suggestion-reason">{this.state.reason}</div>
          </div>
        </div>
      </div>
    );
  }
}
