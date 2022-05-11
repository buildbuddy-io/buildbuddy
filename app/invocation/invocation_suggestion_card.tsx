import React from "react";
import { Radio } from "lucide-react";
import InvocationModel from "./invocation_model";

interface Props {
  model: InvocationModel;
  buildLogs: string;
  /** Show only suggestions with this level. */
  level?: SuggestionLevel;
  /** Show up to this many suggestions. */
  limit?: number;
}

interface Suggestion {
  level: SuggestionLevel;
  message: React.ReactNode;
  reason: React.ReactNode;
}

interface MatchParams {
  model: InvocationModel;
  buildLogs: string;
}

export enum SuggestionLevel {
  INFO,
  ERROR,
}

/** Given some data about an invocation, optionally returns a suggestion. */
type SuggestionMatcher = (params: MatchParams) => Suggestion | null;

export default class SuggestionCardComponent extends React.Component<Props> {
  shouldComponentUpdate(nextProps: Readonly<Props>): boolean {
    return nextProps.buildLogs !== this.props.buildLogs;
  }

  // TODO(siggisim): server side suggestion storing, parsing, and fetching.
  matchers: SuggestionMatcher[] = [
    buildLogRegex({
      level: SuggestionLevel.ERROR,
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
    }),
    buildLogRegex({
      level: SuggestionLevel.ERROR,
      regex: /exec user process caused "exec format error"/,
      message: (
        <>
          It looks like the architecture of the host machine does not match the architecture of the remote execution
          workers. This is likely because Bazel is running on a Mac, but only Linux remote executors are registered in
          the remote execution cluster. Try running Bazel on a Linux host or in a Docker container.
        </>
      ),
    }),
    buildLogRegex({
      level: SuggestionLevel.ERROR,
      regex: /rpc error: code = Unavailable desc = No registered executors./,
      message: (
        <>
          It looks like no executors are registered for the configured platform. This is likely because Bazel is running
          on a Mac, but only Linux remote executors are registered in the remote execution cluster. Try running Bazel on
          a Linux host or in a Docker container.
        </>
      ),
    }),
  ];

  getSuggestions(): Suggestion[] {
    if (!this.props.buildLogs || !this.props.model) return [];

    const suggestions: Suggestion[] = [];
    for (let matcher of this.matchers) {
      const suggestion = matcher({ buildLogs: this.props.buildLogs, model: this.props.model });
      if (this.props.level !== undefined && suggestion.level !== this.props.level) continue;
      if (suggestion) suggestions.push(suggestion);
    }
    return suggestions;
  }

  render() {
    const suggestions = this.getSuggestions();
    if (!suggestions.length) return null;

    return (
      <>
        {suggestions.map((suggestion) => {
          <div className="card card-suggestion">
            <Radio className="icon white" />
            <div className="content">
              <div className="title">Suggestion from the BuildBuddy Team</div>
              <div className="details">
                <div className="card-suggestion-message">{suggestion.message}</div>
                <div className="card-suggestion-reason">{suggestion.reason}</div>
              </div>
            </div>
          </div>;
        })}
      </>
    );
  }
}

/** Returns the given suggestion message if the given regex matches the build logs. */
function buildLogRegex({
  level,
  regex,
  message,
}: {
  level: SuggestionLevel;
  regex: RegExp;
  message: React.ReactNode;
}): SuggestionMatcher {
  return ({ buildLogs }) => {
    const matches = buildLogs.match(regex);
    if (!matches) return null;
    const reason = <>Shown because your build log contains "{matches[0]}"</>;
    return { level, message, reason };
  };
}
