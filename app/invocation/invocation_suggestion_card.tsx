import React from "react";
import { AlertCircle, HelpCircle } from "lucide-react";
import { TextLink } from "../components/link/link";
import InvocationModel from "./invocation_model";

interface Props {
  suggestions: Suggestion[];
  /**
   * Whether this is being displayed from the overview page.
   * Instead of showing all suggestions, this will show a summary
   * and a link to the suggestions tab.
   */
  overview?: boolean;
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

// TODO(siggisim): server side suggestion storing, parsing, and fetching.
const matchers: SuggestionMatcher[] = [
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
        workers. This is likely because Bazel is running on a Mac, but only Linux remote executors are registered in the
        remote execution cluster. Try running Bazel on a Linux host or in a Docker container.
      </>
    ),
  }),
  buildLogRegex({
    level: SuggestionLevel.ERROR,
    regex: /rpc error: code = Unavailable desc = No registered executors./,
    message: (
      <>
        It looks like no executors are registered for the configured platform. This is likely because Bazel is running
        on a Mac, but only Linux remote executors are registered in the remote execution cluster. Try running Bazel on a
        Linux host or in a Docker container.
      </>
    ),
  }),
];

export function getSuggestions({ model, buildLogs }: { model: InvocationModel; buildLogs: string }): Suggestion[] {
  if (!buildLogs || !model) return [];

  const suggestions: Suggestion[] = [];
  for (let matcher of matchers) {
    const suggestion = matcher({ buildLogs, model });
    if (suggestion) suggestions.push(suggestion);
  }
  return suggestions;
}

export default class SuggestionCardComponent extends React.Component<Props> {
  renderIcon(level: SuggestionLevel) {
    switch (level) {
      case SuggestionLevel.INFO:
        return <HelpCircle className="icon" />;
      case SuggestionLevel.ERROR:
        return <AlertCircle className="icon red" />;
    }
  }

  render() {
    const suggestions = this.props.suggestions;
    if (!suggestions.length) return null;

    // When rendering the overview, only show 1 up to one suggestion.
    if (this.props.overview) {
      const suggestion = suggestions[0];
      const hiddenSuggestionsCount = suggestions.length - 1;
      return (
        <div className="card card-suggestion">
          {this.renderIcon(suggestion.level)}
          <div className="content">
            <div className="title">Suggestion{suggestions.length === 1 ? "" : "s"} from the BuildBuddy Team</div>
            <div className="details">
              <div className="card-suggestion-message">{suggestion.message}</div>
              {hiddenSuggestionsCount > 0 && (
                <div className="suggestions-tab-link">
                  <TextLink href="#suggestions">
                    See {suggestions.length - 1} more suggestion{hiddenSuggestionsCount === 1 ? "" : "s"}
                  </TextLink>
                </div>
              )}
            </div>
          </div>
        </div>
      );
    }

    return (
      <div className="suggestions">
        {suggestions.map((suggestion) => (
          <div className="card card-suggestion">
            {this.renderIcon(suggestion.level)}
            <div className="content">
              <div className="details">
                <div className="card-suggestion-message">{suggestion.message}</div>
                <div className="card-suggestion-reason">{suggestion.reason}</div>
              </div>
            </div>
          </div>
        ))}
      </div>
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
