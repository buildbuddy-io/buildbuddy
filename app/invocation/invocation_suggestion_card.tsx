import React from "react";
import { AlertCircle, HelpCircle } from "lucide-react";
import { TextLink } from "../components/link/link";
import InvocationModel from "./invocation_model";
import capabilities from "../capabilities/capabilities";

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
  ({ model, buildLogs }) => {
    if (!capabilities.config.expandedSuggestionsEnabled) return null;

    if (!model.optionsMap.get("remote_cache") && !model.optionsMap.get("remote_executor")) return null;
    if (!buildLogs.includes("DEADLINE_EXCEEDED")) return null;
    if (model.optionsMap.get("remote_timeout") && Number(model.optionsMap.get("remote_timeout")) >= 600) return null;
    if (!model.isComplete() || model.invocations[0]?.success) return null;

    return {
      level: SuggestionLevel.INFO,
      message: (
        <>
          If this build uploads or downloads large artifacts, Bazel may have timed out. If you see this often, consider
          setting a higher value of <BazelFlag>--remote_timeout</BazelFlag>. We recommend a value of 600 (10 minutes).
        </>
      ),
      reason: (
        <>
          Shown because this build is cache-enabled, has an effective{" "}
          <span className="inline-code">remote_timeout</span> less than 10 minutes, and failed with a log message
          containing "DEADLINE_EXCEEDED"
        </>
      ),
    };
  },
  // Suggest remote.buildbuddy.io instead of cloud.buildbuddy.io
  ({ model }) => {
    if (!capabilities.config.expandedSuggestionsEnabled) return null;

    // remote.buildbuddy.io doesn't support cert-based auth
    if (model.optionsMap.get("tls_client_certificate")) return null;
    const flags = [];
    for (const flag of ["bes_backend", "remote_cache", "remote_executor"]) {
      const value = model.optionsMap.get(flag) || "";
      if (value === "cloud.buildbuddy.io" || value === "grpcs://cloud.buildbuddy.io") {
        flags.push(flag);
      }
    }
    if (!flags.length) return null;
    return {
      level: SuggestionLevel.INFO,
      message: (
        <>
          This build is using the old BuildBuddy endpoint, <span className="inline-code">cloud.buildbuddy.io</span>.
          Migrate to <span className="inline-code">remote.buildbuddy.io</span> for improved performance.
        </>
      ),
      reason: (
        <>
          Shown because this build has the effective flag{flags.length === 1 ? "" : "s"}{" "}
          {flags.map((flag, i) => (
            <>
              <span className="inline-code">
                --{flag}={model.optionsMap.get(flag)}
              </span>
              {i < flags.length - 1 && " "}
            </>
          ))}
        </>
      ),
    };
  },
  // Suggest using cache compression
  ({ model }) => {
    if (!capabilities.config.expandedSuggestionsEnabled) return null;

    if (model.optionsMap.get("experimental_remote_cache_compression")) return null;
    if (!model.optionsMap.get("remote_cache") && !model.optionsMap.get("remote_executor")) return null;
    const version = model.started?.buildToolVersion;
    if (!version) return null;
    const segments = version.split(".").map(Number);
    // Bazel pre-v5 doesn't support compression.
    if (!segments[0] || segments[0] < 5) return null;

    return {
      level: SuggestionLevel.INFO,
      message: (
        <>
          Consider adding the Bazel flag <BazelFlag>--experimental_remote_cache_compression</BazelFlag> to improve
          remote cache throughput.
        </>
      ),
      reason: <>Shown because this build is cache-enabled but compression is not enabled.</>,
    };
  },
  // Suggest using --jobs
  ({ model }) => {
    if (!capabilities.config.expandedSuggestionsEnabled) return null;

    if (!model.optionsMap.get("remote_executor")) return null;
    if (model.optionsMap.get("jobs")) return null;

    return {
      level: SuggestionLevel.INFO,
      message: (
        <>
          Consider setting the Bazel flag <BazelFlag>--jobs</BazelFlag> to allow more actions to execute in parallel,
          which can significantly improve remote execution performance. We recommend starting with{" "}
          <span className="inline-code">--jobs=50</span> and working your way up.
        </>
      ),
      reason: <>Shown because this build has remote execution enabled, but a jobs count is not configured.</>,
    };
  },
  // Suggest using remote_download_minimal
  ({ model }) => {
    if (!capabilities.config.expandedSuggestionsEnabled) return null;

    if (!model.optionsMap.get("remote_cache") && !model.optionsMap.get("remote_executor")) return null;
    if (model.optionsMap.get("remote_download_outputs")) return null;

    return {
      level: SuggestionLevel.INFO,
      message: (
        <>
          Consider adding the Bazel flag <BazelFlag>--remote_download_minimal</BazelFlag> or{" "}
          <BazelFlag>--remote_download_toplevel</BazelFlag>, which can significantly improve performance for builds with
          a large number of intermediate results.
        </>
      ),
      reason: (
        <>
          Shown because this build is cache-enabled and the flag{" "}
          <span className="inline-code">--remote_download_outputs</span> is not explicitly set.
        </>
      ),
    };
  },
  // Suggest configuring metadata to enable test grid
  ({ model }) => {
    if (!capabilities.config.expandedSuggestionsEnabled) return null;

    if (!capabilities.config.testDashboardEnabled) return null;
    if (model.invocations[0]?.role !== "CI") return null;
    if (model.invocations[0]?.command !== "test") return null;
    if (model.getCommit() && model.getRepo()) return null;

    const missing = [
      !model.getCommit() && <span className="inline-code">COMMIT_SHA</span>,
      !model.getRepo() && <span className="inline-code">REPO_URL</span>,
    ];

    return {
      level: SuggestionLevel.INFO,
      message: (
        <>
          Add <InlineProseList items={missing} /> metadata to see CI tests in the{" "}
          <TextLink href="/tests/">test dashboard</TextLink> (see the{" "}
          <TextLink href="https://www.buildbuddy.io/docs/guide-metadata/">build metadata guide</TextLink> for help).
        </>
      ),
      reason: (
        <>
          Shown because this invocation was detected to be a CI test, but is missing metadata for{" "}
          <InlineProseList items={missing} />.
        </>
      ),
    };
  },
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

function BazelFlag({ children }: { children: string }) {
  return (
    <TextLink
      className="inline-code bazel-flag"
      href={`https://docs.bazel.build/versions/main/command-line-reference.html#flag${children}`}>
      {children}
    </TextLink>
  );
}

/**
 * Renders a list of items separated by commas, with "and" preceding the final item.
 * For convenience, falsy list items are filtered out.
 */
function InlineProseList({ items }: { items: React.ReactNode[] }) {
  items = items.filter((item) => item);
  const out = [];
  for (let i = 0; i < items.length; i++) {
    out.push(items[i]);

    if (i === items.length - 1) break;

    if (i === items.length - 2) {
      if (items.length === 2) {
        out.push(" and ");
      } else {
        out.push(", and ");
      }
    } else {
      out.push(", ");
    }
  }
  return <>{out}</>;
}
