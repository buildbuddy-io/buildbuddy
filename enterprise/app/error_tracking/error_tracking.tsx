import {
  AlertTriangle,
  Bug,
  CalendarClock,
  ChevronRight,
  ExternalLink,
  GitBranch,
  GitCommit,
  Layers3,
  Search,
  Terminal,
  User,
  Workflow as WorkflowIcon,
} from "lucide-react";
import Long from "long";
import React from "react";
import Breadcrumbs from "../../../app/components/breadcrumbs/breadcrumbs";
import { FilledButton } from "../../../app/components/button/button";
import Link from "../../../app/components/link/link";
import Select, { Option } from "../../../app/components/select/select";
import errorService from "../../../app/errors/error_service";
import router, { Path } from "../../../app/router/router";
import rpcService, { CancelablePromise } from "../../../app/service/rpc_service";
import { error_tracking } from "../../../proto/error_tracking_ts_proto";
import { Subscription } from "rxjs";

interface Props {
  search: URLSearchParams;
  enabled?: boolean;
}

interface State {
  groups: error_tracking.ErrorGroup[];
  selected?: error_tracking.ErrorGroup;
  loading: boolean;
  query: string;
  error?: string;
  appendError?: string;
  appending?: boolean;
  nextPageToken?: string;
}

export interface InvocationOccurrences {
  invocationId: string;
  latestEventTimeUsec: Long;
  occurrences: error_tracking.ErrorOccurrence[];
}

export interface ErrorTrackingTimeWindow {
  startTimeUsec: Long;
  endTimeUsec: Long;
}

export type ErrorGroupSortParam = "recent" | "affected" | "frequency";
export type ErrorOriginScopeParam = "bazel" | "workflow";

export function errorOriginScopeParam(search: URLSearchParams): ErrorOriginScopeParam {
  return search.get("kind") === "workflow" ? "workflow" : "bazel";
}

export function errorOriginScopeProto(search: URLSearchParams) {
  return errorOriginScopeParam(search) === "workflow"
    ? error_tracking.ErrorOrigin.ERROR_ORIGIN_WORKFLOW
    : error_tracking.ErrorOrigin.ERROR_ORIGIN_BAZEL;
}

export function isWorkflowOrigin(origin: error_tracking.ErrorOrigin) {
  return origin === error_tracking.ErrorOrigin.ERROR_ORIGIN_WORKFLOW;
}

export function errorOriginLabel(origin: error_tracking.ErrorOrigin) {
  if (origin === error_tracking.ErrorOrigin.ERROR_ORIGIN_WORKFLOW) return "Workflow";
  if (origin === error_tracking.ErrorOrigin.ERROR_ORIGIN_WORKFLOW_BAZEL_CHILD) return "Bazel · Workflow child";
  return "Bazel";
}

export function errorGroupSortParam(search: URLSearchParams): ErrorGroupSortParam {
  const sort = search.get("sort");
  return sort === "recent" || sort === "frequency" ? sort : "affected";
}

export function errorGroupSortProto(search: URLSearchParams) {
  switch (errorGroupSortParam(search)) {
    case "affected":
      return error_tracking.ErrorGroupSort.ERROR_GROUP_SORT_AFFECTED_BUILDS;
    case "frequency":
      return error_tracking.ErrorGroupSort.ERROR_GROUP_SORT_RECENT_FREQUENCY;
    default:
      return error_tracking.ErrorGroupSort.ERROR_GROUP_SORT_LAST_SEEN;
  }
}

export function errorGroupSortDescription(sort: ErrorGroupSortParam, affectedNoun = "builds") {
  switch (sort) {
    case "affected":
      return `Sorted by affected ${affectedNoun}, then most recently seen.`;
    case "frequency":
      return "Sorted by latest-period frequency, then most recently seen.";
    default:
      return "Sorted by most recently seen.";
  }
}

export function paginationTimeWindow(
  previous: ErrorTrackingTimeWindow | undefined,
  append: boolean,
  nowMillis = Date.now()
): ErrorTrackingTimeWindow {
  if (append && previous) return previous;
  const endTimeUsec = nowMillis * 1000;
  return {
    startTimeUsec: Long.fromNumber(endTimeUsec - 7 * 24 * 60 * 60 * 1_000_000),
    endTimeUsec: Long.fromNumber(endTimeUsec),
  };
}

export function groupOccurrencesByInvocation(occurrences: error_tracking.ErrorOccurrence[]): InvocationOccurrences[] {
  const groups = new Map<string, InvocationOccurrences>();
  for (const occurrence of occurrences) {
    const invocationId = occurrence.invocationId;
    const group = groups.get(invocationId);
    if (group) {
      group.occurrences.push(occurrence);
      if (Number(occurrence.eventTimeUsec) > Number(group.latestEventTimeUsec)) {
        group.latestEventTimeUsec = occurrence.eventTimeUsec;
      }
    } else {
      groups.set(invocationId, {
        invocationId,
        latestEventTimeUsec: occurrence.eventTimeUsec,
        occurrences: [occurrence],
      });
    }
  }
  return [...groups.values()].sort((a, b) => Number(b.latestEventTimeUsec) - Number(a.latestEventTimeUsec));
}

function testIdentity(suite: string, testClass: string, name: string) {
  return [suite, testClass, name].filter((part, index, all) => part && all.indexOf(part) === index).join(" › ");
}

function firstMessageLine(message: string) {
  return message
    .split("\n")
    .map((line) => line.trim())
    .find(Boolean);
}

export function issueTitle(group: error_tracking.IErrorGroup) {
  if (isWorkflowOrigin(group.origin || error_tracking.ErrorOrigin.ERROR_ORIGIN_UNKNOWN)) {
    return group.sampleInvocationPattern || firstMessageLine(group.sampleMessage || "") || "Workflow failure";
  }
  return (
    testIdentity(group.sampleTestSuite || "", group.sampleTestClass || "", group.sampleTestName || "") ||
    firstMessageLine(group.sampleMessage || "") ||
    group.errorType ||
    "Unknown error"
  );
}

export function fingerprintExplanation(version: string, source: string) {
  if (version.startsWith("workflow:") || source === "workflow_bes") {
    return "Uses the Workflow action name and the underlying normalized BES failure fingerprint. This keeps failures from different workflow actions separate while grouping repeated failures of the same action.";
  }
  if (version.startsWith("test:") || source.toLowerCase().includes("test.xml")) {
    return "Uses the Bazel target and structured test identity, failure kind and type, and a normalized diagnostic. Run, shard, attempt, and source line numbers do not split the issue; stable frames retain only the final path components.";
  }
  if (version.startsWith("test_fallback") || source === "test_result_fallback") {
    return "No structured test case was available. This issue is conservatively scoped to the Bazel test target and normalized test log diagnostic, so unrelated tests are not merged automatically.";
  }
  if (version.startsWith("action_fallback:")) {
    return "Uses the BES failure category, action mnemonic, target, and a normalized diagnostic signature. The target keeps generic failures from unrelated actions in separate groups.";
  }
  return "Uses the BES failure category, action mnemonic, and a normalized diagnostic signature. File locations, UUIDs, long hexadecimal values, and labeled volatile values such as process IDs are normalized; semantic numbers are retained.";
}

export interface FingerprintTechnicalDetails {
  formula: string;
  included: string[];
  normalizedOrExcluded: string[];
}

export function fingerprintTechnicalDetails(version: string, source: string): FingerprintTechnicalDetails {
  if (version.startsWith("workflow:") || source === "workflow_bes") {
    return {
      formula: "H(workflow:v1, workflow action, underlying BES failure fingerprint)",
      included: ["BuildBuddy Workflow action name", "Underlying normalized BES failure fingerprint"],
      normalizedOrExcluded: [
        "Repeated whitespace in the Workflow action name is normalized",
        "Run ID, invocation ID, branch, commit, user, and timestamps are excluded",
        "The underlying BES fingerprint retains its own action, target, diagnostic, and normalization rules",
      ],
    };
  }
  if (version.startsWith("test:") || source.toLowerCase().includes("test.xml")) {
    return {
      formula:
        "H(test:v2, target, suite, class, test, failure kind, failure type, normalized message, stable app frame*)",
      included: [
        "Bazel target and structured suite, class, and test name",
        "Failure kind and type plus the normalized JUnit message",
        "A stable non-framework app frame only when the message is missing or generic",
      ],
      normalizedOrExcluded: [
        "UUIDs, timestamps, addresses, long hex values, whitespace, and file line/column locations are normalized",
        "Run, shard, attempt, exit code, cache strategy, invocation, and event metadata are excluded",
        "Stable frames discard line numbers and retain at most the final two path components",
      ],
    };
  }
  if (version.startsWith("test_fallback:") || source === "test_result_fallback") {
    return {
      formula: "H(test_fallback:v2, target, final status, normalized test-log diagnostic)",
      included: ["Bazel test target", "Final Bazel test status", "Normalized test.log diagnostic"],
      normalizedOrExcluded: [
        "Uses the same UUID, timestamp, address, source-location, and whitespace normalization as structured tests",
        "Configuration, run, shard, attempt, exit code, cache strategy, invocation, and event metadata are excluded",
        "No stack frame is added because structured testcase identity was unavailable",
      ],
    };
  }
  if (version.startsWith("action_fallback:")) {
    return {
      formula: "H(action_fallback:v1, failure category, mnemonic, target, diagnostic signature)",
      included: ["BES failure category", "Action mnemonic and Bazel target", "Up to three diagnostic lines"],
      normalizedOrExcluded: [
        "Among the first 32 non-empty lines, prefers the first source-located line; otherwise the first distinctive line, then the first generic failure line",
        "File locations, UUIDs, long hex values, and labeled volatile numbers are normalized",
        "Semantic numbers and wording are retained; exit code and invocation metadata are excluded",
      ],
    };
  }
  return {
    formula: "H(failure category, mnemonic, diagnostic signature)",
    included: ["BES failure category", "Action mnemonic when present", "Up to three diagnostic lines"],
    normalizedOrExcluded: [
      "Among the first 32 non-empty lines, prefers the first source-located line; otherwise the first distinctive line, then the first generic failure line",
      "File locations, UUIDs, long hex values, and labeled volatile numbers are normalized",
      "Semantic numbers and wording are retained; target, exit code, and invocation metadata are excluded",
    ],
  };
}

export function isTargetRedundant(target: string, identity: string) {
  if (!target || !identity) return false;
  const normalize = (value: string) => value.toLowerCase().replace(/[^a-z0-9]+/g, "");
  const normalizedIdentity = normalize(identity);
  const normalizedTarget = normalize(target);
  return normalizedTarget.length >= 4 && normalizedIdentity.includes(normalizedTarget);
}

export function groupRowTarget(group: error_tracking.IErrorGroup) {
  const target = group.sampleTargetLabel || "";
  const identity = testIdentity(group.sampleTestSuite || "", group.sampleTestClass || "", group.sampleTestName || "");
  if (!target || isTargetRedundant(target, identity)) return "";
  return issueTitle(group).toLowerCase().includes(target.toLowerCase()) ? "" : target;
}

function failureSeverity(...values: (string | number)[]) {
  const classification = values.join(" ").toLowerCase();
  return classification.includes("timeout") || classification.includes("cancel") ? "warning" : "error";
}

export function frequencyBarHeights(buckets: error_tracking.IErrorFrequencyBucket[], sharedMaxCount?: number) {
  const maxCount =
    sharedMaxCount || Math.max(0, ...buckets.map((bucket) => Number(bucket.affectedInvocationCount || 0)));
  return buckets.map((bucket) => {
    const count = Number(bucket.affectedInvocationCount || 0);
    if (!count || !maxCount) return 0;
    return Math.max(8, Math.round((count / maxCount) * 100));
  });
}

export function frequencyDescription(buckets: error_tracking.IErrorFrequencyBucket[], affectedNoun = "builds") {
  const counts = buckets.map((bucket) => Number(bucket.affectedInvocationCount || 0));
  const latestCount = counts[counts.length - 1] || 0;
  return `Frequency, oldest to newest: ${counts.join(", ")} affected ${affectedNoun}. ${latestCount} in the latest bucket.`;
}

function bucketDateRange(bucket: error_tracking.IErrorFrequencyBucket) {
  const options: Intl.DateTimeFormatOptions = { month: "short", day: "numeric" };
  const start = new Date(Number(bucket.startTimeUsec) / 1000).toLocaleDateString(undefined, options);
  const end = new Date(Number(bucket.endTimeUsec) / 1000).toLocaleDateString(undefined, options);
  return start === end ? start : `${start} – ${end}`;
}

function latestBucketLabel(bucket: error_tracking.IErrorFrequencyBucket | undefined) {
  if (!bucket) return "latest bucket";
  const durationHours = (Number(bucket.endTimeUsec) - Number(bucket.startTimeUsec) + 1) / (60 * 60 * 1_000_000);
  return durationHours <= 26 ? "last 24h" : `latest ${Math.ceil(durationHours / 24)}d`;
}

function relativeTime(timestampUsec: number | Long, nowMillis = Date.now()) {
  const deltaMillis = Math.max(0, nowMillis - Number(timestampUsec) / 1000);
  const units: [number, string][] = [
    [24 * 60 * 60 * 1000, "day"],
    [60 * 60 * 1000, "hour"],
    [60 * 1000, "minute"],
  ];
  for (const [unitMillis, unit] of units) {
    if (deltaMillis >= unitMillis) {
      const value = Math.floor(deltaMillis / unitMillis);
      return `${value} ${unit}${value === 1 ? "" : "s"} ago`;
    }
  }
  return "just now";
}

export function displayCount(count: number | Long) {
  return count.toString();
}

export function fingerprintLabel(version: string, source: string) {
  if (version.startsWith("workflow:") || source === "workflow_bes") return "Workflow failure";
  const isFallback = version.startsWith("test_fallback") || source === "test_result_fallback";
  if (isFallback) return "Conservative fallback";
  if (version.startsWith("test:") || source.toLowerCase().includes("test.xml")) return "Structured test result";
  return "BES diagnostic";
}

export function fingerprintSourceLabel(source: string) {
  if (source === "test_xml") return "test.xml";
  if (source === "test_result_fallback") return "test.log";
  if (source === "action_output" || source === "action_output_fallback") return "action output";
  if (source === "action_event" || source === "action_event_fallback") return "BES action event";
  return source.replaceAll("_", " ");
}

export default class ErrorTrackingComponent extends React.Component<Props, State> {
  state: State = { groups: [], loading: this.props.enabled !== false, query: this.props.search.get("q") || "" };
  private rpc?: CancelablePromise<void>;
  private activeTimeWindow?: ErrorTrackingTimeWindow;
  private refreshSubscription?: Subscription;

  componentDidMount() {
    this.refreshSubscription = rpcService.events.subscribe({
      next: (name) => name === "refresh" && this.refresh(),
    });
    if (this.props.enabled !== false) {
      this.fetchGroups(this.selectedFingerprint());
    }
  }

  componentDidUpdate(previousProps: Props) {
    const previousFingerprint = this.selectedFingerprint(previousProps);
    const fingerprint = this.selectedFingerprint();
    const previousQuery = previousProps.search.get("q") || "";
    const query = this.props.search.get("q") || "";
    const previousSort = errorGroupSortParam(previousProps.search);
    const sort = errorGroupSortParam(this.props.search);
    const previousOrigin = errorOriginScopeParam(previousProps.search);
    const origin = errorOriginScopeParam(this.props.search);
    if (
      fingerprint !== previousFingerprint ||
      query !== previousQuery ||
      sort !== previousSort ||
      origin !== previousOrigin
    ) {
      if (query !== this.state.query) {
        this.setState({ query }, () => this.fetchGroups(fingerprint, query));
      } else {
        this.fetchGroups(fingerprint, query);
      }
    }
  }

  componentWillUnmount() {
    this.rpc?.cancel();
    this.refreshSubscription?.unsubscribe();
  }

  private selectedFingerprint(props = this.props) {
    return props.search.get("fingerprint") || "";
  }

  private appliedQuery() {
    return this.props.search.get("q") || "";
  }

  private refresh() {
    if (this.props.enabled === false) return;
    this.activeTimeWindow = undefined;
    this.fetchGroups(this.selectedFingerprint(), this.appliedQuery());
  }

  private fingerprintHref(fingerprint: string) {
    const query = new URLSearchParams(this.props.search);
    query.set("fingerprint", fingerprint);
    return `${Path.errorTrackingPath}?${query}`;
  }

  private showAllErrors() {
    const query = new URLSearchParams(this.props.search);
    query.delete("fingerprint");
    router.navigateTo(query.size ? `${Path.errorTrackingPath}?${query}` : Path.errorTrackingPath);
  }

  private applySearch() {
    const query = new URLSearchParams(this.props.search);
    query.delete("fingerprint");
    if (this.state.query) {
      query.set("q", this.state.query);
    } else {
      query.delete("q");
    }
    const destination = query.size ? `${Path.errorTrackingPath}?${query}` : Path.errorTrackingPath;
    const current = `${Path.errorTrackingPath}${this.props.search.size ? `?${this.props.search}` : ""}`;
    if (destination === current) {
      this.fetchGroups("", this.state.query);
    } else {
      router.navigateTo(destination);
    }
  }

  private clearSearch() {
    this.setState({ query: "" });
    const query = new URLSearchParams(this.props.search);
    query.delete("q");
    query.delete("fingerprint");
    router.navigateTo(query.size ? `${Path.errorTrackingPath}?${query}` : Path.errorTrackingPath);
  }

  private setSort(sort: ErrorGroupSortParam) {
    const query = new URLSearchParams(this.props.search);
    query.delete("fingerprint");
    if (sort === "affected") {
      query.delete("sort");
    } else {
      query.set("sort", sort);
    }
    router.navigateTo(query.size ? `${Path.errorTrackingPath}?${query}` : Path.errorTrackingPath);
  }

  private setOriginScope(origin: ErrorOriginScopeParam) {
    const query = new URLSearchParams(this.props.search);
    query.delete("fingerprint");
    if (origin === "workflow") {
      query.set("kind", "workflow");
    } else {
      query.delete("kind");
    }
    router.navigateTo(query.size ? `${Path.errorTrackingPath}?${query}` : Path.errorTrackingPath);
  }

  private fetchGroups(fingerprint = "", query = this.appliedQuery(), pageToken = "", append = false) {
    this.rpc?.cancel();
    if (append) {
      this.setState({ appending: true, appendError: undefined });
    } else {
      this.setState({ loading: true, error: undefined, appending: false, appendError: undefined });
    }
    this.activeTimeWindow = paginationTimeWindow(this.activeTimeWindow, append);
    const request = new error_tracking.GetErrorGroupsRequest({
      startTimeUsec: this.activeTimeWindow.startTimeUsec,
      endTimeUsec: this.activeTimeWindow.endTimeUsec,
      query,
      fingerprint,
      pageSize: fingerprint ? 5 : 50,
      pageToken,
      sort: errorGroupSortProto(this.props.search),
      origin: errorOriginScopeProto(this.props.search),
    });
    this.rpc = rpcService.service
      .getErrorGroups(request)
      .then((response) => {
        if (fingerprint) {
          const page = response.groups[0];
          const selected =
            append && page && this.state.selected
              ? new error_tracking.ErrorGroup({
                  ...page,
                  occurrences: [...this.state.selected.occurrences, ...page.occurrences],
                })
              : page;
          this.setState({
            selected,
            nextPageToken: response.nextPageToken,
            loading: false,
            appending: false,
            appendError: undefined,
            error: selected ? undefined : "This error group is no longer available in the selected time range.",
          });
        } else {
          this.setState({
            groups: append ? [...this.state.groups, ...response.groups] : response.groups,
            selected: undefined,
            nextPageToken: response.nextPageToken,
            loading: false,
            appending: false,
            appendError: undefined,
          });
        }
      })
      .catch((e) => {
        errorService.handleError(e);
        if (append) {
          this.setState({ appendError: e?.message || "Failed to load more errors", appending: false });
        } else {
          this.setState({ error: e?.message || "Failed to load errors", loading: false });
        }
      });
  }

  private renderFingerprintMetadata(version: string, source: string, confidence: string) {
    const label = fingerprintLabel(version, source);
    const isFallback = label === "Conservative fallback";
    const isStructuredTest = label === "Structured test result";
    return (
      <div className="fingerprint-metadata" aria-label="Fingerprint strategy">
        <span
          className={`fingerprint-badge${isStructuredTest ? " fingerprint-badge-structured" : ""}${
            isFallback ? " fingerprint-badge-fallback" : ""
          }`}>
          {label}
        </span>
        {confidence && <span>{confidence.toLowerCase()} confidence</span>}
        {version && <code>{version}</code>}
        {source && <span>Evidence: {fingerprintSourceLabel(source)}</span>}
      </div>
    );
  }

  private renderFingerprintTechnicalDetails(group: error_tracking.ErrorGroup) {
    const details = fingerprintTechnicalDetails(group.fingerprintVersion, group.fingerprintSource);
    return (
      <details className="fingerprint-explanation">
        <summary className="fingerprint-explanation-summary">
          <span className="fingerprint-explanation-summary-copy">
            <strong>How this fingerprint is calculated</strong>
            <small>View grouping inputs, normalization, and metadata</small>
          </span>
          <ChevronRight className="fingerprint-explanation-chevron" size={18} aria-hidden="true" />
        </summary>
        <div className="fingerprint-explanation-body">
          <p>{fingerprintExplanation(group.fingerprintVersion, group.fingerprintSource)}</p>
          <section className="fingerprint-metadata-section" aria-labelledby="fingerprint-metadata-title">
            <div>
              <h4 id="fingerprint-metadata-title">Fingerprint metadata</h4>
              <p>
                Strategy describes which evidence formed this group. Confidence indicates how much structured failure
                information was available; version identifies the exact grouping algorithm. Evidence names the BES
                artifact or event that supplied the diagnostic.
              </p>
            </div>
            {this.renderFingerprintMetadata(
              group.fingerprintVersion,
              group.fingerprintSource,
              group.fingerprintConfidence
            )}
          </section>
          <div className="fingerprint-formula">
            <span>Canonical basis</span>
            <code>{details.formula}</code>
            <small>H = the first 16 bytes of SHA-256 over UTF-8 fields separated by NUL bytes.</small>
          </div>
          <div className="fingerprint-rules">
            <div>
              <h4>Included inputs</h4>
              <ul>
                {details.included.map((item) => (
                  <li key={item}>{item}</li>
                ))}
              </ul>
            </div>
            <div>
              <h4>Normalization and exclusions</h4>
              <ul>
                {details.normalizedOrExcluded.map((item) => (
                  <li key={item}>{item}</li>
                ))}
              </ul>
            </div>
          </div>
          <div className="fingerprint-id">
            <span>Group fingerprint</span>
            <code>{group.fingerprint}</code>
          </div>
        </div>
      </details>
    );
  }

  private renderFrequency(
    group: error_tracking.ErrorGroup,
    index: number,
    sharedMaxCount: number,
    affectedNoun: string
  ) {
    const buckets = group.frequencyBuckets;
    if (!buckets.length) {
      return (
        <span className="error-frequency-empty" id={`error-frequency-${index}`}>
          Frequency unavailable
        </span>
      );
    }
    const heights = frequencyBarHeights(buckets, sharedMaxCount);
    const latest = buckets[buckets.length - 1];
    const latestCount = Number(latest?.affectedInvocationCount || 0);
    const description = frequencyDescription(buckets, affectedNoun);
    return (
      <div className="error-frequency">
        <div className="mini-histogram" role="img" aria-label={description}>
          {buckets.map((bucket, bucketIndex) => (
            <span
              className={bucketIndex >= buckets.length - 2 ? "recent" : ""}
              key={bucketIndex}
              title={`${bucketDateRange(bucket)}: ${bucket.affectedInvocationCount} affected ${affectedNoun}`}
              aria-hidden="true">
              <i style={{ height: `${heights[bucketIndex]}%` }} />
            </span>
          ))}
        </div>
        <span className="frequency-latest" aria-hidden="true">
          <b>{latestCount}</b> {latestBucketLabel(latest)}
        </span>
        <span className="visually-hidden" id={`error-frequency-${index}`}>
          {description}
        </span>
      </div>
    );
  }

  private renderOccurrenceContext(
    occurrence: error_tracking.ErrorOccurrence,
    invocationHref: string,
    index: number,
    openDiagnostic: boolean
  ) {
    const identity = testIdentity(occurrence.testSuite, occurrence.testClass, occurrence.testName);
    const workflow = isWorkflowOrigin(occurrence.origin);
    const targetIsIdentity = isTargetRedundant(occurrence.targetLabel, identity);
    const targetHref = `${invocationHref}?target=${encodeURIComponent(occurrence.targetLabel)}`;
    const severity = failureSeverity(
      occurrence.testFailureKind,
      occurrence.testFailureType,
      occurrence.errorType,
      occurrence.exitCode
    );
    const cacheLabel = occurrence.testCachedLocally
      ? "cached locally"
      : occurrence.testCachedRemotely
        ? "remote cache hit"
        : "";
    return (
      <div
        className={`error-context error-context-${severity}`}
        key={`${occurrence.eventTimeUsec}-${occurrence.targetLabel}-${occurrence.testSuite}-${occurrence.testClass}-${occurrence.testName}-${occurrence.testRun}-${occurrence.testShard}-${occurrence.testAttempt}-${index}`}>
        <div className="error-context-heading">
          <div>
            {occurrence.targetLabel &&
              !targetIsIdentity &&
              (workflow ? (
                <span>Step: {occurrence.targetLabel}</span>
              ) : (
                <Link href={targetHref}>{occurrence.targetLabel}</Link>
              ))}
            {identity && targetIsIdentity ? (
              <Link href={targetHref} className="error-context-identity-link">
                {identity}
              </Link>
            ) : (
              identity && <strong>{identity}</strong>
            )}
          </div>
          {(occurrence.fingerprintVersion.startsWith("test") || identity) && (
            <div className="test-context-flags">
              <span className="test-coordinates">
                Run {occurrence.testRun} · shard {occurrence.testShard} · attempt {occurrence.testAttempt}
              </span>
              {(occurrence.testStrategy || cacheLabel) && (
                <span>{[occurrence.testStrategy, cacheLabel].filter(Boolean).join(" · ")}</span>
              )}
            </div>
          )}
        </div>
        {(occurrence.testFailureKind || occurrence.testFailureType) && (
          <div className={`failure-classification failure-classification-${severity}`}>
            {occurrence.testFailureKind && <span>{occurrence.testFailureKind}</span>}
            {occurrence.testFailureType && <code>{occurrence.testFailureType}</code>}
          </div>
        )}
        {(occurrence.actionMnemonic || occurrence.exitCode !== 0) && (
          <div className="error-meta error-context-meta">
            {occurrence.actionMnemonic && <span>Action: {occurrence.actionMnemonic}</span>}
            {occurrence.exitCode !== 0 && <span>Exit code: {occurrence.exitCode}</span>}
          </div>
        )}
        <details className="error-diagnostic" open={openDiagnostic}>
          <summary>Diagnostic</summary>
          <pre>{occurrence.message}</pre>
        </details>
        {occurrence.relatedExecutions.length > 0 && (
          <div className="related-executions">
            <b>Related failed executions</b>
            {occurrence.relatedExecutions.map((execution) => (
              <Link
                key={execution.executionId}
                href={`${invocationHref}?executionId=${encodeURIComponent(execution.executionId)}#execution`}>
                {execution.actionMnemonic || execution.targetLabel || "Execution"}:{" "}
                {execution.statusMessage || `exit ${execution.exitCode}`}
              </Link>
            ))}
          </div>
        )}
      </div>
    );
  }

  private renderInvocation(group: InvocationOccurrences, openFirstDiagnostic: boolean) {
    const first = group.occurrences[0];
    const workflow = isWorkflowOrigin(first.origin);
    const invocationHref = `/invocation/${group.invocationId}`;
    return (
      <article className="error-invocation" key={group.invocationId}>
        <div className="error-invocation-header">
          <div>
            <Link href={invocationHref} className="error-link">
              {workflow ? "Workflow run" : "Build"} {group.invocationId.slice(0, 8)} <ExternalLink size={14} />
            </Link>
            <time title={new Date(Number(group.latestEventTimeUsec) / 1000).toLocaleString()}>
              {relativeTime(group.latestEventTimeUsec)}
            </time>
            <span className={`error-origin-badge error-origin-${workflow ? "workflow" : "bazel"}`}>
              {errorOriginLabel(first.origin)}
            </span>
          </div>
          <span className="context-count">
            {group.occurrences.length} {group.occurrences.length === 1 ? "occurrence" : "occurrences"}
          </span>
        </div>
        <div className="error-meta">
          {first.branchName && (
            <span>
              <GitBranch /> {first.branchName}
            </span>
          )}
          {first.commitSha && (
            <span>
              <GitCommit /> {first.commitSha.slice(0, 12)}
            </span>
          )}
          {first.user && (
            <span>
              <User /> {first.user}
            </span>
          )}
          {first.command && (
            <span>
              <Terminal /> {workflow ? first.invocationPattern || first.command : `bazel ${first.command}`}
            </span>
          )}
          {first.parentRunId && <span>Workflow parent: {first.parentRunId.slice(0, 8)}</span>}
        </div>
        <div className="error-contexts">
          {group.occurrences.map((occurrence, index) =>
            this.renderOccurrenceContext(occurrence, invocationHref, index, openFirstDiagnostic && index === 0)
          )}
        </div>
        <Link href={workflow ? invocationHref : `${invocationHref}#execution`} className="muted-link">
          {workflow ? "Inspect workflow logs" : "Inspect all executions from this invocation"}
        </Link>
      </article>
    );
  }

  render() {
    if (this.props.enabled === false) {
      return (
        <div className="error-tracking-page">
          <header className="error-tracking-header">
            <div>
              <Breadcrumbs>
                <span>Error tracking</span>
              </Breadcrumbs>
              <h1>
                <Bug /> Error tracking
              </h1>
            </div>
          </header>
          <div className="empty-state">
            <AlertTriangle />
            <h2>Error tracking is not enabled</h2>
            <p>Ask your BuildBuddy administrator to enable Error Tracking for this deployment.</p>
          </div>
        </div>
      );
    }
    const selected = this.state.selected;
    const totalOccurrences = this.state.groups.reduce((sum, group) => sum + Number(group.occurrenceCount), 0);
    const frequencyScale = this.state.groups.reduce(
      (maximum, group) =>
        Math.max(maximum, ...group.frequencyBuckets.map((bucket) => Number(bucket.affectedInvocationCount || 0))),
      0
    );
    const selectedIdentity = selected
      ? testIdentity(selected.sampleTestSuite, selected.sampleTestClass, selected.sampleTestName)
      : "";
    const selectedOccurrenceGroups = selected ? groupOccurrencesByInvocation(selected.occurrences) : [];
    const listSort = errorGroupSortParam(this.props.search);
    const originScope = errorOriginScopeParam(this.props.search);
    const workflowScope = originScope === "workflow";
    const affectedLabel = workflowScope ? "Failed workflow runs" : "Affected builds";
    return (
      <div className="error-tracking-page">
        <header className="error-tracking-header">
          <div>
            <Breadcrumbs>
              {selected && (
                <button className="breadcrumb-button" onClick={() => this.showAllErrors()}>
                  Error tracking
                </button>
              )}
              <span>{selected ? issueTitle(selected) : "Error tracking"}</span>
            </Breadcrumbs>
            <h1>
              <Bug /> Error tracking
            </h1>
            <p>Separate recurring Bazel build and test failures from Workflow orchestration failures.</p>
          </div>
          <form
            onSubmit={(e) => {
              e.preventDefault();
              this.applySearch();
            }}>
            <Search size={16} />
            <input
              aria-label="Filter errors"
              placeholder={workflowScope ? "Filter workflow action or diagnostic" : "Filter message, type, or target"}
              value={this.state.query}
              onChange={(e) => this.setState({ query: e.target.value })}
            />
            <FilledButton type="submit" disabled={this.state.loading}>
              Search
            </FilledButton>
          </form>
        </header>
        <nav className="error-origin-tabs" aria-label="Error source">
          <button
            className={workflowScope ? "" : "active"}
            aria-current={workflowScope ? undefined : "page"}
            onClick={() => this.setOriginScope("bazel")}>
            <Bug /> Bazel build &amp; test
          </button>
          <button
            className={workflowScope ? "active" : ""}
            aria-current={workflowScope ? "page" : undefined}
            onClick={() => this.setOriginScope("workflow")}>
            <WorkflowIcon /> Workflow orchestration
          </button>
        </nav>
        {this.state.error && (
          <div className="error-state">
            <AlertTriangle /> {this.state.error}
          </div>
        )}
        {this.state.loading && <div className="loading">Loading errors…</div>}
        {!this.state.loading && !this.state.error && !selected && this.state.groups.length === 0 && (
          <div className="empty-state">
            {workflowScope ? <WorkflowIcon /> : <Bug />}
            <h2>
              {this.appliedQuery()
                ? "No errors match this filter"
                : workflowScope
                  ? "No Workflow orchestration failures in the last 7 days"
                  : "No Bazel failures in the last 7 days"}
            </h2>
            <p>
              {this.appliedQuery()
                ? workflowScope
                  ? "Try a different Workflow action or diagnostic."
                  : "Try a different message, type, or target."
                : workflowScope
                  ? "Failures from the outer BuildBuddy Workflow runner will appear here. Bazel command failures are tracked separately."
                  : "Failed Bazel action, target, test, abort, and build events will appear here."}
            </p>
            {this.appliedQuery() && <FilledButton onClick={() => this.clearSearch()}>Clear filter</FilledButton>}
          </div>
        )}
        {!this.state.loading && !this.state.error && !selected && this.state.groups.length > 0 && (
          <section className="error-groups-section">
            <div className="error-overview">
              <div>
                <Layers3 />
                <div>
                  <b>{this.state.groups.length}</b>
                  <span>Issue groups shown</span>
                </div>
              </div>
              <div>
                {workflowScope ? <WorkflowIcon /> : <Bug />}
                <div>
                  <b>{totalOccurrences}</b>
                  <span>{affectedLabel} shown</span>
                </div>
              </div>
              <div>
                <CalendarClock />
                <div>
                  <b>7 days</b>
                  <span>Current time range</span>
                </div>
              </div>
            </div>
            <div className="error-list-heading">
              <div>
                <h2>{workflowScope ? "Recent Workflow failures" : "Recent Bazel issues"}</h2>
                <p>
                  {errorGroupSortDescription(listSort, workflowScope ? "workflow runs" : "builds")} Counts represent{" "}
                  {workflowScope ? "failed runs" : "affected builds"} within each issue.
                </p>
              </div>
              <div className="error-list-controls">
                {this.appliedQuery() && (
                  <button className="clear-filter-button" onClick={() => this.clearSearch()}>
                    Clear filter
                  </button>
                )}
                <label className="error-sort-control">
                  <span>Sort by</span>
                  <Select
                    className="small-select"
                    aria-label="Sort recent issues"
                    value={listSort}
                    onChange={(event) => this.setSort(event.target.value as ErrorGroupSortParam)}>
                    <Option value="recent">Most recently seen</Option>
                    <Option value="affected">
                      {workflowScope ? "Most affected workflow runs" : "Most affected builds"}
                    </Option>
                    <Option value="frequency">Most frequent recently</Option>
                  </Select>
                </label>
              </div>
            </div>
            <div className="error-groups">
              <div className="error-groups-header">
                <span>Issue</span>
                <span>{affectedLabel}</span>
                <span>
                  Frequency <small>oldest → newest</small>
                </span>
                <span>Last seen</span>
                <span aria-hidden="true" />
              </div>
              {this.state.groups.map((group, index) => (
                <Link
                  className="error-group-row"
                  key={group.fingerprint}
                  href={this.fingerprintHref(group.fingerprint)}
                  aria-labelledby={`error-title-${index}`}
                  aria-describedby={`error-impact-${index} error-frequency-${index} error-latest-${index}`}>
                  <div className="error-group-main">
                    <div className="error-type-row">
                      <span className="error-indicator" aria-hidden="true" />
                      <span className={`error-origin-badge error-origin-${workflowScope ? "workflow" : "bazel"}`}>
                        {errorOriginLabel(group.origin)}
                      </span>
                      <code className="inline-code error-type-code">{group.errorType}</code>
                    </div>
                    <h3 id={`error-title-${index}`}>{issueTitle(group)}</h3>
                    {(workflowScope ||
                      testIdentity(group.sampleTestSuite, group.sampleTestClass, group.sampleTestName)) && (
                      <p>{firstMessageLine(group.sampleMessage) || "No diagnostic supplied"}</p>
                    )}
                    {(group.sampleActionMnemonic || groupRowTarget(group)) && (
                      <div className="error-group-context">
                        {group.sampleActionMnemonic && <span>{group.sampleActionMnemonic}</span>}
                        {groupRowTarget(group) && <span className="error-target-context">{groupRowTarget(group)}</span>}
                      </div>
                    )}
                  </div>
                  <b className="error-count" id={`error-impact-${index}`}>
                    {displayCount(group.occurrenceCount)}
                    <span className="visually-hidden"> {affectedLabel.toLowerCase()}</span>
                  </b>
                  {this.renderFrequency(group, index, frequencyScale, workflowScope ? "workflow runs" : "builds")}
                  <time
                    id={`error-latest-${index}`}
                    title={new Date(Number(group.lastSeenUsec) / 1000).toLocaleString()}>
                    <span className="visually-hidden">Last seen: </span>
                    {relativeTime(group.lastSeenUsec)}
                  </time>
                  <ChevronRight className="error-row-chevron" size={18} aria-hidden="true" />
                </Link>
              ))}
            </div>
            {this.state.nextPageToken && (
              <FilledButton
                disabled={this.state.appending}
                onClick={() => this.fetchGroups("", this.appliedQuery(), this.state.nextPageToken, true)}>
                {this.state.appending ? "Loading…" : "Load more issues"}
              </FilledButton>
            )}
            {this.state.appendError && (
              <div className="error-state">
                <AlertTriangle /> {this.state.appendError} Select load more to retry.
              </div>
            )}
          </section>
        )}
        {!this.state.error && selected && !this.state.loading && (
          <div className="error-detail">
            <div className="error-detail-heading">
              <div className="error-type-row">
                <span className="error-indicator" aria-hidden="true" />
                <span className={`error-origin-badge error-origin-${workflowScope ? "workflow" : "bazel"}`}>
                  {errorOriginLabel(selected.origin)}
                </span>
                <code className="inline-code error-type-code">{selected.errorType}</code>
              </div>
              <h2>{issueTitle(selected)}</h2>
              {(workflowScope || selectedIdentity) && (
                <p className="error-detail-diagnostic">
                  {firstMessageLine(selected.sampleMessage) || "No diagnostic supplied"}
                </p>
              )}
              {(selected.sampleActionMnemonic || selected.sampleTargetLabel) && (
                <div className="error-group-context">
                  {selected.sampleActionMnemonic && <span>{selected.sampleActionMnemonic}</span>}
                  {selected.sampleTargetLabel && (
                    <span className="error-target-context">{selected.sampleTargetLabel}</span>
                  )}
                </div>
              )}
              {(selected.sampleTestFailureKind || selected.sampleTestFailureType) && (
                <div
                  className={`failure-classification failure-classification-${failureSeverity(
                    selected.sampleTestFailureKind,
                    selected.sampleTestFailureType,
                    selected.errorType
                  )}`}>
                  {selected.sampleTestFailureKind && <span>{selected.sampleTestFailureKind}</span>}
                  {selected.sampleTestFailureType && <code>{selected.sampleTestFailureType}</code>}
                </div>
              )}
            </div>
            <section className="error-detail-summary" aria-label="Impact in the last 7 days">
              <div>
                <b>{displayCount(selected.occurrenceCount)}</b>
                <span>{affectedLabel}</span>
              </div>
              <div>
                <b title={new Date(Number(selected.lastSeenUsec) / 1000).toLocaleString()}>
                  {relativeTime(selected.lastSeenUsec)}
                </b>
                <span>Last seen</span>
              </div>
              <div>
                <b title={new Date(Number(selected.firstSeenUsec) / 1000).toLocaleString()}>
                  {relativeTime(selected.firstSeenUsec)}
                </b>
                <span>Earliest in range · last 7 days</span>
              </div>
            </section>
            {this.renderFingerprintTechnicalDetails(selected)}
            <main className="error-detail-main">
              <div className="occurrences-heading">
                <div>
                  <h3>Occurrences</h3>
                  <p>
                    {workflowScope
                      ? "Each occurrence is one matching failure from the outer Workflow orchestration run. Bazel command failures are tracked in the Bazel view."
                      : "Each occurrence is one matching failed action, test case, or test attempt within a Bazel build."}
                  </p>
                </div>
                <span>
                  {selectedOccurrenceGroups.length} {workflowScope ? "workflow runs" : "builds"} ·{" "}
                  {selected.occurrences.length} occurrences shown
                </span>
              </div>
              {selectedOccurrenceGroups.map((group, index) => this.renderInvocation(group, index === 0))}
              {this.state.nextPageToken && (
                <FilledButton
                  disabled={this.state.appending}
                  onClick={() =>
                    this.fetchGroups(selected.fingerprint, this.appliedQuery(), this.state.nextPageToken, true)
                  }>
                  {this.state.appending
                    ? "Loading…"
                    : `Load more ${workflowScope ? "workflow runs" : "affected builds"}`}
                </FilledButton>
              )}
              {this.state.appendError && (
                <div className="error-state">
                  <AlertTriangle /> {this.state.appendError} Select load more to retry.
                </div>
              )}
            </main>
          </div>
        )}
      </div>
    );
  }
}
