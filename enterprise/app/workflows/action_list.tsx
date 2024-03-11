import React from "react";
import router from "../../../app/router/router";
import { invocation_status } from "../../../proto/invocation_status_ts_proto";
import { workflow } from "../../../proto/workflow_ts_proto";
import format from "../../../app/format/format";
import { durationToMillis } from "../../../app/util/proto";
import { GitCommit } from "lucide-react";
import Link from "../../../app/components/link/link";
import { MouseCoords, Tooltip, pinBottomLeftOffsetFromMouse } from "../../../app/components/tooltip/tooltip";

export type ActionListComponentProps = {
  repoUrl: string;
  history: workflow.ActionHistory[];
};

type RunStatus = "success" | "failure" | "in-progress" | "disconnected" | "unknown";

function findLatestCompletedRun(actionHistory: workflow.ActionHistory): workflow.ActionHistory.Entry | undefined {
  return (
    actionHistory.entries.find((v) => v.status === invocation_status.InvocationStatus.COMPLETE_INVOCATION_STATUS) ??
    undefined
  );
}

function entryToStatus(entry?: workflow.ActionHistory.Entry): RunStatus {
  if (!entry) {
    return "unknown";
  }
  if (entry.status === invocation_status.InvocationStatus.COMPLETE_INVOCATION_STATUS) {
    return entry.success ? "success" : "failure";
  } else if (entry.status === invocation_status.InvocationStatus.PARTIAL_INVOCATION_STATUS) {
    return "in-progress";
  } else if (entry.status === invocation_status.InvocationStatus.DISCONNECTED_INVOCATION_STATUS) {
    return "disconnected";
  }
  return "unknown";
}

function getCardClass(status: RunStatus): string {
  if (status === "unknown" || status === "disconnected") {
    return "neutral";
  }
  return status;
}

function getRunStatusText(status: RunStatus): string {
  switch (status) {
    case "disconnected":
      return "Disconnected";
    case "failure":
      return "Failed";
    case "in-progress":
      return "In Progress";
    case "success":
      return "Succeeded";
    case "unknown":
    default:
      return "Unknown";
  }
}

const MINIMUM_BAR_HEIGHT = 12;
const MAXIMUM_BAR_HEIGHT = 36;

function durationToBarHeight(duration: number, maxDuration: number): string {
  if (maxDuration === 0) {
    return MAXIMUM_BAR_HEIGHT + "px";
  }
  return Math.max(MINIMUM_BAR_HEIGHT, (duration / maxDuration) * MAXIMUM_BAR_HEIGHT) + "px";
}

function renderTooltipContent(
  actionHistory: workflow.ActionHistory,
  lookup: Map<string, workflow.ActionHistory.Entry>,
  c: MouseCoords
): JSX.Element | null {
  const elem = document.elementFromPoint(c.clientX, c.clientY);
  if (!elem) {
    return null;
  }
  const invocationId = elem.getAttribute("data-invocation");
  if (!invocationId) {
    return null;
  }
  if (invocationId === "no-data") {
    return <div className="workflows-page-action-history-tooltip">No more runs in the last 7 days.</div>;
  }
  const entry = lookup.get(invocationId);

  if (entry) {
    return (
      <div className="workflows-page-action-history-tooltip">
        <div>
          <strong>Invocation ID: </strong>
          {entry.invocationId}
        </div>
        <div>
          <strong>Commit: </strong>
          <span title={entry.commitSha}>{format.formatCommitHash(entry.commitSha)}</span>
        </div>
        <div>
          <strong>Status: </strong>
          {getRunStatusText(entryToStatus(entry))}
        </div>
        <div>
          <strong>Run started: </strong>
          {format.formatTimestampUsec(entry.createdAtUsec)}
        </div>
        <div>
          <strong>Duration: </strong>
          {format.durationMillis(durationToMillis(entry.duration ?? {}))}
        </div>
      </div>
    );
  } else {
    return <div className="workflows-page-action-history-tooltip">Failed to load run data.</div>;
  }
}

function renderHistoryChart(actionHistory: workflow.ActionHistory): JSX.Element {
  const branchName = findLatestCompletedRun(actionHistory)?.branchName ?? "main";
  const emptyEntries = Math.max(0, 30 - actionHistory.entries.length);

  let maxDuration = 0;
  const invocationLookupMap = new Map<string, workflow.ActionHistory.Entry>();
  actionHistory.entries.forEach((entry) => {
    invocationLookupMap.set(entry.invocationId, entry);
    maxDuration = Math.max(maxDuration, durationToMillis(entry.duration ?? {}));
  });
  return (
    <div className="action-history-chart">
      <div className="action-history-explainer-text">{branchName}</div>
      <Tooltip
        pin={pinBottomLeftOffsetFromMouse}
        renderContent={(c: MouseCoords) => renderTooltipContent(actionHistory, invocationLookupMap, c)}>
        {actionHistory.entries.map((entry) => {
          return (
            <div data-invocation={entry.invocationId} className="action-history-bar-wrapper">
              <div
                data-invocation={entry.invocationId}
                className={"action-history-bar " + getCardClass(entryToStatus(entry))}
                style={{ height: durationToBarHeight(durationToMillis(entry.duration ?? {}), maxDuration) }}></div>
            </div>
          );
        })}
        {[...Array(emptyEntries)].map((entry) => (
          <div data-invocation="no-data" className="action-history-bar-wrapper">
            <div
              data-invocation="no-data"
              className="action-history-bar no-data"
              style={{ height: durationToBarHeight(0, maxDuration) }}></div>
          </div>
        ))}
      </Tooltip>
    </div>
  );
}

export default class ActionListComponent extends React.Component<ActionListComponentProps> {
  render() {
    return (
      <>
        {this.props.history.map((h) => {
          const latestCompletedRun = findLatestCompletedRun(h);
          const latestRunStatus = entryToStatus(latestCompletedRun);
          return (
            <Link resetFilters={true} href={router.getWorkflowActionHistoryUrl(this.props.repoUrl, h.actionName)}>
              <div className={"card action-history-card card-" + getCardClass(latestRunStatus)}>
                <div className="title-section">
                  <div className="title">{h.actionName}</div>
                  {latestCompletedRun && (
                    <div className="subtitle">
                      {getRunStatusText(latestRunStatus)} at <GitCommit className="icon inline-icon" />{" "}
                      {format.formatCommitHash(latestCompletedRun.commitSha)}
                    </div>
                  )}
                  {!latestCompletedRun && <div className="subtitle">No runs on main in last 7 days.</div>}
                </div>
                <div className="action-history-section">{renderHistoryChart(h)}</div>
                <div className="action-history-section action-stats-section">
                  <div className="action-history-explainer-text">Last 7 days</div>
                  <div className="action-stat">
                    <div className="title">{format.count(h.summary?.totalRuns ?? 0)}</div>
                    <div className="subtitle">Action Runs</div>
                  </div>
                  <div className="action-stat">
                    <div className="title">
                      {h.summary ? format.durationMillis(durationToMillis(h.summary.averageDuration ?? {})) : "--"}
                    </div>
                    <div className="subtitle">Avg. Duration</div>
                  </div>
                  <div className="action-stat">
                    <div className="title">
                      {h.summary
                        ? format.percent(+(h.summary?.successfulRuns ?? 0) / +(h.summary?.totalRuns ?? 1)) + "%"
                        : "--"}
                    </div>
                    <div className="subtitle">Success Rate</div>
                  </div>
                </div>
              </div>
            </Link>
          );
        })}
      </>
    );
  }
}
