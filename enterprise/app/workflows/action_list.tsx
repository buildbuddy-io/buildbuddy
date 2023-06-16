import React from "react";
import router from "../../../app/router/router";
import { invocation_status } from "../../../proto/invocation_status_ts_proto";
import { workflow } from "../../../proto/workflow_ts_proto";
import format from "../../../app/format/format";
import { durationToMillis } from "../../../app/util/proto";
import { GitCommit } from "lucide-react";

export type ActionListComponentProps = {
  history: workflow.ActionHistory[];
};

type RunStatus = "success" | "failure" | "in-progress" | "disconnected" | "unknown";

function findLatestCompletedRun(
  actionHistory: workflow.ActionHistory
): workflow.ActionHistory.ActionHistoryEntry | undefined {
  return (
    actionHistory.entries.find((v) => v.status === invocation_status.InvocationStatus.COMPLETE_INVOCATION_STATUS) ??
    undefined
  );
}

function entryToStatus(entry?: workflow.ActionHistoryEntry): RunStatus {
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
const MAXIMUM_BAR_HEIGHT = 35;

function durationToBarHeight(duration: number, maxDuration: number): string {
  return Math.max(MINIMUM_BAR_HEIGHT, (duration / maxDuration) * MAXIMUM_BAR_HEIGHT) + "px";
}

function renderHistoryChart(actionHistory: workflow.ActionHistory): JSX.Element {
  const maxDuration = actionHistory.entries.reduce((max, entry) => Math.max(max, durationToMillis(entry.duration)), 0);
  const emptyEntries = Math.max(0, 30 - actionHistory.entries.length);
  return (
    <div className="action-history-chart">
      <div>
        {actionHistory.entries.map((entry) => {
          return (
            <div
              className={"action-history-bar " + getCardClass(entryToStatus(entry))}
              style={{ height: durationToBarHeight(durationToMillis(entry.duration), maxDuration) }}></div>
          );
        })}
        {[...Array(emptyEntries)].map((entry) => (
          <div className="action-history-bar"></div>
        ))}
      </div>
      <div className="action-history-chart-axis"></div>
    </div>
  );
}

export default class ActionListComponent extends React.Component<ActionListComponentProps> {
  componentDidMount() {}

  render() {
    return (
      <div className="container">
        {this.props.history.map((h) => {
          const latestCompletedRun = findLatestCompletedRun(h);
          const latestRunStatus = entryToStatus(latestCompletedRun);
          return (
            <div className={"card action-history-card card-" + getCardClass(latestRunStatus)}>
              <div>
                <div className="title">{h.actionName}</div>
                <div className="subtitle">
                  {getRunStatusText(latestRunStatus)} at <GitCommit className="icon inline-icon" />{" "}
                  {format.formatCommitHash(latestCompletedRun.commitSha)}
                </div>
              </div>
              <div className="action-history-chart">
                <div className="action-history-explainer-text">{/* XXX */}master</div>
                {renderHistoryChart(h)}
              </div>
              <div className="action-history-stats">
                <div className="action-history-explainer-text">Last 7 days</div>
                <div className="action-stat">
                  <div className="title">{format.count(h.summary?.totalRuns ?? 0)}</div>
                  <div className="subtitle">Action Runs</div>
                </div>
                <div className="action-stat">
                  <div className="title">
                    {h.summary ? format.durationMillis(durationToMillis(h.summary.averageDuration)) : "--"}
                  </div>
                  <div className="subtitle">Avg. Duration</div>
                </div>
                <div className="action-stat">
                  <div className="title">
                    {h.summary ? format.percent(h.summary.successfulRuns / (h.summary.totalRuns || 1)) + "%" : "--"}
                  </div>
                  <div className="subtitle">Success Rate</div>
                </div>
              </div>
            </div>
          );
        })}
      </div>
    );
  }
}
