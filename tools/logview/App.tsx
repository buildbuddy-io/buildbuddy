import React, { useMemo, useState } from "react";
import { AND, NOT, contains } from "./filter";
import { Line, Severity } from "./logs";
import { severityColor, severityLabel, severityValue } from "./severity";
import colors from "./colors";
import QueryEditor from "./QueryEditor";

const initialData = (window as any)._initialData as {
  logs: Line[];
  query: string;
};

const LOGS = initialData.logs;
const EXECUTION_ID = getQueryAnnotation(initialData.query, "execution");

/**
 * Looks for a query comment of the form `-- @{annotation} {value}`
 * and returns the {value} if it exists.
 */
function getQueryAnnotation(query: string, annotation: string): string {
  const lines = query.split("\n");
  const prefix = `-- @${annotation} `;
  for (const line of lines) {
    if (line.startsWith(prefix)) {
      return line.substring(prefix.length).trim();
    }
  }
  return "";
}

const DURATION_THRESHOLD_SECONDS = 10;

const MIN_SEVERITY: Severity = "DEBUG";

const EXCLUDE = [
  "Enqueue task reservations took",
  "execution_server.go",
  "StatsRecorder",
  "COWStore conversion",
  "cancelling node registration",
  "Distributed cache",

  // Task queue
  "Added task task_id",
  "code = NotFound desc = task already claimed",

  // Runner pool debugging
  "Created new firecracker runner",
  "Reusing existing runner",
  "Preparing runner",
  "Looking for match",

  // Execution stages
  "Getting a runner for task",
  "Downloading inputs",
  "Transitioning to EXECUTING",
  "Uploading outputs",

  // Migration
  "Migration copying",

  // Scheduler debugging
  "Pinging scheduler",
  "Ping done",
].map((text) => NOT(contains(text)));

function replace(p: RegExp | string, replacement: string) {
  return (m: string) => m.replace(p, replacement);
}

const TRANSFORM_MESSAGE = [
  // replace(/Added task .*/, 'Added task [...]'),
  replace(/Enqueueing task reservations.*/, "Enqueueing task reservations [...]"),
  replace(/.*?finished with non-zero exit code.*/, "[...] finished (exit != 0) [...]"),
];

// if (EXECUTION_ID) {
//   TRANSFORM_MESSAGE.push(replace(new RegExp(EXECUTION_ID, "g"), "$TASK"));
// }

type Tag = {
  name: string;
  match: RegExp;
  color: string;
};

const TAGS: Tag[] = [
  {
    match: /LeaseTask attempt \(reconnect=true\)/,
    name: "Reconnecting lease",
    color: colors.deepPurple700,
  },
  {
    match: /Error running task/,
    name: "Task error",
    color: colors.red800,
  },
  {
    match: /Caught interrupt signal.*/,
    name: "Shutdown signal",
    color: colors.red800,
  },
  {
    match: /Worker leased task.*/,
    name: "Acquired lease",
    color: colors.blue700,
  },
  {
    match: /Successfully re-enqueued/,
    name: "Re-enqueued task",
    color: colors.pink500,
  },
];

function jsonStringify(e: Line): string {
  return (e._jsonStringifyCache ??= JSON.stringify(e));
}

function getTags(e: Line) {
  const out: Tag[] = [];
  for (const tag of TAGS) {
    if (tag.match.test(jsonStringify(e))) {
      out.push(tag);
    }
  }
  return out;
}

const IS_MATCH = AND(...EXCLUDE);

function getMessage(e: Line) {
  let m = e.textPayload || e.jsonPayload?.message || "";
  for (const t of TRANSFORM_MESSAGE) {
    m = t(m);
  }
  return m;
}

function getTimestamp(e: Line) {
  return e.jsonPayload?.timestamp || e.timestamp || e.receiveTimestamp;
}

function getPodNameAbbrev(pod: string): string {
  if (pod.startsWith("buildbuddy-app-")) {
    pod = pod.substring("buildbuddy-app-".length);
    return "bb:" + pod;
  }
  if (pod.startsWith("executor-")) {
    const workflows = pod.includes("-workflows-");
    pod = pod.replace(/executor-(workflows-)?[^-]+-(.+)$/, "$2");
    return workflows ? "wf:" + pod : "ex:" + pod;
  }
  return pod;
}

function parseTimestamp(timestamp: string) {
  const d = new Date(timestamp);
  return d.getTime() / 1e3;
}

function durationBetween(t1: string, t2: string) {
  if (!t1 || !t2) return NaN;
  return parseTimestamp(t2) - parseTimestamp(t1);
}

type Data = {
  logs: Line[];
  podNameByExecutorId: Record<string, string>;
  pods: string[];
  containerNames: string[];
};

function getData(): Data {
  let logs = structuredClone(LOGS);

  for (const e of logs) {
    e.computed = {
      podShortName: e.resource?.labels?.pod_name ? getPodNameAbbrev(e.resource.labels.pod_name) : "",
      tags: getTags(e).map((t) => t.name),
    };
    e._jsonStringifyCache = undefined;
  }

  logs.sort((a, b) => getTimestamp(a).localeCompare(getTimestamp(b)));

  logs = logs.filter((e) => severityValue(e.severity) >= severityValue(MIN_SEVERITY));
  logs = logs.filter((e) => e.resource?.labels?.pod_name);
  logs = logs.filter((e) => IS_MATCH(e));

  let pods = [...new Set(LOGS.map((e) => e.resource?.labels?.pod_name))].sort();
  if (EXECUTION_ID) {
    pods = pods.filter((pod) =>
      logs.some((e) => e.resource?.labels?.pod_name === pod && e.jsonPayload?.execution_id?.includes(EXECUTION_ID))
    );
  }
  logs = logs.filter((e) => pods.includes(e.resource.labels.pod_name));

  const executorIds: string[] = [
    ...new Set(logs.map((e) => e.jsonPayload?.executor_id).filter((id) => id) as string[]),
  ];
  const podNameByExecutorId: Record<string, string> = {};
  for (const id of executorIds) {
    const e = logs.find((e) => e.jsonPayload?.executor_id === id && e.resource.labels.pod_name);
    if (e) podNameByExecutorId[id] = e.resource.labels.pod_name;
  }

  const containerNames = [...new Set(logs.map((e) => e.resource?.labels?.container_name).filter((x) => x))].sort();

  return {
    logs,
    pods,
    containerNames,
    podNameByExecutorId,
  };
}

function filterData(data: Data, filter: string): Data {
  let logs = data.logs;
  if (filter) {
    logs = data.logs.filter((e) => jsonStringify(e).toLocaleLowerCase().includes(filter.toLocaleLowerCase()));
  }
  return { ...data, logs };
}

const POD_COLORS = [
  colors.red600,
  colors.blue600,
  colors.green600,
  colors.amber600,
  colors.brown600,
  colors.indigo600,
  colors.cyan600,
  colors.lime600,
  colors.deepOrange600,
];

const getPodColor = (podIndex: number) => {
  return POD_COLORS[podIndex % POD_COLORS.length];
};

function renderIndent(data: Data, e: Line, indent: IndentBy) {
  if (indent === "Off") {
    return null;
  }

  const indentLevel =
    indent === "Pod"
      ? data.pods.indexOf(e.resource?.labels?.pod_name)
      : data.containerNames.indexOf(e.resource?.labels?.container_name);
  const out = [];
  for (let i = 0; i < indentLevel; i++) {
    out.push(
      <div
        key={i}
        className="log-margin"
        style={{
          width: indent === "Pod" ? "24px" : "32px",
          borderLeft: indent === "Pod" ? `1px solid ${getPodColor(i)}` : `1px solid ` + colors.grey500,
          opacity: "0.3",
        }}
      />
    );
  }
  return out;
}

function toLocaleTimeStringMillis(date: Date) {
  let localeTimeString = date.toLocaleTimeString();
  if (localeTimeString.endsWith(" AM") || localeTimeString.endsWith(" PM")) {
    const parts = localeTimeString.split(" ");
    const ms = "." + ("000" + date.getMilliseconds()).slice(-3);
    localeTimeString = parts.slice(0, parts.length - 1) + ms + " " + parts[parts.length - 1];
  }
  return localeTimeString;
}

function renderLineTimestamp(e: Line) {
  const timestamp = getTimestamp(e);
  const date = new Date(timestamp);
  const localeTimeString = toLocaleTimeStringMillis(date);
  return (
    <div
      className="mono"
      style={{
        width: "96px",
        flexShrink: 0,
        background: colors.blue50,
        display: "flex",
        alignItems: "center",
        paddingLeft: "8px",
      }}>
      {localeTimeString}
    </div>
  );
}

const renderPodName = (pods: string[], pod: string, focusMode: PodFocusMode, focusPod: (name: string) => any) => {
  return (
    <span
      className="pod-name"
      style={{
        cursor: "pointer",
        background: getPodColor(pods.indexOf(pod)) + "33",
      }}
      onPointerEnter={focusMode === "Hover" ? () => focusPod(pod) : undefined}
      onPointerLeave={focusMode === "Hover" ? () => focusPod("") : undefined}
      onClick={
        focusMode === "Click"
          ? (e) => {
              e.stopPropagation();
              focusPod(pod);
            }
          : undefined
      }>
      <b>{getPodNameAbbrev(pod)} </b>
    </span>
  );
};

function decorate(parts: React.ReactNode[], find: string, replace: () => React.ReactNode): React.ReactNode[] {
  parts = [...parts];
  let i = 0;
  while (i < parts.length) {
    const part = parts[i];
    if (typeof part !== "string") {
      i++;
      continue;
    }
    const idx = part.indexOf(find);
    if (idx < 0) {
      i++;
      continue;
    }
    parts = [
      ...parts.slice(0, i),
      part.substring(0, idx), // i
      replace(), // i + 1
      part.substring(idx + find.length), // i + 2
      ...parts.slice(i + 1),
    ];
    i += 2;
  }
  return parts;
}

const renderMessage = (data: Data, e: Line, focusMode: PodFocusMode, focusPod: (v: string) => any) => {
  const { podNameByExecutorId } = data;

  let parts: Array<React.ReactNode> = [getMessage(e)];
  // Substitute executor_id => fancy pod_name
  for (const [executorId, podName] of Object.entries(podNameByExecutorId)) {
    parts = decorate(parts, executorId, () => renderPodName(data.pods, podName, focusMode, focusPod));
  }

  return [
    ...getTags(e).map((tag) => (
      <>
        <b style={{ color: tag.color }}>{tag.name}</b>{" "}
      </>
    )),
    ...parts,
  ];
};

function RadioSetting<T extends string>({
  name,
  options,
  value,
  onChange,
}: {
  name: string;
  options: T[];
  value: T;
  onChange: (value: T) => any;
}) {
  return (
    <div className="setting radio-setting">
      <div className="radio-setting-title">{name}:</div>
      {options.map((label, i) => (
        <label key={i} onClick={() => onChange(label)}>
          <input type="radio" name={name} checked={label === value} /> {label}
        </label>
      ))}
    </div>
  );
}

function CheckBoxSetting({ name, value, onChange }: { name: string; value: boolean; onChange: (v: boolean) => any }) {
  return (
    <div className="setting checkbox-setting">
      <label>
        <input type="checkbox" checked={value} onClick={() => onChange(!value)} /> {name}
      </label>
    </div>
  );
}

type IndentBy = "Off" | "Workload" | "Pod";
type PodFocusMode = "Hover" | "Click";

export default function App() {
  const [filter, setFilter] = useState("");
  const [query, setQuery] = useState(initialData.query);

  const originalData = useMemo(() => getData(), []);
  const data = useMemo(() => filterData(originalData, filter), [originalData, filter]);

  const { logs, pods } = data;

  // Set up fancy executor_id => pod_name div

  const [focusedPod, setFocusedPod] = useState("");
  const [indent, setIndent] = useState<IndentBy>("Pod");
  const [podFocusMode, setPodFocusMode] = useState<PodFocusMode>("Click");
  const [showQuietPeriods, setShowQuietPeriods] = useState(true);
  const [wrapLines, setWrapLines] = useState(false);

  return (
    <>
      <div className="query-editor-bar">
        <QueryEditor value={query} onChange={setQuery} />
      </div>
      <div className="top-bar">
        <div className="search">
          <input
            className="search-input"
            type="text"
            placeholder="Filter..."
            value={filter}
            onChange={(e) => setFilter(e.target.value)}
          />
        </div>
        <div className="settings">
          <RadioSetting<IndentBy>
            name="Indent"
            options={["Off", "Workload", "Pod"]}
            value={indent}
            onChange={setIndent}
          />
          <RadioSetting<PodFocusMode>
            name="Focus pod"
            options={["Hover", "Click"]}
            value={podFocusMode}
            onChange={setPodFocusMode}
          />
          <CheckBoxSetting name="Wrap text" value={wrapLines} onChange={setWrapLines} />
          <CheckBoxSetting name="Show quiet periods" value={showQuietPeriods} onChange={setShowQuietPeriods} />
        </div>
      </div>
      <div className="grid" onClick={() => setFocusedPod("")}>
        {logs.map(function (e: Line, i: number) {
          const duration = i === 0 ? 0 : durationBetween(getTimestamp(logs[i - 1]), getTimestamp(e));
          return (
            <>
              {(i === 0 || duration > DURATION_THRESHOLD_SECONDS) && showQuietPeriods && (
                <div className="duration-row mono" style={{ padding: "2px 8px" }}>
                  <b>{toLocaleTimeStringMillis(new Date(e.timestamp || ""))}</b>{" "}
                  {i > 0 && (
                    <>
                      - <b>{duration.toFixed(2)}s</b> later:
                    </>
                  )}
                </div>
              )}
              <div className="log-row" key={e.insertId}>
                {renderLineTimestamp(e)}
                {renderIndent(data, e, indent)}
                <div
                  className="log-line"
                  style={{
                    background: e.severity === "INFO" || e.severity === "DEBUG" ? "" : severityColor(e.severity) + "20",
                    borderLeft: `3px solid ${getPodColor(pods.indexOf(e.resource?.labels?.pod_name))}`,
                    opacity: focusedPod && e.resource?.labels?.pod_name !== focusedPod ? "0.25" : "1",
                    transition: "120ms ease-out",
                  }}>
                  {renderPodName(data.pods, e.resource?.labels?.pod_name, podFocusMode, setFocusedPod)}
                  {e.severity !== "INFO" && (
                    <b
                      className="log-level"
                      style={{
                        background: severityColor(e.severity) + "30",
                        color: severityColor(e.severity),
                      }}>
                      {severityLabel(e.severity)}
                    </b>
                  )}
                  <div
                    className="log-message"
                    style={{
                      overflow: wrapLines ? undefined : "hidden",
                      textOverflow: wrapLines ? undefined : "ellipsis",
                      whiteSpace: wrapLines ? undefined : "nowrap",
                      flexGrow: 1,
                      flexShrink: 1,
                      width: 0,
                    }}>
                    {renderMessage(data, e, podFocusMode, setFocusedPod)}
                  </div>
                </div>
              </div>
            </>
          );
        })}
      </div>
    </>
  );
}
