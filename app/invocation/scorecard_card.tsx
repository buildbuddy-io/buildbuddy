import React from "react";
import InvocationModel from "./invocation_model";
import { X, ArrowUp, ArrowDown, ArrowLeftRight, ChevronRight, Check, Upload } from "lucide-react";
import { cache } from "../../proto/cache_ts_proto";
import router from "../router/router";
import rpc_service from "../service/rpc_service";
import DigestComponent from "../components/digest/digest";
import Link from "../components/link/link";
import { durationToMillis, timestampToDate } from "../util/proto";

interface Props {
  model: InvocationModel;
}

interface State {
  resultsLimit: number | null;
  digestContents: Record<string, string>;
}

// How many cache misses to show initially.
const INITIAL_RESULTS_LIMIT = 64;

export default class ScorecardCardComponent extends React.Component<Props, State> {
  state: State = {
    resultsLimit: INITIAL_RESULTS_LIMIT,
    digestContents: {},
  };

  componentDidMount() {
    this.fetchDigestContents(); // DO NOT SUBMIT
  }

  getActionUrl(digestHash: string) {
    return `/invocation/${this.props.model.getId()}?actionDigest=${digestHash}#action`;
  }

  onClickShowAll() {
    this.setState({ resultsLimit: null });
  }

  onClickAction(e: React.MouseEvent) {
    e.preventDefault();
    const url = (e.target as HTMLAnchorElement).getAttribute("href");
    router.navigateTo(url);
  }

  fetchDigestContents() {
    const instanceName = this.props.model.optionsMap.get("remote_instance_name");

    for (const result of this.props.model.scoreCard.results) {
      const url = [
        `bytestream://localhost:1985`,
        ...(instanceName ? [instanceName] : []),
        "blobs",
        result.digest.hash,
        String(result.digest.sizeBytes),
      ].join("/");
      rpc_service.fetchBytestreamFile(url, this.props.model.getId(), "text").then((contents: string) =>
        this.setState({
          digestContents: {
            ...this.state.digestContents,
            [result.digest.hash]: (contents?.substring(0, 24) || "") + ((contents?.length || 0) > 24 ? "..." : ""),
          },
        })
      );
    }
  }

  renderWaterfallBar(result: cache.ScoreCard.IResult, timelineStartTimeMillis: number, timelineDurationMillis: number) {
    const resultStartTimeMillis = timestampToDate(result.startTime).getTime();
    const resultDurationMillis = durationToMillis(result.duration);

    const startWeight = (resultStartTimeMillis - timelineStartTimeMillis) / timelineDurationMillis;
    const eventWeight = resultDurationMillis / timelineDurationMillis;
    const afterWeight = 1 - startWeight - eventWeight;

    return (
      <div className="waterfall-column">
        <div style={{ flexGrow: startWeight }} />
        <div style={{ flexGrow: eventWeight }} className="waterfall-bar" />
        <div style={{ flexGrow: afterWeight }} />
      </div>
    );
  }

  getStartTimestampAndDurationMillis(): [number, number] {
    const startTimesMillis = this.props.model.scoreCard.results.map((result) =>
      timestampToDate(result.startTime).getTime()
    );
    const endTimesMillis = this.props.model.scoreCard.results.map(
      (result) => timestampToDate(result.startTime).getTime() + durationToMillis(result.duration)
    );
    const earliestStartTimeMillis = startTimesMillis.reduce((acc, cur) => Math.min(acc, cur), startTimesMillis[0] || 0);
    const invocationStartTimeMillis = this.props.model.getStartTimeDate().getTime();
    console.log({ invocationStartTimeMillis });
    const startTimeMillis = Math.min(earliestStartTimeMillis, invocationStartTimeMillis);

    const latestEndTimeMillis = endTimesMillis.reduce((acc, cur) => Math.max(acc, cur), earliestStartTimeMillis);
    const invocationEndTimeMillis = this.props.model.getEndTimeDate().getTime();
    const endTimeMillis = Math.max(latestEndTimeMillis, invocationEndTimeMillis);

    const durationMillis = endTimeMillis - startTimeMillis;

    return [startTimeMillis, durationMillis];
  }

  render() {
    if (!this.props.model.scoreCard) return null;

    const groups = groupResultsByTargetId(this.props.model.scoreCard.misses);
    const visibleGroups = limitResults(groups, this.state.resultsLimit);
    const hiddenGroupsCount = groups.length - visibleGroups.length;

    const groupsV2 = groupResultsByActionId(this.props.model.scoreCard.results);

    const [startTimeMillis, durationMillis] = this.getStartTimestampAndDurationMillis();

    return (
      <div className="card scorecard">
        <div className="content">
          <div className="title">
            <ArrowLeftRight className="icon" />
            <span>Cache requests</span>
          </div>
          <div className="details">
            {groupsV2.map((group) => (
              <div className="group">
                <div className="group-title action-id row">
                  {isDigest(group.actionId) ? (
                    <Link className="action-id" href={this.getActionUrl(group.results[0]?.actionId)}>
                      <DigestComponent
                        hashWidth="168px"
                        digest={{ hash: group.actionId, sizeBytes: null }}
                        expandOnHover={false}
                      />
                    </Link>
                  ) : (
                    <span className="action-id">{group.actionId}</span>
                  )}
                  <div className="row action-label">
                    <span>{group.results[0]?.targetId}</span>
                    {group.results[0]?.actionMnemonic && (
                      <>
                        <ChevronRight className="icon chevron" />
                        <span>{group.results[0]?.actionMnemonic}</span>
                      </>
                    )}
                  </div>
                </div>
                <div className="group-contents results-list column">
                  {group.results.map((result) => (
                    <div className="row">
                      <div>
                        <DigestComponent
                          hashWidth="96px"
                          sizeWidth="72px"
                          digest={result.digest}
                          expandOnHover={false}
                        />
                      </div>
                      <div className="cache-type-column">{cacheTypeLabel(result.cacheType)}</div>
                      <div className="status-column">{statusLabel(result)}</div>
                      {this.renderWaterfallBar(result, startTimeMillis, durationMillis)}
                      {/* <td className="timestamp-column"></td>
                          <td className="duration-column"></td> */}
                      {/* <td>
                            <code className="inline-code">{this.state.digestContents[result.digest.hash]}</code>
                          </td> */}
                    </div>
                  ))}
                </div>
              </div>
            ))}
          </div>
        </div>
      </div>
    );
  }
}

type ResultGroup = {
  targetId: string;
  results: cache.ScoreCard.IResult[];
  hiddenResultsCount?: number;
};

function groupResultsByTargetId(results: cache.ScoreCard.IResult[]): ResultGroup[] {
  const resultsByTarget = new Map<string, cache.ScoreCard.IResult[]>();
  for (const result of results) {
    const resultsForTarget = resultsByTarget.get(result.targetId) || [];
    resultsForTarget.push(result);
    resultsByTarget.set(result.targetId, resultsForTarget);
  }
  return [...resultsByTarget.entries()]
    .map(([targetId, results]) => ({ targetId, results }))
    .sort((a, b) => a.targetId.localeCompare(b.targetId));
}

function limitResults(groups: ResultGroup[], limit: number | null): ResultGroup[] {
  if (limit === null) {
    return groups;
  }
  const limitedGroups = [];
  let remaining = limit;
  for (let group of groups) {
    if (remaining === 0) break;

    group = { ...group }; // shallow clone
    limitedGroups.push(group);
    if (group.results.length > remaining) {
      group.hiddenResultsCount = group.results.length - remaining;
      group.results = group.results.slice(0, remaining);
    }
    remaining -= group.results.length;
  }
  return limitedGroups;
}

function cacheTypeLabel(cacheType: cache.CacheType): React.ReactNode {
  switch (cacheType) {
    case cache.CacheType.CAS:
      return "File";
    case cache.CacheType.AC:
      return "Action";
    default:
      return "";
  }
}

function statusLabel(result: cache.ScoreCard.IResult): React.ReactNode {
  if (
    result.requestType === cache.RequestType.READ &&
    result.cacheType === cache.CacheType.AC &&
    result.status.code !== 0 /*=OK*/
  ) {
    return (
      <>
        <X className="icon red" />
        <span>Miss</span>
      </>
    );
  }
  if (result.requestType === cache.RequestType.READ) {
    return (
      <>
        <ArrowDown className="icon green" />
        <span>Read</span>
      </>
    );
  }
  if (result.requestType === cache.RequestType.WRITE) {
    return (
      <>
        <ArrowUp className="icon red" />
        <span>Write</span>
      </>
    );
  }
  return "";
}

type ActionResults = {
  actionId: string;
  results: cache.ScoreCard.IResult[];
};

// TODO: Iterate through server response and group by action ID.
// Server always sorts

function groupResultsByActionId(results: cache.ScoreCard.IResult[]): ActionResults[] {
  return groupByKey(results, "actionId", compareStrings).map((group) => ({
    actionId: group.key,
    results: group.values,
  }));
}

type Group<T, U> = {
  key: U;
  values: T[];
};

type CompareFn<T> = (a: T, b: T) => number;

const compareStrings: CompareFn<string> = (a, b) => a.localeCompare(b);

function compareComputedNumber<T>(fn: (value: T) => number): CompareFn<T> {
  return (a, b) => fn(a) - fn(b);
}

function groupByKey<K extends string, T extends Pick<T, K>>(
  list: T[],
  key: K,
  sortCompareFn?: CompareFn<T[K]>
): Group<T, T[K]>[] {
  const groupsByKeyValue: Record<T[K], T[]> = {};
  for (const item of list) {
    const keyValue = item[key];
    let values = groupsByKeyValue[keyValue];
    if (!values) {
      values = [];
      groupsByKeyValue[keyValue] = values;
    }
    values.push(item);
  }

  const out: Group<T, T[K]>[] = [];
  for (const [key, values] of Object.entries(groupsByKeyValue) as [T[K], T[]][]) {
    out.push({ key, values });
  }

  if (sortCompareFn) {
    out.sort((a, b) => sortCompareFn(a.key, b.key));
  }

  return out;
}

function isDigest(actionId: string) {
  return actionId.length === 64;
}
