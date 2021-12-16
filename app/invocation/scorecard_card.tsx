import React from "react";
import InvocationModel from "./invocation_model";
import { XCircle } from "lucide-react";
import { cache } from "../../proto/cache_ts_proto";
import router from "../router/router";

interface Props {
  model: InvocationModel;
}

interface State {
  resultsLimit: number | null;
}

// How many cache misses to show initially.
const INITIAL_RESULTS_LIMIT = 64;

export default class ScorecardCardComponent extends React.Component<Props, State> {
  state: State = {
    resultsLimit: INITIAL_RESULTS_LIMIT,
  };

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

  render() {
    if (!this.props.model.scoreCard) return null;

    const groups = groupResultsByTargetId(this.props.model.scoreCard.misses);
    const visibleGroups = limitResults(groups, this.state.resultsLimit);
    const hiddenGroupsCount = groups.length - visibleGroups.length;

    return (
      <div className="card scorecard">
        <XCircle className="icon" />
        <div className="content">
          <div className="title">Cache Misses</div>
          <div className="details">
            {visibleGroups.map((group) => (
              <div key={group.targetId}>
                <div className="scorecard-target-name">{group.targetId}</div>
                <div className="scorecard-action-id-list">
                  {group.results.map((result) => (
                    <a
                      key={result.actionId}
                      href={this.getActionUrl(result.actionId)}
                      onClick={this.onClickAction.bind(this)}
                      className="scorecard-action-id">
                      {result.actionId}
                      {result.actionMnemonic ? ` (${result.actionMnemonic})` : ""}
                    </a>
                  ))}
                  {group.hiddenResultsCount ? (
                    <div className="scorecard-hidden-count">
                      {group.hiddenResultsCount} more {group.hiddenResultsCount === 1 ? "cache miss" : "misses"} for
                      this target
                    </div>
                  ) : null}
                </div>
              </div>
            ))}
            {hiddenGroupsCount > 0 ? (
              <>
                <div className="scorecard-hidden-count">
                  {hiddenGroupsCount} more {hiddenGroupsCount === 1 ? "target" : "targets"} with cache misses
                </div>
                <div className="more" onClick={this.onClickShowAll.bind(this)}>
                  See all cache misses
                </div>
              </>
            ) : null}
          </div>
          {this.props.model.scoreCard.misses.length == 0 && <span>No Misses</span>}
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
