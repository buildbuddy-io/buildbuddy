import React from "react";
import { List, Cloud } from "lucide-react";

import * as format from "../../../app/format/format";
import { stats } from "../../../proto/stats_ts_proto";
import {
  isAnyNonDateFilterSet,
  formatDateRangeFromSearchParams,
  formatPreviousDateRangeFromSearchParams,
} from "../filter/filter_util";

interface Props {
  search: URLSearchParams;
  currentPeriod: stats.Summary;
  previousPeriod?: stats.Summary;
}

function renderDelta(currentValue: number, previousValue: number): JSX.Element {
  const rawPercentage = (currentValue - previousValue) / previousValue;
  const percentClass = rawPercentage > 0 ? "up" : "down";
  return (
    <span className={"trend-change " + percentClass}>
      {percentClass} {format.percent(Math.abs(rawPercentage))}%
    </span>
  );
}

export default class TrendsSummaryCard extends React.Component<Props> {
  renderChangeText(currentValue: number, previousValue: number): JSX.Element {
    if (previousValue == 0) {
      return <div className="trend-sub-item">No data available for the previous period.</div>;
    }
    const rawPercentage = (currentValue - previousValue) / previousValue;
    if (Math.abs(rawPercentage) < 0.01) {
      return <div className="trend-sub-item">That's the same as the previous period.</div>;
    }

    return (
      <div className="trend-sub-item">
        That's {renderDelta(currentValue, previousValue)} from{" "}
        {formatPreviousDateRangeFromSearchParams(this.props.search)}.
      </div>
    );
  }

  render() {
    const cacheRequestTotal = +this.props.currentPeriod.acCacheHits + +this.props.currentPeriod.acCacheMisses;
    return (
      <div className="trend-chart">
        <div className="trend-chart-title">
          Summary ({formatDateRangeFromSearchParams(this.props.search)}
          {isAnyNonDateFilterSet(this.props.search) ? ", including filters" : ""})
        </div>
        <div className="trend-summary-block">
          <a className="card trend-summary-group" href="#builds">
            <div>
              <div className="trend-headline-stat">
                <List size="27" className="icon"></List>
                <span className="trend-headline-text">{format.count(this.props.currentPeriod.numBuilds)} builds</span>
              </div>
              {this.props.previousPeriod &&
                this.renderChangeText(+this.props.currentPeriod.numBuilds, +this.props.previousPeriod.numBuilds)}
            </div>
          </a>
          <a href="#cache" className="card trend-summary-group">
            <div className="trend-headline-stat">
              <Cloud size="27" className="icon"></Cloud>
              <span className="trend-headline-text">
                {format.durationMillis(+this.props.currentPeriod.cpuMicrosSaved / 1000)} CPU saved
              </span>
            </div>
            {this.props.previousPeriod &&
              this.renderChangeText(
                +this.props.currentPeriod.cpuMicrosSaved,
                +this.props.previousPeriod.cpuMicrosSaved
              )}
            <div className="trend-sub-item">
              {cacheRequestTotal > 0 && (
                <span>
                  Your action cache hit rate is{" "}
                  <strong>{format.percent(+this.props.currentPeriod.acCacheHits / cacheRequestTotal)}%</strong>
                </span>
              )}
              {"."}
            </div>
          </a>
        </div>
      </div>
    );
  }
}
