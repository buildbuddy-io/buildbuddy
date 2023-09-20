import React from "react";
import { Cpu, Hash, Package } from "lucide-react";

import * as format from "../../../app/format/format";
import { stats } from "../../../proto/stats_ts_proto";
import {
  isAnyNonDateFilterSet,
  formatDateRangeFromSearchParams,
  formatDateRangeDurationFromSearchParams,
} from "../filter/filter_util";

interface Props {
  search: URLSearchParams;
  currentPeriod: stats.Summary;
  previousPeriod?: stats.Summary;
}

function renderDelta(currentValue: number, previousValue: number): JSX.Element {
  const rawPercentage = (currentValue - previousValue) / previousValue;

  if (Math.abs(Math.round(rawPercentage * 100)) < 1) {
    return <span className={"trend-change unchanged"}>roughly unchanged</span>;
  }

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
      return (
        <div className="trend-sub-item">
          No data available for the previous {formatDateRangeDurationFromSearchParams(this.props.search)}.
        </div>
      );
    }
    const rawPercentage = (currentValue - previousValue) / previousValue;
    if (Math.abs(rawPercentage) < 0.01) {
      return (
        <div className="trend-sub-item">
          That's the same as the previous {formatDateRangeDurationFromSearchParams(this.props.search)}.
        </div>
      );
    }

    return (
      <div className="trend-sub-item">
        That's {renderDelta(currentValue, previousValue)} from the previous{" "}
        {formatDateRangeDurationFromSearchParams(this.props.search)}.
      </div>
    );
  }

  render() {
    const cacheRequestTotal = +this.props.currentPeriod.acCacheHits + +this.props.currentPeriod.acCacheMisses;
    const cacheHitRate = +this.props.currentPeriod.acCacheHits / cacheRequestTotal;
    const previousCacheRequestTotal =
      +(this.props.previousPeriod?.acCacheHits ?? 0) + +(this.props.previousPeriod?.acCacheMisses ?? 0);
    const previousCacheHitRate = +(this.props.previousPeriod?.acCacheHits ?? 0) / previousCacheRequestTotal;
    const formattedTimePeriod = formatDateRangeDurationFromSearchParams(this.props.search);
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
                <Hash size="27" className="icon"></Hash>
                <span className="trend-headline-text">{format.count(this.props.currentPeriod.numBuilds)} builds</span>
              </div>
              {this.props.previousPeriod &&
                this.renderChangeText(+this.props.currentPeriod.numBuilds, +this.props.previousPeriod.numBuilds)}
            </div>
          </a>
          <a href="#cache" className="card trend-summary-group">
            <div className="trend-headline-stat">
              <Package size="27" className="icon"></Package>
              <span className="trend-headline-text">{format.percent(cacheHitRate)}% AC hit rate</span>
            </div>
            {this.props.previousPeriod && previousCacheRequestTotal > 0 && (
              <div className="trend-sub-item">
                {cacheRequestTotal > 0 && (
                  <span>
                    That's {renderDelta(cacheHitRate, previousCacheHitRate)} from the previous {formattedTimePeriod},
                    when your action cache hit rate was <strong>{format.percent(previousCacheHitRate)}%</strong>.
                  </span>
                )}
              </div>
            )}
          </a>
          <a href="#savings" className="card trend-summary-group">
            <div className="trend-headline-stat">
              <Cpu size="27" className="icon"></Cpu>
              <span className="trend-headline-text">
                {format.cpuSavingsSec(+this.props.currentPeriod.cpuMicrosSaved / 1000000)} saved
              </span>
            </div>
            {+this.props.currentPeriod.cpuMicrosSaved > 0 && (
              <div className="trend-sub-item">
                <span>
                  That's at least{" "}
                  <span className="trend-change up">
                    {format.durationMillis(+this.props.currentPeriod.cpuMicrosSaved / 8000)}
                  </span>{" "}
                  not waiting for code to compile on an 8-core machine in the last {formattedTimePeriod}.
                </span>
              </div>
            )}
          </a>
        </div>
      </div>
    );
  }
}
