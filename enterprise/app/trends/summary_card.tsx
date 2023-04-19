import React from "react";
import { List, Cloud } from "lucide-react";

import * as format from "../../../app/format/format";
import { stats } from "../../../proto/stats_ts_proto";

interface Props {
  currentPeriod: stats.Summary;
  previousPeriod?: stats.Summary;
}

export default class TrendsSummaryCard extends React.Component<Props> {
  render() {
    return (
      <div className="trend-chart">
        <div className="trend-chart-title">Summary ({"Last 30 days"})</div>
        <div className="trend-summary-block">
          <a className="card trend-summary-group" href="#builds">
            <div>
              <div className="trend-headline-stat">
                <List size="27" className="icon"></List>
                <span className="trend-highlight">{format.count(this.props.currentPeriod.numBuilds)} builds</span>
              </div>
              {this.props.previousPeriod && +this.props.previousPeriod.numBuilds > 0 && (
                <div className="trend-sub-item">
                  That's{" "}
                  <span className="trend-change up">
                    {format.percent(
                      (+this.props.currentPeriod.numBuilds - +this.props.previousPeriod.numBuilds) /
                        +(this.props.previousPeriod.numBuilds || 1)
                    )}
                  </span>{" "}
                  from the previous period.
                </div>
              )}
              <div className="trend-sub-item">
                <span className="trend-change up">99%</span> of builds have remote caching enabled.
              </div>
            </div>
          </a>
          <a href="#cache" className="card trend-summary-group">
            <div className="trend-headline-stat">
              <Cloud size="27" className="icon"></Cloud>
              <span className="trend-highlight">
                {format.durationMillis(+this.props.currentPeriod.cpuMicrosSaved / 1000)} CPU saved
              </span>
            </div>
            <div className="trend-sub-item">
              That's <span className="trend-savings-eco">3.6kg of CO2 (haha this is kinda low)</span>
            </div>
            <div className="trend-sub-item">
              Your cache hit rate is <span className="trend-highlight">95%</span> (a{" "}
              <span className="trend-change up">30% increase</span> from the previous period).
            </div>
          </a>
        </div>
      </div>
    );
  }
}
