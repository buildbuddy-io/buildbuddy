import React from "react";
import { User } from "../../../app/auth/auth_service";
import FilterComponent from "../filter/filter";
import capabilities from "../../../app/capabilities/capabilities";
import DrilldownPageComponent from "./drilldown_page";
import TrendsModel from "./trends_model";
import TrendsTab from "./common";
import SimpleTrendsTabComponent from "./simple_trends_tab";
import { TrendsRpcCache } from "./trends_requests";

interface Props {
  user: User;
  tab: string;
  search: URLSearchParams;
}

interface State {
  trendsModel: TrendsModel;
}

export default class NewTrendsComponent extends React.Component<Props, State> {
  state: State = {
    trendsModel: new TrendsModel(true),
  };

  private rpcCache = new TrendsRpcCache();

  componentWillMount() {
    document.title = `Trends | BuildBuddy`;
  }

  setSelectedTab(tab: TrendsTab) {
    window.location.hash = "#" + tab;
  }

  getSelectedTab(): TrendsTab {
    const value = Number(this.props.tab.replace("#", ""));
    if (value && TrendsTab[value] && (value != TrendsTab.DRILLDOWN || capabilities.config.trendsHeatmapEnabled)) {
      return value;
    }
    return TrendsTab.OVERVIEW;
  }

  private renderTab(tab: TrendsTab, title: String) {
    return (
      <div
        onClick={() => this.setSelectedTab(tab)}
        className={`tab ${this.getSelectedTab() === tab ? "selected" : ""}`}>
        {title}
      </div>
    );
  }

  render() {
    const selectedTab = this.getSelectedTab();
    return (
      <div className="trends">
        <div className="container">
          <div className="trends-header">
            <div className="trends-title">Trends</div>
            <FilterComponent search={this.props.search} />
          </div>
          <div className="tabs">
            {this.renderTab(TrendsTab.OVERVIEW, "Overview")}
            {this.renderTab(TrendsTab.BUILDS, "Builds")}
            {this.renderTab(TrendsTab.CACHE, "Cache")}
            {this.renderTab(TrendsTab.EXECUTIONS, "Remote Execution")}
            {capabilities.config.trendsHeatmapEnabled && this.renderTab(TrendsTab.DRILLDOWN, "Drilldown")}
          </div>
          {selectedTab == TrendsTab.OVERVIEW && (
            <SimpleTrendsTabComponent tab={TrendsTab.OVERVIEW} search={this.props.search} cache={this.rpcCache} />
          )}
          {selectedTab == TrendsTab.BUILDS && (
            <SimpleTrendsTabComponent tab={TrendsTab.BUILDS} search={this.props.search} cache={this.rpcCache} />
          )}
          {selectedTab == TrendsTab.CACHE && (
            <SimpleTrendsTabComponent tab={TrendsTab.CACHE} search={this.props.search} cache={this.rpcCache} />
          )}
          {selectedTab == TrendsTab.EXECUTIONS && (
            <SimpleTrendsTabComponent tab={TrendsTab.EXECUTIONS} search={this.props.search} cache={this.rpcCache} />
          )}
          {capabilities.config.trendsHeatmapEnabled && selectedTab == TrendsTab.DRILLDOWN && (
            <DrilldownPageComponent user={this.props.user} search={this.props.search}></DrilldownPageComponent>
          )}
        </div>
      </div>
    );
  }
}
