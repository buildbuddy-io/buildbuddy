import React from "react";
import { CancelablePromise } from "../../../app/util/async";
import { target } from "../../../proto/target_ts_proto";
import { api as api_common } from "../../../proto/api/v1/common_ts_proto";
import rpc_service from "../../../app/service/rpc_service";
import TrendsChartComponent from "../trends/trends_chart";
import moment from "moment";
import { ChartColor } from "../trends/trends_chart";
import format, { count } from "../../../app/format/format";
import { Tooltip, pinBottomMiddleToMouse } from "../../../app/components/tooltip/tooltip";
import { FilterInput } from "../../../app/components/filter_input/filter_input";
import Link from "../../../app/components/link/link";
import { Target } from "lucide-react";
import TargetLogCardComponent from "../../../app/target/target_log_card";
import TerminalComponent from "../../../app/terminal/terminal";
import router from "../../../app/router/router";
import TargetTestCasesCardComponent from "../../../app/target/target_test_cases_card";
import Select, { Option } from "../../../app/components/select/select";
import TapEmptyStateComponent from "./tap_empty_state";
import Banner from "../../../app/components/banner/banner";
import TargetFlakyTestCardComponent from "../../../app/target/target_flaky_test_card";

interface Props {
  search: URLSearchParams;
  repo: string;
}

interface TestXmlOrError {
  errorMessage?: string;
  testXmlDocument?: Document;
}

type TableSort = "Flaky %" | "Flakes + Likely flakes" | "Flakes";
const TableSortValues: TableSort[] = ["Flaky %", "Flakes + Likely flakes", "Flakes"];

interface State {
  pendingChartRequest?: CancelablePromise<target.GetDailyTargetStatsResponse>;
  chartData?: target.GetDailyTargetStatsResponse;
  pendingTableRequest?: CancelablePromise<target.GetTargetStatsResponse>;
  tableData?: target.GetTargetStatsResponse;
  tableSort: TableSort;
  pendingFlakeSamplesRequest?: CancelablePromise<target.GetTargetFlakeSamplesResponse>;
  flakeSamples?: target.GetTargetFlakeSamplesResponse;
  flakeTestXmlDocs: Map<string, TestXmlOrError>;
  error?: string;
}

export default class FlakesComponent extends React.Component<Props, State> {
  state: State = {
    flakeTestXmlDocs: new Map(),
    tableSort: "Flaky %",
  };

  componentDidMount(): void {
    this.fetch();
  }

  componentDidUpdate(prevProps: Props) {
    const currentTarget = this.props.search.get("target") ?? "";
    const prevTarget = prevProps.search.get("target") ?? "";
    if (currentTarget !== prevTarget) {
      this.fetch();
    }
  }

  fetch() {
    const label = this.props.search.get("target");
    const labels = label ? [label] : [];

    this.state.pendingChartRequest?.cancel();
    this.state.pendingTableRequest?.cancel();
    this.state.pendingFlakeSamplesRequest?.cancel();

    this.setState({
      pendingChartRequest: undefined,
      pendingTableRequest: undefined,
      pendingFlakeSamplesRequest: undefined,
      error: undefined,
    });

    const chartRequest = rpc_service.service.getDailyTargetStats({ labels, repo: this.props.repo });
    const tableRequest = rpc_service.service.getTargetStats({ labels, repo: this.props.repo });
    this.setState({ pendingChartRequest: chartRequest, pendingTableRequest: tableRequest });

    chartRequest
      .then((r) => this.setState({ pendingChartRequest: undefined, chartData: r }))
      .catch(() => {
        this.setState({
          pendingChartRequest: undefined,
          error: "Failed to load flakes data.  Please try again later.",
        });
      });
    tableRequest
      .then((r) => {
        console.log(r);
        this.setState({ pendingTableRequest: undefined, tableData: r });
      })
      .catch(() => {
        this.setState({
          pendingTableRequest: undefined,
          error: "Failed to load flakes data.  Please try again later.",
        });
      });

    if (label) {
      const flakeSamplesRequest = rpc_service.service.getTargetFlakeSamples({ label, repo: this.props.repo });
      this.setState({ pendingFlakeSamplesRequest: flakeSamplesRequest });

      flakeSamplesRequest.then((r) => {
        console.log(r);
        this.setState({ pendingFlakeSamplesRequest: undefined, flakeSamples: r });
        r.samples.forEach((s) => {
          this.fetchTestXml(s);
        });
      });
    }
  }

  fetchTestXml(sample: target.FlakeSample) {
    rpc_service
      .fetchBytestreamFile(sample.testXmlFileUri, sample.invocationId)
      .then((contents: string) => {
        let parser = new DOMParser();
        let xmlDoc = parser.parseFromString(contents, "text/xml");
        this.setState((s) => {
          const newMap = new Map(s.flakeTestXmlDocs);
          newMap.set(sample.testXmlFileUri, { testXmlDocument: xmlDoc });
          return { flakeTestXmlDocs: newMap };
        });
      })
      .catch(() => {
        this.setState((s) => {
          const newMap = new Map(s.flakeTestXmlDocs);
          newMap.set(sample.testXmlFileUri, {
            errorMessage: "Failed to load test results (cache expired or invalid test xml?)",
          });
          return { flakeTestXmlDocs: newMap };
        });
      });
  }

  loadMoreSamples() {
    const label = this.props.search.get("target");
    if (!label || !this.state.flakeSamples?.nextPageToken) {
      // Shouldn't actually happen, just making TS happy.
      return;
    }

    const flakeSamplesRequest = rpc_service.service.getTargetFlakeSamples({
      label,
      repo: this.props.repo,
      pageToken: this.state.flakeSamples.nextPageToken,
    });

    this.setState({ pendingFlakeSamplesRequest: flakeSamplesRequest });

    const previousSamples = this.state.flakeSamples.samples;

    flakeSamplesRequest.then((r) => {
      console.log(r);
      r.samples.forEach((s) => {
        this.fetchTestXml(s);
      });

      r.samples = previousSamples.concat(r.samples);
      this.setState({ pendingFlakeSamplesRequest: undefined, flakeSamples: r });
    });
  }

  getChartData(start: number): target.TargetStatsData {
    const date = moment.unix(start).format("YYYY-MM-DD");
    return this.state.chartData?.stats.find((v) => v.date === date)?.data ?? new target.TargetStatsData({});
  }

  handleStatsFilterChange(newValue: string) {
    router.updateParams({ targetFilter: newValue.trim() });
  }

  handleTableSortChange(tableSortString: string) {
    const tableSort: TableSort = TableSortValues.find((v) => v === tableSortString) ?? "Flaky %";
    this.setState({ tableSort });
  }

  renderFlakePercent(stats: target.TargetStatsData | null | undefined): string {
    if (!stats) {
      return "0%";
    }
    const totalFlakes = (+stats.flakyRuns ?? 0) + (+stats.likelyFlakyRuns ?? 0);
    if (totalFlakes === 0) {
      return "0%";
    }
    const percent = format.percent(totalFlakes / (+stats.totalRuns ?? 1));

    return percent === "0" ? "<1%" : percent + "%";
  }

  renderPluralName(value: number, label: string) {
    return label + (value !== 1 ? "s" : "");
  }

  renderPluralCount(value: number | undefined, label: string) {
    const val = value ?? 0;
    return `${val} ${this.renderPluralName(val, label)}`;
  }

  render() {
    if (this.state.pendingChartRequest || this.state.pendingTableRequest) {
      return <div className="loading"></div>;
    }
    if (this.state.error) {
      return (
        <div className="container">
          <Banner type="warning">{this.state.error}</Banner>
        </div>
      );
    }

    const singleTarget = this.props.search.get("target");
    console.log(singleTarget);

    let tableData = singleTarget ? [] : this.state.tableData?.stats ?? [];
    let sortFn: (a: target.AggregateTargetStats, b: target.AggregateTargetStats) => number;
    if (this.state.tableSort === "Flakes") {
      sortFn = (a, b) => {
        const aFlakes = +(a.data?.flakyRuns ?? 0);
        const bFlakes = +(b.data?.flakyRuns ?? 0);

        return bFlakes - aFlakes;
      };
    } else if (this.state.tableSort === "Flakes + Likely flakes") {
      sortFn = (a, b) => {
        const aFlakes = +(a.data?.flakyRuns ?? 0);
        const aLikelyFlakes = +(a.data?.likelyFlakyRuns ?? 0);
        const bFlakes = +(b.data?.flakyRuns ?? 0);
        const bLikelyFlakes = +(b.data?.likelyFlakyRuns ?? 0);

        return bFlakes + bLikelyFlakes - (aFlakes + aLikelyFlakes);
      };
    } else {
      sortFn = (a, b) => {
        const aFlakes = +(a.data?.flakyRuns ?? 0);
        const aLikelyFlakes = +(a.data?.likelyFlakyRuns ?? 0);
        const bFlakes = +(b.data?.flakyRuns ?? 0);
        const bLikelyFlakes = +(b.data?.likelyFlakyRuns ?? 0);
        const aTotal = +(a.data?.totalRuns ?? 1);
        const bTotal = +(b.data?.totalRuns ?? 1);

        return (bFlakes + bLikelyFlakes) / bTotal - (aFlakes + aLikelyFlakes) / aTotal;
      };
    }

    let filteredTableData = tableData;
    const tableFilters = (this.props.search.get("targetFilter") ?? "").split(" ").filter((f) => f.length > 0);
    if (tableFilters.length > 0) {
      filteredTableData = filteredTableData.filter((v) => tableFilters.find((f) => v.label.includes(f)));
    }
    filteredTableData.sort(sortFn);

    let dates: number[] = [];
    let currentDay = moment().startOf("day");
    for (let i = 0; i < 7; i++) {
      dates = [currentDay.unix(), ...dates];
      currentDay = currentDay.subtract(1, "day");
    }

    const isEmpty = this.state.tableData && this.state.tableData.stats.length === 0;

    if (isEmpty) {
      return (
        <TapEmptyStateComponent
          message="Wow! Either you have no flaky CI tests, or no CI test data all. To see CI test data, make sure your CI tests are configured as follows:"
          showV2Instructions={true}></TapEmptyStateComponent>
      );
    }

    return (
      <div>
        <div className="container">
          <h3 className="flakes-chart-header">{`Daily flakes ${
            singleTarget ? `for ${singleTarget} ` : ""
          }(last 7 days)`}</h3>
          <div className="card chart-card">
            <TrendsChartComponent
              title=""
              standaloneChart={true}
              data={dates}
              dataSeries={[
                {
                  name: "flakes",
                  extractValue: (ts) => +(this.getChartData(ts).flakyRuns ?? 0),
                  formatHoverValue: (value) => this.renderPluralCount(value, "flake"),
                  stackId: "flakes",
                  color: ChartColor.ORANGE,
                },
                {
                  name: "likely flakes",
                  extractValue: (ts) => +(this.getChartData(ts).likelyFlakyRuns ?? 0),
                  formatHoverValue: (value) => this.renderPluralCount(value, "likely flake"),
                  stackId: "flakes",
                  color: ChartColor.RED,
                },
              ]}
              primaryYAxis={{
                formatTickValue: count,
                allowDecimals: false,
              }}
              formatXAxisLabel={(ts) => moment.unix(ts).format("MMM D")}
              formatHoverXAxisLabel={(ts) => moment.unix(ts).format("dddd, MMMM Do YYYY")}
              ticks={[]}></TrendsChartComponent>
          </div>
        </div>
        {tableData.length > 0 && (
          <div className="container">
            <h3 className="flakes-list-header">Flaky targets (last 7 days)</h3>

            <div className="card">
              <div className="content">
                <div className="flake-list-table-header">
                  <FilterInput onChange={(e) => this.handleStatsFilterChange(e.target.value)}></FilterInput>
                  <div className="flake-list-table-sort-controls">
                    <span className="invocation-sort-title">Sort by</span>
                    <Select onChange={(e) => this.handleTableSortChange(e.target.value)} value={this.state.tableSort}>
                      <Option value="Flaky %">Flaky %</Option>
                      <Option value="Flakes">Flakes</Option>
                      <Option value="Flakes + Likely Flakes">Flakes + Likely Flakes</Option>
                    </Select>
                  </div>
                </div>
                <div className="list-table">
                  {filteredTableData.map((s, index) => {
                    return (
                      <Link key={index} className="list-table-row" href={`/tests/?target=${s.label}#flakes`}>
                        <div className="list-table-row-image">
                          <Target className="icon"></Target>
                        </div>
                        <div className="list-table-row-content">
                          <div className="list-table-row-header">{s.label}</div>
                          <div className="list-table-row-stats">
                            <div className="flake-stat">
                              <span className="flake-stat-value">{this.renderFlakePercent(s.data)}</span> flaky
                            </div>
                            <div className="flake-stat">
                              <span className="flake-stat-value">{s.data?.flakyRuns ?? 0}</span>{" "}
                              {this.renderPluralName(+(s.data?.flakyRuns ?? 0), "flake")}
                            </div>
                            <div className="flake-stat">
                              <span className="flake-stat-value">{s.data?.likelyFlakyRuns ?? 0}</span>{" "}
                              {this.renderPluralName(+(s.data?.likelyFlakyRuns ?? 0), "likely flake")}
                            </div>
                            <div className="flake-stat">
                              <span className="flake-stat-value">{s.data?.totalRuns ?? 0}</span> total runs
                            </div>
                          </div>
                        </div>
                      </Link>
                    );
                  })}
                </div>
              </div>
            </div>
          </div>
        )}
        {singleTarget && this.state.tableData && <div></div>}
        {singleTarget && this.state.tableData && this.state.flakeSamples && (
          <div className="container">
            <h3 className="flakes-list-header">Sample flakes for {singleTarget}</h3>
            {this.state.flakeSamples.samples.map((s) => {
              const status = s.status === api_common.v1.Status.FLAKY ? "flaky" : "failure";
              const testXmlDoc = this.state.flakeTestXmlDocs.get(s.testXmlFileUri);
              if (!testXmlDoc) {
                return <div className="loading"></div>;
              } else if (testXmlDoc.errorMessage) {
                return <div>{testXmlDoc.errorMessage}</div>;
              } else if (testXmlDoc.testXmlDocument) {
                return Array.from(testXmlDoc.testXmlDocument.getElementsByTagName("testsuite"))
                  .filter((testSuite) => testSuite.getElementsByTagName("testcase").length > 0)
                  .sort((a, b) => +(b.getAttribute("failures") || 0) - +(a.getAttribute("failures") || 0))
                  .map((testSuite) => {
                    return (
                      <TargetFlakyTestCardComponent
                        invocationId={s.invocationId}
                        invocationStartTimeUsec={+s.invocationStartTimeUsec}
                        target={singleTarget}
                        testSuite={testSuite}
                        buildEvent={s.event!}
                        status={status}
                        dark={true}></TargetFlakyTestCardComponent>
                    );
                  });
              }
            })}
            {this.state.flakeSamples.nextPageToken && (
              <button className="load-more" onClick={() => this.loadMoreSamples()}>
                Load more samples
              </button>
            )}
          </div>
        )}
      </div>
    );
  }
}