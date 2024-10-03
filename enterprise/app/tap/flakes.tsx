import React from "react";
import { CancelablePromise } from "../../../app/util/async";
import { target } from "../../../proto/target_ts_proto";
import rpc_service from "../../../app/service/rpc_service";
import TrendsChartComponent from "../trends/trends_chart";
import moment from "moment";
import { ChartColor } from "../trends/trends_chart";
import format, { count } from "../../../app/format/format";
import { FilterInput } from "../../../app/components/filter_input/filter_input";
import Link from "../../../app/components/link/link";
import { Check, Copy, Target } from "lucide-react";
import router from "../../../app/router/router";
import Select, { Option } from "../../../app/components/select/select";
import TapEmptyStateComponent from "./tap_empty_state";
import Banner from "../../../app/components/banner/banner";
import TargetFlakyTestCardComponent from "../../../app/target/target_flaky_test_card";
import { getProtoFilterParams } from "../filter/filter_util";
import { timestampToDateWithFallback } from "../../../app/util/proto";
import { copyToClipboard } from "../../../app/util/clipboard";

interface Props {
  search: URLSearchParams;
  repo: string;
  dark: boolean;
}

interface TestXmlOrError {
  errorMessage?: string;
  testXmlDocument?: Document;
}

type TableSort = "Flaky %" | "Flakes + Likely Flakes" | "Flakes";
const TableSortValues: TableSort[] = ["Flaky %", "Flakes + Likely Flakes", "Flakes"];

const TABLE_TRUNCATION_LENGTH = 25;

interface State {
  pendingChartRequest?: CancelablePromise<target.GetDailyTargetStatsResponse>;
  chartData?: target.GetDailyTargetStatsResponse;
  pendingTableRequest?: CancelablePromise<target.GetTargetStatsResponse>;
  tableData?: target.GetTargetStatsResponse;
  tableSort: TableSort;
  showAllTableEntries: boolean;
  pendingFlakeSamplesRequest?: CancelablePromise<target.GetTargetFlakeSamplesResponse>;
  flakeSamples?: target.GetTargetFlakeSamplesResponse;
  flakeTestXmlDocs: Map<string, TestXmlOrError>;
  error?: string;
}

export default class FlakesComponent extends React.Component<Props, State> {
  state: State = {
    flakeTestXmlDocs: new Map(),
    tableSort: "Flaky %",
    showAllTableEntries: false,
  };

  componentDidMount(): void {
    this.fetch();
  }

  componentDidUpdate(prevProps: Props) {
    const currentTarget = this.props.search.get("target") ?? "";
    const prevTarget = prevProps.search.get("target") ?? "";
    const currentProtoParams = getProtoFilterParams(this.props.search);
    const currentStart = timestampToDateWithFallback(currentProtoParams.updatedAfter, 0).getTime();
    const currentEnd = timestampToDateWithFallback(currentProtoParams.updatedBefore, 0).getTime();

    const prevProtoParams = getProtoFilterParams(prevProps.search);
    const prevStart = timestampToDateWithFallback(prevProtoParams.updatedAfter, 0).getTime();
    const prevEnd = timestampToDateWithFallback(prevProtoParams.updatedBefore, 0).getTime();

    const dateChanged = currentStart != prevStart || currentEnd != prevEnd;
    if (currentTarget !== prevTarget || this.props.repo !== prevProps.repo || dateChanged) {
      this.fetch();
    }
  }

  fetch() {
    const label = this.props.search.get("target");
    const labels = label ? [label] : [];
    const params = getProtoFilterParams(this.props.search);

    this.state.pendingChartRequest?.cancel();
    this.state.pendingTableRequest?.cancel();
    this.state.pendingFlakeSamplesRequest?.cancel();

    this.setState({
      pendingChartRequest: undefined,
      pendingTableRequest: undefined,
      pendingFlakeSamplesRequest: undefined,
      error: undefined,
    });

    const chartRequest = rpc_service.service.getDailyTargetStats({
      labels,
      repo: this.props.repo,
      startedAfter: params.updatedAfter,
      startedBefore: params.updatedBefore,
    });
    const tableRequest = rpc_service.service.getTargetStats({
      labels,
      repo: this.props.repo,
      startedAfter: params.updatedAfter,
      startedBefore: params.updatedBefore,
    });
    this.setState({ pendingChartRequest: chartRequest, pendingTableRequest: tableRequest });

    chartRequest
      .then((r) => {
        console.log(r);
        this.setState({ pendingChartRequest: undefined, chartData: r });
      })
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
      const flakeSamplesRequest = rpc_service.service.getTargetFlakeSamples({
        label,
        repo: this.props.repo,
        startedAfter: params.updatedAfter,
        startedBefore: params.updatedBefore,
      });
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
            errorMessage: "Cache expired or invalid test xml.",
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

  toggleShowAllTableEntries() {
    this.setState({ showAllTableEntries: !this.state.showAllTableEntries });
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

  renderFlakeSamples(targetLabel: string) {
    return (
      <div className="container">
        <h3 className="flakes-list-header">Sample flakes for {targetLabel}</h3>
        {!this.state.pendingFlakeSamplesRequest && !(this.state.flakeSamples?.samples.length ?? 0) && (
          <div>No samples found. Their logs may have expired from the remote cache.</div>
        )}
        {this.state.flakeSamples?.samples.map((s) => {
          const testXmlDoc = this.state.flakeTestXmlDocs.get(s.testXmlFileUri);
          if (!testXmlDoc) {
            return <div className="loading"></div>;
          } else if (testXmlDoc.errorMessage) {
            // Error messages will just be aggregated at the end.
            return (
              <div className={"card artifacts card-broken"}>
                <div>
                  Failed to load test xml for a failure in invocation{" "}
                  <Link href={router.getInvocationUrl(s.invocationId)}>{s.invocationId}</Link>:{" "}
                  {testXmlDoc.errorMessage}
                </div>
              </div>
            );
          } else if (testXmlDoc.testXmlDocument) {
            return Array.from(testXmlDoc.testXmlDocument.getElementsByTagName("testsuite"))
              .filter((testSuite) => testSuite.getElementsByTagName("testcase").length > 0)
              .sort((a, b) => +(b.getAttribute("failures") || 0) - +(a.getAttribute("failures") || 0))
              .map((testSuite) => {
                return (
                  <TargetFlakyTestCardComponent
                    invocationId={s.invocationId}
                    invocationStartTimeUsec={+s.invocationStartTimeUsec}
                    target={targetLabel}
                    testSuite={testSuite}
                    buildEvent={s.event!}
                    dark={this.props.dark}></TargetFlakyTestCardComponent>
                );
              });
          }
        })}
        {Boolean(this.state.pendingFlakeSamplesRequest) && <div className="loading"></div>}
        {!this.state.pendingFlakeSamplesRequest && this.state.flakeSamples?.nextPageToken && (
          <button className="load-more" onClick={() => this.loadMoreSamples()}>
            Look for more samples
          </button>
        )}
      </div>
    );
  }

  render() {
    const singleTarget = this.props.search.get("target");

    const dailyFlakesHeader = (
      <h3 className="flakes-chart-header">{`Daily flakes ${singleTarget ? `for ${singleTarget} ` : ""}`}</h3>
    );

    if (this.state.pendingChartRequest || this.state.pendingTableRequest) {
      return (
        <div className="container">
          {dailyFlakesHeader}
          <div className="loading"></div>
        </div>
      );
    }
    if (this.state.error) {
      return (
        <div className="container">
          <Banner type="warning">{this.state.error}</Banner>
        </div>
      );
    }

    let tableData = singleTarget ? [] : this.state.tableData?.stats ?? [];
    let sortFn: (a: target.AggregateTargetStats, b: target.AggregateTargetStats) => number;
    if (this.state.tableSort === "Flakes") {
      sortFn = (a, b) => {
        const aFlakes = +(a.data?.flakyRuns ?? 0);
        const bFlakes = +(b.data?.flakyRuns ?? 0);

        return bFlakes - aFlakes;
      };
    } else if (this.state.tableSort === "Flakes + Likely Flakes") {
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

    let filteredTableData = [...tableData];
    const tableFilters = (this.props.search.get("targetFilter") ?? "").split(" ").filter((f) => f.length > 0);
    if (tableFilters.length > 0) {
      filteredTableData = filteredTableData.filter((v) => tableFilters.find((f) => v.label.includes(f)));
    }
    filteredTableData.sort(sortFn);

    let tableIsPaginated = filteredTableData.length > TABLE_TRUNCATION_LENGTH;
    if (!this.state.showAllTableEntries && tableIsPaginated) {
      filteredTableData.length = TABLE_TRUNCATION_LENGTH; // Javascript is so cool
    }

    let dates: number[] = [];
    const params = getProtoFilterParams(this.props.search);
    let currentDay = moment().startOf("day");
    if (+(params.updatedBefore?.seconds ?? 0) > 0) {
      // Drop an extra second from the "updatedBefore" value: the end of the range
      // is exclusive + we don't want to render "Sep 13" if the end of the range is
      // midnight on September 13th.
      currentDay = moment.unix(+params.updatedBefore!.seconds - 1).startOf("day");
    }

    dates = [currentDay.unix()];

    while (currentDay.unix() > +(params.updatedAfter?.seconds ?? 0)) {
      currentDay = currentDay.subtract(1, "day");
      dates = [currentDay.unix(), ...dates];
    }

    const isEmpty = this.state.tableData && this.state.tableData.stats.length === 0;

    let totalFlakes = 0;
    let totalLikelyFlakes = 0;
    this.state.tableData?.stats.forEach((s) => {
      totalFlakes += +(s.data?.flakyRuns ?? 0);
      totalLikelyFlakes += +(s.data?.likelyFlakyRuns ?? 0);
    });

    if (isEmpty) {
      return (
        <TapEmptyStateComponent
          title="No flakes found!"
          message="Wow! Either you have no flaky CI tests, or no CI test data all. To see CI test data, make sure your CI tests are configured as follows:"
          showV2Instructions={true}></TapEmptyStateComponent>
      );
    }

    return (
      <div>
        <div className="container">
          {dailyFlakesHeader}
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
            <h3 className="flakes-list-header">Flaky targets</h3>
            <div className="card">
              <div className="content">
                <div className="flake-table">
                  {!singleTarget && (
                    <div className="flake-table-row flake-table-summary-row">
                      <div className="flake-table-row-image">
                        <Target className="icon"></Target>
                      </div>
                      <div className="flake-table-row-content">
                        <div className="flake-table-row-header">Totals</div>
                        <div className="flake-table-row-stats">
                          <div className="flake-stat">
                            <span className="flake-stat-value">{tableData.length}</span>{" "}
                            {this.renderPluralName(tableData.length, "flaky target")}
                          </div>
                          <div className="flake-stat">
                            <span className="flake-stat-value">{totalFlakes}</span>{" "}
                            {this.renderPluralName(totalFlakes, "flake")}
                          </div>
                          <div className="flake-stat">
                            <span className="flake-stat-value">{totalLikelyFlakes}</span>{" "}
                            {this.renderPluralName(totalLikelyFlakes, "likely flake")}
                          </div>
                        </div>
                      </div>
                    </div>
                  )}
                </div>
                <div className="flake-table-header">
                  <FilterInput onChange={(e) => this.handleStatsFilterChange(e.target.value)}></FilterInput>
                  <div className="flake-table-sort-controls">
                    <span className="invocation-sort-title">Sort by</span>
                    <Select onChange={(e) => this.handleTableSortChange(e.target.value)} value={this.state.tableSort}>
                      <Option value="Flaky %">Flaky %</Option>
                      <Option value="Flakes">Flakes</Option>
                      <Option value="Flakes + Likely Flakes">Flakes + Likely Flakes</Option>
                    </Select>
                  </div>
                </div>
                <div className="flake-table">
                  {filteredTableData.map((s, index) => {
                    return (
                      <Link key={index} className="flake-table-row" href={`/tests/?target=${s.label}#flakes`}>
                        <div className="flake-table-row-image">
                          <Target className="icon"></Target>
                        </div>
                        <div className="flake-table-row-content">
                          <div className="flake-table-row-header">
                            {s.label} <CopyButton text={s.label}></CopyButton>
                          </div>
                          <div className="flake-table-row-stats">
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
                            <div className="flake-stat">
                              <span className="flake-stat-value">
                                {format.compactDurationSec(
                                  +(s.data?.totalFlakeRuntimeUsec ?? 0) /
                                    1e6 /
                                    (+(s.data?.flakyRuns ?? 0) + +(s.data?.flakyRuns ?? 0) || 1)
                                )}
                              </span>{" "}
                              per flake
                            </div>
                          </div>
                        </div>
                      </Link>
                    );
                  })}
                </div>
                {tableIsPaginated && (
                  <button className="load-more" onClick={() => this.toggleShowAllTableEntries()}>
                    {this.state.showAllTableEntries ? "Show less" : "Show all"}
                  </button>
                )}
              </div>
            </div>
          </div>
        )}
        {singleTarget && this.state.tableData && this.renderFlakeSamples(singleTarget)}
      </div>
    );
  }
}

interface CopyButtonProps {
  text: string;
}

interface CopyButtonState {
  copied: boolean;
}

class CopyButton extends React.Component<CopyButtonProps, CopyButtonState> {
  state: CopyButtonState = {
    copied: false,
  };

  timeout: number = 0;

  onCopyClicked(e: React.MouseEvent<HTMLSpanElement>) {
    e.stopPropagation();
    e.preventDefault();
    copyToClipboard(this.props.text);
    if (this.timeout) {
      clearTimeout(this.timeout);
    }
    this.setState({ copied: true });
    this.timeout = window.setTimeout(() => {
      this.setState({ copied: false });
    }, 2000);
  }

  render() {
    const content = this.state.copied ? (
      <>
        <span className="copy-icon-wrapper" onClick={(e) => this.onCopyClicked(e)}>
          <Check className="copy-icon green" />
        </span>
        <span className="copy-button-text">Copied!</span>
      </>
    ) : (
      <span className="copy-icon-wrapper" onClick={(e) => this.onCopyClicked(e)}>
        <Copy className="copy-icon" />
      </span>
    );
    return <span className="target-copy-button">{content}</span>;
  }
}
