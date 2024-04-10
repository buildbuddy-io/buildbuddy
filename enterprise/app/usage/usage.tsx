import React from "react";
import errorService from "../../../app/errors/error_service";
import rpcService, { CancelablePromise } from "../../../app/service/rpc_service";
import { User } from "../../../app/auth/auth_service";
import { usage } from "../../../proto/usage_ts_proto";
import Select, { Option } from "../../../app/components/select/select";
import { cpuSavingsSec, formatWithCommas, bytes as formatBytes } from "../../../app/format/format";
import moment from "moment";

export interface UsageProps {
  user?: User;
}

interface State {
  response?: usage.GetUsageResponse;
  selectedPeriod: string;
  loading?: boolean;
}

export default class UsageComponent extends React.Component<UsageProps, State> {
  // TODO: remove getDefaultTimePeriodString() after the server
  // is updated to unconditionally send the current period
  state: State = { selectedPeriod: getDefaultTimePeriodString() };
  pendingRequest?: CancelablePromise<any>;

  componentDidMount() {
    document.title = "Usage | BuildBuddy";
    this.fetchUsageForPeriod(this.state.selectedPeriod);
  }

  private onChangePeriod(e: React.ChangeEvent<HTMLSelectElement>) {
    const period = e.target.value;
    this.setState({
      selectedPeriod: period,
    });
    this.fetchUsageForPeriod(period);
  }

  private fetchUsageForPeriod(period: string) {
    this.pendingRequest?.cancel();
    this.setState({ loading: true });

    rpcService.service
      .getUsage(new usage.GetUsageRequest({ usagePeriod: period }))
      .then((response) => {
        if (!response.usage) {
          throw new Error("Server did not return usage data.");
        }
        this.setState({ response, selectedPeriod: period });
      })
      .catch((e) => errorService.handleError(e))
      .finally(() => this.setState({ loading: false }));
  }

  render() {
    const orgName = this.props.user?.selectedGroup.name;
    // Selected period may not be found because of a pending or failed RPC.
    const selection = this.state.response?.usage;

    return (
      <div className="usage-page">
        <div className="container usage-page-container">
          <div className="usage-header">
            <div className="usage-title">Usage</div>
          </div>
          {this.state.response && (
            <div className="card usage-card">
              <div className="content">
                <div className="usage-period-header">
                  <div>
                    {orgName && <div className="org-name">{orgName}</div>}
                    <div className="selected-period-label">
                      BuildBuddy usage for <span className="usage-period">{this.state.selectedPeriod} (UTC)</span>
                    </div>
                  </div>
                  <Select title="Usage period" onChange={this.onChangePeriod.bind(this)}>
                    {this.state.response.availableUsagePeriods.map((period, i) => (
                      <Option key={period} value={period}>
                        {period}
                        {i === 0 ? " (Current period)" : ""}
                      </Option>
                    ))}
                  </Select>
                </div>
                {this.state.loading && <div className="loading" />}
                {!this.state.loading && !selection && <span>Failed to load usage data.</span>}
                {!this.state.loading && selection && (
                  <div className="usage-period-table">
                    <div className="usage-resource-name">Invocations</div>
                    <div className="usage-value">{formatWithCommas(selection.invocations)}</div>
                    <div className="usage-resource-name">Action cache hits</div>
                    <div className="usage-value">{formatWithCommas(selection.actionCacheHits)}</div>
                    <div className="usage-resource-name">Cached build minutes</div>
                    <div className="usage-value">{formatMinutes(Number(selection.totalCachedActionExecUsec))}</div>
                    <div className="usage-resource-name">Content addressable storage cache hits</div>
                    <div className="usage-value">{formatWithCommas(selection.casCacheHits)}</div>
                    <div className="usage-resource-name">Total bytes downloaded from cache</div>
                    <div className="usage-value" title={formatWithCommas(selection.totalDownloadSizeBytes)}>
                      {formatBytes(selection.totalDownloadSizeBytes)}
                    </div>
                    {/*
                  <div className="usage-resource-name">Total bytes uploaded from cache</div>
                  <div className="usage-value" title={formatWithCommas(selection.totalUploadSizeBytes)}>
                    {formatBytes(period.totalUploadSizeBytes)}
                  </div>
				    */}
                    <div className="usage-resource-name">Linux remote execution duration</div>
                    <div className="usage-value">{formatMinutes(Number(selection.linuxExecutionDurationUsec))}</div>
                    {/*
                  <div className="usage-resource-name">Saved CPU Time</div>
                  <div className="usage-value">{cpuSavingsSec(Number(selection.totalCachedActionExecUsec))}</div>
				    */}
                  </div>
                )}
              </div>
            </div>
          )}
        </div>
      </div>
    );
  }
}

function getDefaultTimePeriodString(): string {
  return moment.utc().format("YYYY-MM");
}

function formatMinutes(usec: number): string {
  return `${formatWithCommas(usec / 60e6)} minutes`;
}
