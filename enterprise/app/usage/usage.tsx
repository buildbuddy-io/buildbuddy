import React from "react";
import errorService from "../../../app/errors/error_service";
import rpcService from "../../../app/service/rpc_service";
import { User } from "../../../app/auth/auth_service";
import { usage } from "../../../proto/usage_ts_proto";
import Select, { Option } from "../../../app/components/select/select";
import { cpuSavingsSec, formatWithCommas, bytes as formatBytes } from "../../../app/format/format";

export interface UsageProps {
  user?: User;
}

interface State {
  response?: usage.GetUsageResponse;
  selectedPeriod: usage.Usage;
  loading?: boolean;
}

export default class UsageComponent extends React.Component<UsageProps, State> {
  state: State = { selectedPeriod: usage.Usage.create({}) };

  componentDidMount() {
    document.title = "Usage | BuildBuddy";

    this.setState({ loading: true });
    rpcService.service
      .getUsage(new usage.GetUsageRequest())
      .then((response) => {
        if (response.usage.length === 0) {
          throw new Error("Server did not return usage data.");
        }
        this.setState({ response, selectedPeriod: response.usage[0] });
      })
      .catch((e) => errorService.handleError(e))
      .finally(() => this.setState({ loading: false }));
  }

  private onChangePeriod(e: React.ChangeEvent<HTMLSelectElement>) {
    const selectedPeriod = e.target.value;
    this.setState({
      // Value comes directly from the response, it's a bug if we don't find it.
      selectedPeriod: this.state.response?.usage.find((v) => v.period === selectedPeriod) || usage.Usage.create({}),
    });
  }

  render() {
    const orgName = this.props.user?.selectedGroup.name;
    const period = this.state.selectedPeriod;

    return (
      <div className="usage-page">
        <div className="container usage-page-container">
          <div className="usage-header">
            <div className="usage-title">Usage</div>
          </div>
          {this.state.loading && <div className="loading" />}
          {this.state.response && (
            <div className="card usage-card">
              <div className="content">
                <div className="usage-period-header">
                  <div>
                    {orgName && <div className="org-name">{orgName}</div>}
                    <div className="selected-period-label">
                      BuildBuddy usage for <span className="usage-period">{period.period} (UTC)</span>
                    </div>
                  </div>
                  <Select title="Usage period" onChange={this.onChangePeriod.bind(this)}>
                    {this.state.response.usage.map((usage, i) => (
                      <Option key={usage.period} value={usage.period}>
                        {usage.period}
                        {i === 0 ? " (Current period)" : ""}
                      </Option>
                    ))}
                  </Select>
                </div>
                <div className="usage-period-table">
                  <div className="usage-resource-name">Invocations</div>
                  <div className="usage-value">{formatWithCommas(period.invocations)}</div>
                  <div className="usage-resource-name">Action cache hits</div>
                  <div className="usage-value">{formatWithCommas(period.actionCacheHits)}</div>
                  <div className="usage-resource-name">Content addressable storage cache hits</div>
                  <div className="usage-value">{formatWithCommas(period.casCacheHits)}</div>
                  <div className="usage-resource-name">Total bytes downloaded from cache</div>
                  <div className="usage-value" title={formatWithCommas(period.totalDownloadSizeBytes)}>
                    {formatBytes(period.totalDownloadSizeBytes)}
                  </div>
                  {/*
                  <div className="usage-resource-name">Total bytes uploaded from cache</div>
                  <div className="usage-value" title={formatWithCommas(period.totalUploadSizeBytes)}>
                    {formatBytes(period.totalUploadSizeBytes)}
                  </div>
				    */}
                  <div className="usage-resource-name">Linux remote execution duration</div>
                  <div className="usage-value">{formatMinutes(Number(period.linuxExecutionDurationUsec))}</div>
                  {/*
                  <div className="usage-resource-name">Saved CPU Time</div>
                  <div className="usage-value">{cpuSavingsSec(Number(period.totalCachedActionExecUsec))}</div>
				    */}
                </div>
              </div>
            </div>
          )}
        </div>
      </div>
    );
  }
}

function formatMinutes(usec: number): string {
  return `${formatWithCommas(usec / 60e6)} minutes`;
}
