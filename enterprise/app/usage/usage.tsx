import React from "react";
import errorService from "../../../app/errors/error_service";
import rpcService from "../../../app/service/rpc_service";
import { User } from "../../../app/auth/auth_service";
import { usage } from "../../../proto/usage_ts_proto";
import Select, { Option } from "../../../app/components/select/select";
import { formatWithCommas, bytes as formatBytes } from "../../../app/format/format";

export interface UsageProps {
  user?: User;
}

interface State {
  response?: usage.GetUsageResponse;
  selectedPeriod?: string;
  loading?: boolean;
}

export default class UsageComponent extends React.Component<UsageProps, State> {
  state: State = {};

  componentDidMount() {
    document.title = "Usage | BuildBuddy";

    this.setState({ loading: true });
    rpcService.service
      .getUsage(new usage.GetUsageRequest())
      .then((response) => {
        if (response.usage.length === 0) {
          throw new Error("Server did not return usage data.");
        }
        this.setState({ response, selectedPeriod: response.usage[0].period });
      })
      .catch((e) => errorService.handleError(e))
      .finally(() => this.setState({ loading: false }));
  }

  private onChangePeriod(e: React.ChangeEvent<HTMLSelectElement>) {
    const selectedPeriod = e.target.value;
    this.setState({ selectedPeriod });
  }

  render() {
    const usage = this.state.response
      ? this.state.response.usage.find((usage) => usage.period === this.state.selectedPeriod)
      : null;

    const orgName = this.props.user.selectedGroup?.name;

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
                      BuildBuddy usage for <span className="usage-period">{usage.period} (UTC)</span>
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
                  <div className="usage-value">{formatWithCommas(usage.invocations)}</div>
                  <div className="usage-resource-name">Action cache hits</div>
                  <div className="usage-value">{formatWithCommas(usage.actionCacheHits)}</div>
                  <div className="usage-resource-name">Content addressable storage cache hits</div>
                  <div className="usage-value">{formatWithCommas(usage.casCacheHits)}</div>
                  <div className="usage-resource-name">Total bytes downloaded from cache</div>
                  <div className="usage-value" title={formatWithCommas(usage.totalDownloadSizeBytes)}>
                    {formatBytes(usage.totalDownloadSizeBytes)}
                  </div>
                  <div className="usage-resource-name">Linux remote execution duration</div>
                  <div className="usage-value">{formatMinutes(Number(usage.linuxExecutionDurationUsec))}</div>
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
