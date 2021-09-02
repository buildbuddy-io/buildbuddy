import React from "react";
import errorService from "../../../app/errors/error_service";
import rpcService from "../../../app/service/rpc_service";
import { usage } from "../../../proto/usage_ts_proto";
import Select, { Option } from "../../../app/components/select/select";

export interface UsageProps {}

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
        this.setState({ response, selectedPeriod: response.usage[0].period });
      })
      .catch((e) => errorService.handleError(e))
      .finally(() => this.setState({ loading: false }));
  }

  private onChangePeriod(e: React.ChangeEvent) {
    const selectedPeriod = (e.target as HTMLOptionElement).value;
    this.setState({ selectedPeriod });
  }

  render() {
    const usage = this.state.response
      ? this.state.response.usage.find((usage) => usage.period === this.state.selectedPeriod)
      : null;

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
                  <div className="selected-period-label">
                    Usage for <span className="usage-period">{usage.period} (UTC)</span>
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
                  <div className="usage-value">{usage.totalNumBuilds}</div>
                  <div className="usage-resource-name">Action cache hits</div>
                  <div className="usage-value">{usage.actionCacheHits}</div>
                  <div className="usage-resource-name">Content addressable storage cache hits</div>
                  <div className="usage-value">{usage.casCacheHits}</div>
                  <div className="usage-resource-name">Total bytes downloaded from cache</div>
                  <div className="usage-value">{usage.totalDownloadSizeBytes}</div>
                </div>
              </div>
            </div>
          )}
        </div>
      </div>
    );
  }
}
