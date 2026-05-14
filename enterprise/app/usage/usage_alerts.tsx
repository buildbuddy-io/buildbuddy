import Long from "long";
import React from "react";
import alertService from "../../../app/alert/alert_service";
import FilledButton, { OutlinedButton } from "../../../app/components/button/button";
import SimpleModalDialog from "../../../app/components/dialog/simple_modal_dialog";
import TextInput from "../../../app/components/input/input";
import Select, { Option } from "../../../app/components/select/select";
import Spinner from "../../../app/components/spinner/spinner";
import HelpTooltip from "../../../app/components/tooltip/help_tooltip";
import errorService from "../../../app/errors/error_service";
import { bytes as formatBytes, count as formatCount, formatWithCommas } from "../../../app/format/format";
import rpcService, { type Cancelable } from "../../../app/service/rpc_service";
import { usage } from "../../../proto/usage_ts_proto";
import {
  USAGE_ALERTING_METRICS,
  usageAlertingMetricLabel,
  usageAlertingMetricUnit,
  type UsageAlertingMetric,
} from "./usage_alerts_model";

const UsageAlertingMetric = usage.UsageAlertingMetric.Value;
const UsageAlertingWindow = usage.UsageAlertingWindow.Value;
type UsageAlertingWindow = usage.UsageAlertingWindow.Value;

interface UsageAlertsState {
  /** Usage alerting rules configured for the current org. */
  alertingRules?: usage.UsageAlertingRule[];

  /** Whether the initial alerting rules list is loading. */
  alertingLoading?: boolean;

  /** Alert configuration backing the open create-alert modal. Undefined when closed. */
  createAlertConfig?: usage.UsageAlertingRuleConfiguration;

  /** Raw text backing the create-alert threshold input. */
  createAlertThresholdInput?: string;

  /** Whether a create-alert request is in flight. */
  createAlertLoading?: boolean;

  /** Rule currently shown in the delete confirmation modal. */
  deleteAlertRule?: usage.UsageAlertingRule;

  /** ID of the rule currently being deleted. */
  deleteAlertRuleId?: string;
}

type UsageAlertingWindowOption = {
  window: UsageAlertingWindow;
  label: string;
};

const USAGE_ALERTING_WINDOWS: UsageAlertingWindowOption[] = [
  { window: UsageAlertingWindow.DAY, label: "Daily" },
  { window: UsageAlertingWindow.WEEK, label: "Weekly" },
  { window: UsageAlertingWindow.MONTH, label: "Monthly" },
];

const DEFAULT_CREATE_ALERT_METRIC = UsageAlertingMetric.INVOCATIONS;
const DEFAULT_CREATE_ALERT_WINDOW = UsageAlertingWindow.DAY;

/** UsageAlertsComponent renders and manages usage alerting rules. */
export default class UsageAlertsComponent extends React.Component<{}, UsageAlertsState> {
  state: UsageAlertsState = {};

  private alertingRPC?: Cancelable;
  private createAlertRPC?: Cancelable;
  private deleteAlertRPC?: Cancelable;

  componentDidMount() {
    this.fetchUsageAlertingRules();
  }

  componentWillUnmount() {
    this.alertingRPC?.cancel();
    this.alertingRPC = undefined;
    this.createAlertRPC?.cancel();
    this.createAlertRPC = undefined;
    this.deleteAlertRPC?.cancel();
    this.deleteAlertRPC = undefined;
  }

  private fetchUsageAlertingRules() {
    this.alertingRPC?.cancel();
    this.setState({ alertingLoading: true });

    this.alertingRPC = rpcService.service
      .getUsageAlertingRules({})
      .then((response) => {
        this.setState({ alertingRules: response.usageAlertingRule });
      })
      .catch((e) => {
        errorService.handleError(e);
      })
      .finally(() => {
        this.alertingRPC = undefined;
        this.setState({ alertingLoading: false });
      });
  }

  private onClickOpenCreateAlertModal() {
    this.setState({
      createAlertConfig: new usage.UsageAlertingRuleConfiguration({
        metric: DEFAULT_CREATE_ALERT_METRIC,
        absoluteThreshold: Long.ZERO,
        window: DEFAULT_CREATE_ALERT_WINDOW,
      }),
      createAlertThresholdInput: "",
    });
  }

  private onRequestCloseCreateAlertModal() {
    this.setState({ createAlertConfig: undefined, createAlertThresholdInput: undefined });
  }

  private onChangeCreateAlertMetric(e: React.ChangeEvent<HTMLSelectElement>) {
    const metric = Number(e.target.value) as UsageAlertingMetric;
    const config = this.state.createAlertConfig!;
    const threshold = parseUsageAlertingThresholdInput(this.state.createAlertThresholdInput) ?? Long.ZERO;
    this.setState({
      createAlertConfig: new usage.UsageAlertingRuleConfiguration({
        ...config,
        metric,
        absoluteThreshold: usageAlertingAbsoluteThresholdFromInput(metric, threshold),
      }),
    });
  }

  private onChangeCreateAlertWindow(e: React.ChangeEvent<HTMLSelectElement>) {
    const window = Number(e.target.value) as UsageAlertingWindow;
    this.setState({
      createAlertConfig: new usage.UsageAlertingRuleConfiguration({ ...this.state.createAlertConfig!, window }),
    });
  }

  private onChangeCreateAlertThreshold(e: React.ChangeEvent<HTMLInputElement>) {
    const thresholdText = e.target.value;
    const config = this.state.createAlertConfig!;
    const metric = config.metric ?? UsageAlertingMetric.UNKNOWN;
    const threshold = parseUsageAlertingThresholdInput(thresholdText) ?? Long.ZERO;
    this.setState({
      createAlertThresholdInput: thresholdText,
      createAlertConfig: new usage.UsageAlertingRuleConfiguration({
        ...config,
        absoluteThreshold: usageAlertingAbsoluteThresholdFromInput(metric, threshold),
      }),
    });
  }

  private onSubmitCreateAlert() {
    const createAlertConfig = this.state.createAlertConfig;
    if (!createAlertConfig) {
      alertService.error("Enter alert configuration.");
      return;
    }
    const threshold = parseUsageAlertingThresholdInput(this.state.createAlertThresholdInput);
    if (!threshold) {
      alertService.error("Enter a non-negative whole number.");
      return;
    }
    const metric = createAlertConfig.metric ?? UsageAlertingMetric.UNKNOWN;
    const configuration = new usage.UsageAlertingRuleConfiguration({
      ...createAlertConfig,
      absoluteThreshold: usageAlertingAbsoluteThresholdFromInput(metric, threshold),
    });

    this.setState({ createAlertLoading: true });
    this.createAlertRPC = rpcService.service
      .createUsageAlertingRule({ configuration })
      .then((response) => {
        const rule = response.usageAlertingRule;
        this.setState((state) => ({
          alertingRules: rule ? [...(state.alertingRules ?? []), rule] : state.alertingRules,
          createAlertConfig: undefined,
          createAlertThresholdInput: undefined,
        }));
        alertService.success("Alert created.");
      })
      .catch((e) => {
        errorService.handleError(e);
      })
      .finally(() => {
        this.createAlertRPC = undefined;
        this.setState({ createAlertLoading: false });
      });
  }

  private onClickDeleteAlert(rule: usage.UsageAlertingRule) {
    this.setState({ deleteAlertRule: rule });
  }

  private onRequestCloseDeleteAlertModal() {
    this.setState({ deleteAlertRule: undefined });
  }

  private onConfirmDeleteAlert() {
    const rule = this.state.deleteAlertRule;
    if (!rule) {
      return;
    }
    const usageAlertingRuleId = rule.metadata?.usageAlertingRuleId;
    if (!usageAlertingRuleId) {
      alertService.error("Cannot delete alert: missing alert ID.");
      return;
    }

    this.setState({ deleteAlertRuleId: usageAlertingRuleId });
    this.deleteAlertRPC = rpcService.service
      .deleteUsageAlertingRule({ usageAlertingRuleId })
      .then(() => {
        this.setState((state) => ({
          alertingRules: state.alertingRules?.filter(
            (existingRule) => existingRule.metadata?.usageAlertingRuleId !== usageAlertingRuleId
          ),
          deleteAlertRule: undefined,
        }));
        alertService.success("Alert deleted.");
      })
      .catch((e) => {
        errorService.handleError(e);
      })
      .finally(() => {
        this.deleteAlertRPC = undefined;
        this.setState({ deleteAlertRuleId: undefined });
      });
  }

  private renderDeleteAlertModal() {
    const rule = this.state.deleteAlertRule;
    return (
      <SimpleModalDialog
        title="Confirm deletion"
        isOpen={Boolean(rule)}
        onRequestClose={this.onRequestCloseDeleteAlertModal.bind(this)}
        submitLabel="Delete"
        onSubmit={this.onConfirmDeleteAlert.bind(this)}
        loading={Boolean(this.state.deleteAlertRuleId)}
        destructive>
        {rule && (
          <>
            Are you sure you want to delete the{" "}
            <b>
              {usageAlertingWindowLabel(rule.configuration?.window)}{" "}
              {usageAlertingMetricLabel(rule.configuration?.metric)}
            </b>{" "}
            alert with threshold <b>{formatUsageAlertingThreshold(rule.configuration)}</b>? This action cannot be
            undone.
          </>
        )}
      </SimpleModalDialog>
    );
  }

  private renderCreateAlertModal() {
    const createAlertConfig = this.state.createAlertConfig;
    if (!createAlertConfig) {
      return null;
    }
    const metric = createAlertConfig.metric;
    const window = createAlertConfig.window;
    const thresholdInput = this.state.createAlertThresholdInput ?? "";
    const threshold = parseUsageAlertingThresholdInput(thresholdInput);
    const thresholdInputEmpty = thresholdInput.trim() === "";
    const thresholdInputInvalid = !thresholdInputEmpty && threshold === undefined;
    let thresholdPreview: string;
    if (threshold === undefined) {
      thresholdPreview = "";
    } else {
      switch (usageAlertingMetricUnit(metric)) {
        case "bytes":
          thresholdPreview = formatBytes(threshold);
          break;
        case "duration_nanos":
          thresholdPreview = formatUsageAlertingMinutes(threshold, true);
          break;
        case "count":
          thresholdPreview = formatCount(threshold);
          break;
      }
    }
    return (
      <SimpleModalDialog
        className="usage-alerting-modal"
        title="Create alert"
        isOpen={true}
        onRequestClose={this.onRequestCloseCreateAlertModal.bind(this)}
        submitLabel="Create"
        onSubmit={this.onSubmitCreateAlert.bind(this)}
        submitDisabled={thresholdInputEmpty || thresholdInputInvalid}
        loading={this.state.createAlertLoading}>
        <div className="usage-alerting-field">
          <div className="usage-alerting-field-heading">
            <div className="usage-alerting-field-title-row">
              <label htmlFor="usage-alerting-window">Window</label>
              <UsageAlertingWindowHelpTooltip />
            </div>
            <div className="subtitle usage-alerting-field-subtitle">Period of usage checked by the alert</div>
          </div>
          <Select id="usage-alerting-window" value={window} onChange={this.onChangeCreateAlertWindow.bind(this)}>
            {USAGE_ALERTING_WINDOWS.map(({ window, label }) => (
              <Option key={window} value={window}>
                {label}
              </Option>
            ))}
          </Select>
        </div>
        <div className="usage-alerting-field">
          <div className="usage-alerting-field-heading">
            <label htmlFor="usage-alerting-metric">Metric</label>
            <div className="subtitle usage-alerting-field-subtitle">Which usage metric to alert on</div>
          </div>
          <Select id="usage-alerting-metric" value={metric} onChange={this.onChangeCreateAlertMetric.bind(this)}>
            {USAGE_ALERTING_METRICS.map(({ metric, label }) => (
              <Option key={metric} value={metric}>
                {label}
              </Option>
            ))}
          </Select>
        </div>
        <div className="usage-alerting-field">
          <div className="usage-alerting-field-heading">
            <label htmlFor="usage-alerting-threshold">Threshold</label>
            <div className="subtitle usage-alerting-field-subtitle">Alert if metric exceeds this value</div>
          </div>
          <TextInput
            id="usage-alerting-threshold"
            type="text"
            value={thresholdInput}
            aria-invalid={thresholdInputInvalid}
            aria-describedby={thresholdInputInvalid ? "usage-alerting-threshold-error" : undefined}
            onChange={this.onChangeCreateAlertThreshold.bind(this)}
            autoFocus={true}
          />
          {thresholdInputInvalid ? (
            <div id="usage-alerting-threshold-error" className="usage-alerting-field-error">
              Enter a non-negative whole number.
            </div>
          ) : (
            <div className="subtitle usage-alerting-threshold-preview">{thresholdPreview}</div>
          )}
        </div>
      </SimpleModalDialog>
    );
  }

  render() {
    const rules = [...(this.state.alertingRules ?? [])].sort((a, b) => {
      const windowA = a.configuration?.window ?? UsageAlertingWindow.UNKNOWN;
      const windowB = b.configuration?.window ?? UsageAlertingWindow.UNKNOWN;
      if (windowA !== windowB) {
        return windowA - windowB;
      }
      const metricA = a.configuration?.metric ?? UsageAlertingMetric.UNKNOWN;
      const metricB = b.configuration?.metric ?? UsageAlertingMetric.UNKNOWN;
      if (metricA !== metricB) {
        return metricA - metricB;
      }
      return Long.fromValue(a.configuration?.absoluteThreshold ?? 0).compare(
        Long.fromValue(b.configuration?.absoluteThreshold ?? 0)
      );
    });
    return (
      <>
        <div className="usage-alerting-header">
          <p>
            Usage alerts send emails to all Admin users in the organization when a usage metric exceeds a threshold.
          </p>
          <FilledButton onClick={this.onClickOpenCreateAlertModal.bind(this)}>Create new alert</FilledButton>
        </div>
        {this.state.alertingLoading && <Spinner />}
        {!this.state.alertingLoading && !rules.length && (
          <div className="usage-alerting-empty">No alerts configured.</div>
        )}
        {!this.state.alertingLoading && Boolean(rules.length) && (
          <div className="usage-alerting-table-wrapper">
            <table className="usage-alerting-table">
              <thead>
                <tr>
                  <th>
                    <div className="usage-alerting-table-heading-with-help">
                      <span>Window</span>
                      <UsageAlertingWindowHelpTooltip />
                    </div>
                  </th>
                  <th>Metric</th>
                  <th>Threshold</th>
                  <th></th>
                </tr>
              </thead>
              <tbody>
                {rules.map((rule, index) => {
                  const ruleID = rule.metadata?.usageAlertingRuleId || String(index);
                  const threshold = Long.fromValue(rule.configuration?.absoluteThreshold ?? 0);
                  let preciseThresholdLabel: string;
                  switch (usageAlertingMetricUnit(rule.configuration?.metric)) {
                    case "bytes":
                      preciseThresholdLabel = `${formatWithCommas(threshold)} ${pluralize("byte", threshold)}`;
                      break;
                    case "duration_nanos":
                      preciseThresholdLabel = formatUsageAlertingMinutes(
                        Long.fromNumber(Math.round(Number(threshold) / 60e9)),
                        false
                      );
                      break;
                    case "count":
                      preciseThresholdLabel = formatWithCommas(threshold);
                      break;
                  }
                  return (
                    <tr key={ruleID}>
                      <td>{usageAlertingWindowLabel(rule.configuration?.window)}</td>
                      <td>{usageAlertingMetricLabel(rule.configuration?.metric)}</td>
                      <td title={preciseThresholdLabel}>{formatUsageAlertingThreshold(rule.configuration)}</td>
                      <td className="usage-alerting-controls">
                        <OutlinedButton className="destructive" onClick={this.onClickDeleteAlert.bind(this, rule)}>
                          Delete
                        </OutlinedButton>
                      </td>
                    </tr>
                  );
                })}
              </tbody>
            </table>
          </div>
        )}
        {this.renderCreateAlertModal()}
        {this.renderDeleteAlertModal()}
      </>
    );
  }
}

function usageAlertingWindowLabel(window?: UsageAlertingWindow): string {
  return USAGE_ALERTING_WINDOWS.find((option) => option.window === window)?.label ?? "Unknown";
}

function formatUsageAlertingThreshold(config?: usage.UsageAlertingRuleConfiguration | null): string {
  const threshold = Long.fromValue(config?.absoluteThreshold ?? 0);
  switch (usageAlertingMetricUnit(config?.metric)) {
    case "bytes":
      return formatBytes(threshold);
    case "duration_nanos":
      return formatUsageAlertingMinutes(Long.fromNumber(Math.round(Number(threshold) / 60e9)), true);
    case "count":
      return formatCount(threshold);
  }
}

function parseUsageAlertingThresholdInput(value?: string): Long | undefined {
  const trimmedValue = value?.trim();
  if (!trimmedValue?.match(/^[0-9]+$/)) {
    return undefined;
  }
  return Long.fromString(trimmedValue);
}

function usageAlertingAbsoluteThresholdFromInput(metric: UsageAlertingMetric, threshold: Long): Long {
  switch (usageAlertingMetricUnit(metric)) {
    case "duration_nanos":
      return threshold.multiply(60e9);
    default:
      return threshold;
  }
}

function formatUsageAlertingMinutes(minutes: Long, compact: boolean): string {
  return `${compact ? formatCount(minutes) : formatWithCommas(minutes)} ${pluralize("minute", minutes)}`;
}

function pluralize(singular: string, count: Long): string {
  return count.equals(1) ? singular : `${singular}s`;
}

function UsageAlertingWindowHelpTooltip() {
  return (
    <HelpTooltip>
      <p>
        The window is the period of usage checked by the alert. When a new window starts, the counter starts over at
        zero. All windows use <b>UTC</b>.
      </p>
      <p>
        <b>Daily</b> windows run from 00:00 UTC to 00:00 UTC the next day.
      </p>
      <p>
        <b>Weekly</b> windows run from Monday at 00:00 UTC to Monday at 00:00 UTC the next week.
      </p>
      <p>
        <b>Monthly</b> windows run from 00:00 UTC on the first day of the month to 00:00 UTC on the first day of the
        next month.
      </p>
    </HelpTooltip>
  );
}
