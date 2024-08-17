import React from "react";
import errorService from "../../../app/errors/error_service";
import rpcService, { CancelablePromise } from "../../../app/service/rpc_service";
import { User } from "../../../app/auth/auth_service";
import { usage } from "../../../proto/usage_ts_proto";
import Select, { Option } from "../../../app/components/select/select";
import { count, cpuSavingsSec, formatWithCommas, bytes as formatBytes, bytes } from "../../../app/format/format";
import moment from "moment";
import TrendsChartComponent, { ChartColor } from "../trends/trends_chart";
import router, { TrendsChartId } from "../../../app/router/router";
import capabilities from "../../../app/capabilities/capabilities";
import Link from "../../../app/components/link/link";
import Button, { OutlinedButton } from "../../../app/components/button/button";
import SimpleModalDialog from "../../../app/components/dialog/simple_modal_dialog";
import { parseEnum as parseEnum } from "../../../app/util/proto";
import Long from "long";
import alert_service from "../../../app/alert/alert_service";
import TextInput from "../../../app/components/input/input";
import { BellRing, Info } from "lucide-react";

export interface UsageProps {
  path: string;
  user?: User;
}

export default class UsageComponent extends React.Component<UsageProps> {
  render() {
    // TODO: canonicalize path at a higher level
    const path = this.props.path.replace(/\/+$/, "");

    const showReports = path === "/usage";
    const showAlertingRules = path === "/usage/alerting";

    return (
      <div className="usage-page">
        <div className="container usage-page-container">
          <div className="usage-header">
            <div className="usage-title">Usage</div>
            {capabilities.config.usageAlertingRulesEnabled && (
              <div className="tabs">
                <Link href="/usage/" className={`tab ${showReports ? "selected" : ""}`}>
                  Reports
                </Link>
                <Link href="/usage/alerting" className={`tab ${showAlertingRules ? "selected" : ""}`}>
                  Alerting
                </Link>
              </div>
            )}
          </div>
          {showReports && <UsageReportsComponent user={this.props.user} />}
          {showAlertingRules && <UsageAlertingRulesComponent />}
        </div>
      </div>
    );
  }
}

interface ReportsProps {
  user?: User;
}

interface ReportsState {
  response?: usage.GetUsageResponse;
  selectedPeriod: string;
  loading?: boolean;
}

class UsageReportsComponent extends React.Component<ReportsProps, ReportsState> {
  // TODO: remove getDefaultTimePeriodString() after the server
  // is updated to unconditionally send the current period
  state: ReportsState = { selectedPeriod: getDefaultTimePeriodString() };
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
        console.log(response);
        if (!response.usage) {
          throw new Error("Server did not return usage data.");
        }
        this.setState({ response, selectedPeriod: period });
      })
      .catch((e) => errorService.handleError(e))
      .finally(() => this.setState({ loading: false }));
  }

  getUsage(start: number) {
    const date = moment.unix(start).format("YYYY-MM-DD");
    return this.state.response?.dailyUsage.find((v) => v.period === date) ?? new usage.Usage();
  }

  onBarClicked(chartId: TrendsChartId, ts: number) {
    const date = moment.unix(ts).format("YYYY-MM-DD");
    router.navigateTo(`/trends?start=${date}&end=${date}#${chartId}`, true);
  }

  renderCharts() {
    if (this.state.loading || !this.state.response?.dailyUsage) {
      return undefined;
    }
    const daysInMonth = moment(this.state.selectedPeriod, "YYYY-MM").daysInMonth();
    const dates: number[] = [];
    for (let i = 1; i <= daysInMonth; i++) {
      dates.push(
        moment(this.state.selectedPeriod + i.toString().padStart(2, "0"), "YYYY-MM-DD")
          .startOf("day")
          .unix()
      );
    }
    return (
      <>
        <div className="card usage-card">
          <div className="content">
            <TrendsChartComponent
              title="Invocations"
              standaloneChart={true}
              data={dates}
              dataSeries={[
                {
                  name: "invocations",
                  extractValue: (ts) => +(this.getUsage(ts).invocations ?? 0),
                  formatHoverValue: (value) => (value || 0) + " invocations",
                  onClick: this.onBarClicked.bind(this, "builds"),
                  color: ChartColor.BLUE,
                },
              ]}
              primaryYAxis={{
                formatTickValue: count,
                allowDecimals: false,
              }}
              formatXAxisLabel={(ts) => moment.unix(ts).format("MMM D")}
              formatHoverXAxisLabel={(ts) => moment.unix(ts).format("dddd, MMMM Do YYYY")}
              ticks={[]}
            />
          </div>
        </div>
        <div className="card usage-card">
          <div className="content">
            <TrendsChartComponent
              title="Action cache hits"
              standaloneChart={true}
              data={dates}
              dataSeries={[
                {
                  name: "action cache hits",
                  extractValue: (ts) => +(this.getUsage(ts).actionCacheHits ?? 0),
                  formatHoverValue: (value) => (value || 0) + " action cache hits",
                  onClick: this.onBarClicked.bind(this, "cache"),
                  color: ChartColor.BLUE,
                },
              ]}
              primaryYAxis={{
                formatTickValue: count,
                allowDecimals: false,
              }}
              formatXAxisLabel={(ts) => moment.unix(ts).format("MMM D")}
              formatHoverXAxisLabel={(ts) => moment.unix(ts).format("dddd, MMMM Do YYYY")}
              ticks={[]}
            />
          </div>
        </div>
        <div className="card usage-card">
          <div className="content">
            <TrendsChartComponent
              title="Cached build minutes"
              standaloneChart={true}
              data={dates}
              dataSeries={[
                {
                  name: "cached build minutes",
                  extractValue: (ts) => +(this.getUsage(ts).totalCachedActionExecUsec ?? 0),
                  formatHoverValue: (value) => formatMinutes(value || 0),
                  onClick: this.onBarClicked.bind(this, "savings"),
                  color: ChartColor.BLUE,
                },
              ]}
              primaryYAxis={{
                formatTickValue: (v) => formatWithCommas(Math.floor(v / 60e6)),
                allowDecimals: false,
              }}
              formatXAxisLabel={(ts) => moment.unix(ts).format("MMM D")}
              formatHoverXAxisLabel={(ts) => moment.unix(ts).format("dddd, MMMM Do YYYY")}
              ticks={[]}
            />
          </div>
        </div>
        <div className="card usage-card">
          <div className="content">
            <TrendsChartComponent
              title="Content addressable storage cache hits"
              standaloneChart={true}
              data={dates}
              dataSeries={[
                {
                  name: "content addressable storage cache hits",
                  extractValue: (ts) => +(this.getUsage(ts).casCacheHits ?? 0),
                  formatHoverValue: (value) => (value || 0) + " CAS cache hits",
                  onClick: this.onBarClicked.bind(this, "cas"),
                  color: ChartColor.BLUE,
                },
              ]}
              primaryYAxis={{
                formatTickValue: count,
                allowDecimals: false,
              }}
              formatXAxisLabel={(ts) => moment.unix(ts).format("MMM D")}
              formatHoverXAxisLabel={(ts) => moment.unix(ts).format("dddd, MMMM Do YYYY")}
              ticks={[]}
            />
          </div>
        </div>
        <div className="card usage-card">
          <div className="content">
            <TrendsChartComponent
              title="Total cache download (bytes)"
              standaloneChart={true}
              data={dates}
              dataSeries={[
                {
                  name: "total cache download (bytes)",
                  extractValue: (ts) => +(this.getUsage(ts).totalDownloadSizeBytes ?? 0),
                  formatHoverValue: (value) => bytes(value || 0) + " downloaded",
                  onClick: this.onBarClicked.bind(this, "cas"),
                  color: ChartColor.BLUE,
                },
              ]}
              primaryYAxis={{
                formatTickValue: bytes,
                allowDecimals: false,
              }}
              formatXAxisLabel={(ts) => moment.unix(ts).format("MMM D")}
              formatHoverXAxisLabel={(ts) => moment.unix(ts).format("dddd, MMMM Do YYYY")}
              ticks={[]}
            />
          </div>
        </div>
        <div className="card usage-card">
          <div className="content">
            <TrendsChartComponent
              title="Linux remote execution duration"
              standaloneChart={true}
              data={dates}
              dataSeries={[
                {
                  name: "linux remote execution duration",
                  extractValue: (ts) => +(this.getUsage(ts).linuxExecutionDurationUsec ?? 0),
                  formatHoverValue: (value) => formatMinutes(value || 0),
                  onClick: this.onBarClicked.bind(this, "build_time"),
                  color: ChartColor.BLUE,
                },
              ]}
              primaryYAxis={{
                formatTickValue: (v) => formatWithCommas(Math.floor(v / 60e6)),
                allowDecimals: false,
              }}
              formatXAxisLabel={(ts) => moment.unix(ts).format("MMM D")}
              formatHoverXAxisLabel={(ts) => moment.unix(ts).format("dddd, MMMM Do YYYY")}
              ticks={[]}
            />
          </div>
        </div>
      </>
    );
  }

  render() {
    if (!this.state.response) return <div className="loading loading-slim" />;

    const orgName = this.props.user?.selectedGroup.name;
    // Selected period may not be found because of a pending or failed RPC.
    const selection = this.state.response.usage;

    return (
      <>
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
                <div className="usage-resource-name">Linux remote execution duration</div>
                <div className="usage-value">{formatMinutes(Number(selection.linuxExecutionDurationUsec))}</div>
              </div>
            )}
          </div>
        </div>
        {this.renderCharts()}
      </>
    );
  }
}

type AlertingRuleFields = {
  alertingRuleId?: string;
  alertingPeriod: string | usage.AlertingPeriod;
  usageMetric: string | usage.UsageMetric;
  thresholdInput: string | number | Long;
};

interface AlertingRulesProps {}

interface AlertingRulesState {
  loading?: boolean;
  modalLoading?: boolean;
  response?: usage.GetUsageAlertingRulesResponse;

  creatingRule?: AlertingRuleFields;
  editingRule?: AlertingRuleFields;
  deletingRule?: string;
}

class UsageAlertingRulesComponent extends React.Component<AlertingRulesProps, AlertingRulesState> {
  state: AlertingRulesState = {};

  componentDidMount() {
    this.fetch();
  }

  private fetch() {
    this.setState({
      loading: true,
      creatingRule: undefined,
      editingRule: undefined,
      deletingRule: undefined,
    });
    rpcService.service
      .getUsageAlertingRules({})
      .then((response) => this.setState({ response }))
      .catch((e) => errorService.handleError(e))
      .finally(() => this.setState({ loading: false }));
  }

  private createRule() {
    this.setState({ modalLoading: true });
    rpcService.service
      .updateUsageAlertingRules({
        updates: [
          new usage.AlertingRuleUpdate({
            alertingRule: ruleFromFields(this.state.creatingRule),
          }),
        ],
      })
      .then(() => {
        alert_service.success("Successfully created rule");
        this.fetch();
      })
      .catch((e) => errorService.handleError(e))
      .finally(() => {
        this.setState({ modalLoading: false });
      });
  }
  private saveRule() {
    this.setState({ modalLoading: true });
    rpcService.service
      .updateUsageAlertingRules({
        updates: [
          new usage.AlertingRuleUpdate({
            alertingRule: ruleFromFields(this.state.editingRule),
          }),
        ],
      })
      .then(() => {
        alert_service.success("Rule saved successfully");
        this.fetch();
      })
      .catch((e) => errorService.handleError(e))
      .finally(() => {
        this.setState({ modalLoading: false });
      });
  }
  private deleteRule() {
    this.setState({ modalLoading: true });
    rpcService.service
      .updateUsageAlertingRules({
        updates: [
          new usage.AlertingRuleUpdate({
            deleteAlertingRuleId: this.state.deletingRule,
          }),
        ],
      })
      .then(() => {
        alert_service.success("Rule deleted successfully");
        this.fetch();
      })
      .catch((e) => errorService.handleError(e))
      .finally(() => {
        this.setState({ modalLoading: false });
      });
  }

  private showAddModal() {
    this.setState({
      creatingRule: {
        // Fill in some reasonable defaults when creating.
        alertingPeriod: usage.AlertingPeriod.MONTHLY,
        usageMetric: usage.UsageMetric.TOTAL_DOWNLOAD_SIZE_BYTES,
        thresholdInput: defaultThreshold(parseEnum(usage.UsageMetric, usage.UsageMetric.TOTAL_DOWNLOAD_SIZE_BYTES)),
      },
    });
  }
  private showEditModal(rule: usage.AlertingRule) {
    this.setState({ editingRule: fieldsFromRule(rule) });
  }
  private showDeleteModal(ruleId: string) {
    this.setState({ deletingRule: ruleId });
  }
  private dismissModals() {
    this.setState({
      creatingRule: undefined,
      editingRule: undefined,
      deletingRule: undefined,
    });
  }

  render() {
    if (this.state.loading) return <div className="loading" />;
    if (!this.state.response) return null;
    return (
      <div className="usage-alerting-rules">
        <div className="alerting-rules-header">
          <BellRing className="icon orange" />
          <div className="alerting-rules-title">Alerting rules</div>
          <Button onClick={() => this.showAddModal()}>Create new rule</Button>
        </div>
        <div className="alerting-rules-content">
          {this.state.response.alertingRules.length ? (
            <table className="alerting-rules-list">
              <thead>
                <tr>
                  <th>Period</th>
                  <th>Usage metric</th>
                  <th>Alert threshold</th>
                  <th></th>
                </tr>
              </thead>
              <tbody>
                {this.state.response.alertingRules.map((rule, i) => {
                  return (
                    <tr key={i} className="alerting-rules-item">
                      <td>{formatAlertingPeriod(rule.alertingPeriod)}</td>
                      <td>{formatUsageMetric(rule.usageMetric)}</td>
                      <td>{formatAlertingThreshold(rule.usageMetric, Number(rule.threshold))}</td>
                      <td>
                        <div className="buttons">
                          <OutlinedButton onClick={() => this.showEditModal(rule)}>Edit</OutlinedButton>
                          <OutlinedButton
                            className="destructive"
                            onClick={() => this.showDeleteModal(rule.alertingRuleId)}>
                            Delete
                          </OutlinedButton>
                        </div>
                      </td>
                    </tr>
                  );
                })}
              </tbody>
            </table>
          ) : (
            <div className="alerting-rules-empty">
              <Info className="icon" />
              <div>No alerting rules configured.</div>
            </div>
          )}
          <SimpleModalDialog
            title="Create alerting rule"
            className="usage-alerting-rules usage-alerting-rules-dialog"
            isOpen={!!this.state.creatingRule}
            submitLabel="Create"
            submitDisabled={!ruleFromFields(this.state.creatingRule)}
            onSubmit={() => this.createRule()}
            loading={this.state.modalLoading}
            onRequestClose={() => this.dismissModals()}>
            {this.state.creatingRule && (
              <AlertingRuleEditor
                fields={this.state.creatingRule}
                onChange={(fields) => this.setState({ creatingRule: fields })}
              />
            )}
          </SimpleModalDialog>
          <SimpleModalDialog
            title="Edit alerting rule"
            className="usage-alerting-rules usage-alerting-rules-dialog"
            isOpen={!!this.state.editingRule}
            submitLabel="Save"
            submitDisabled={!ruleFromFields(this.state.editingRule)}
            onSubmit={() => this.saveRule()}
            loading={this.state.modalLoading}
            onRequestClose={() => this.dismissModals()}>
            {this.state.editingRule && (
              <AlertingRuleEditor
                fields={this.state.editingRule}
                onChange={(fields) => this.setState({ editingRule: fields })}
              />
            )}
          </SimpleModalDialog>
          <SimpleModalDialog
            title="Delete alerting rule"
            className="usage-alerting-rules usage-alerting-rules-dialog"
            destructive
            isOpen={!!this.state.deletingRule}
            submitLabel="Delete"
            onSubmit={() => this.deleteRule()}
            loading={this.state.modalLoading}
            onRequestClose={() => this.dismissModals()}>
            <p>Are you sure you want to delete this rule?</p>
          </SimpleModalDialog>
        </div>
      </div>
    );
  }
}

interface AlertingRuleEditorProps {
  fields: AlertingRuleFields;
  onChange: (fields: AlertingRuleFields) => void;
}

class AlertingRuleEditor extends React.Component<AlertingRuleEditorProps> {
  private onChangeAlertingPeriod(e: React.ChangeEvent<HTMLSelectElement>) {
    this.props.onChange({
      ...this.props.fields,
      alertingPeriod: parseEnum(usage.AlertingPeriod, e.target.value),
    });
  }

  private onChangeUsageMetric(e: React.ChangeEvent<HTMLSelectElement>) {
    this.props.onChange({
      ...this.props.fields,
      usageMetric: parseEnum(usage.UsageMetric, e.target.value),
      thresholdInput: defaultThreshold(parseEnum(usage.UsageMetric, e.target.value)),
    });
  }

  private onChangeThreshold(e: React.ChangeEvent<HTMLInputElement>) {
    this.props.onChange({
      ...this.props.fields,
      thresholdInput: e.target.value,
    });
  }

  render() {
    const formattedThreshold = !isNaN(Number(this.props.fields.thresholdInput))
      ? formatAlertingThreshold(
          parseEnum(usage.UsageMetric, this.props.fields.usageMetric),
          Number(this.props.fields.thresholdInput)
        )
      : undefined;

    return (
      <div className="alerting-rule-editor">
        <div className="alerting-rule-editor-row">
          <span className="field-name">Period</span>
          <Select value={String(this.props.fields.alertingPeriod)} onChange={(e) => this.onChangeAlertingPeriod(e)}>
            {[usage.AlertingPeriod.MONTHLY, usage.AlertingPeriod.WEEKLY, usage.AlertingPeriod.DAILY].map((value, i) => (
              <option key={i} value={value}>
                {formatAlertingPeriod(value as usage.AlertingPeriod)}
              </option>
            ))}
          </Select>
        </div>
        <div className="alerting-rule-editor-row">
          <span className="field-name">Usage metric</span>
          <Select value={String(this.props.fields.usageMetric)} onChange={(e) => this.onChangeUsageMetric(e)}>
            {Object.values(usage.UsageMetric)
              .filter((v) => v !== 0)
              .map((value, i) => (
                <option key={i} value={value}>
                  {formatUsageMetric(value as usage.UsageMetric)}
                </option>
              ))}
          </Select>
        </div>
        <div className="alerting-rule-editor-row">
          <span className="field-name">Alert threshold</span>
          <TextInput value={String(this.props.fields.thresholdInput)} onChange={(e) => this.onChangeThreshold(e)} />
          <span className="units">{formatUnits(parseEnum(usage.UsageMetric, this.props.fields.usageMetric))}</span>
        </div>
        <div className="alerting-rule-editor-row">
          <span className="field-name" />
          <span className="formatted-value">
            {formattedThreshold !== undefined && Number(this.props.fields.thresholdInput) >= 1000 ? (
              <>
                (
                {formatAlertingThreshold(
                  parseEnum(usage.UsageMetric, this.props.fields.usageMetric),
                  Number(this.props.fields.thresholdInput)
                )}
                )
              </>
            ) : (
              <>&nbsp;</>
            )}
          </span>
        </div>
      </div>
    );
  }
}

function ruleFromFields(fields?: AlertingRuleFields): usage.AlertingRule | undefined {
  if (!fields) return undefined;

  const threshold = Number(fields.thresholdInput);
  if (!threshold || isNaN(threshold) || threshold < 0) return undefined;

  const alertingPeriod = parseEnum(usage.AlertingPeriod, fields.alertingPeriod);
  if (!alertingPeriod) return undefined;

  const usageMetric = parseEnum(usage.UsageMetric, fields.usageMetric);
  if (!usageMetric) return undefined;

  return new usage.AlertingRule({
    alertingRuleId: fields.alertingRuleId,
    alertingPeriod,
    usageMetric,
    threshold: Long.fromNumber(threshold),
  });
}

function fieldsFromRule(rule?: usage.AlertingRule): AlertingRuleFields | undefined {
  if (!rule) return undefined;

  let thresholdInput: number;
  switch (rule.usageMetric) {
    case usage.UsageMetric.LINUX_EXECUTION_DURATION_USEC:
    case usage.UsageMetric.TOTAL_CACHED_ACTION_EXEC_USEC:
      // minutes -> usec
      thresholdInput = 60 * 1e6 * Number(rule.threshold);
    default:
      thresholdInput = Number(rule.threshold);
  }
  return {
    alertingRuleId: rule.alertingRuleId,
    alertingPeriod: rule.alertingPeriod,
    usageMetric: rule.usageMetric,
    thresholdInput,
  };
}

function formatAlertingPeriod(v: usage.AlertingPeriod): string {
  switch (v) {
    case usage.AlertingPeriod.DAILY:
      return "Day (UTC)";
    case usage.AlertingPeriod.WEEKLY:
      return "Week (Mon – Sun, UTC)";
    case usage.AlertingPeriod.MONTHLY:
      return "Calendar month (UTC)";
    default:
      return "";
  }
}

function formatUsageMetric(v: usage.UsageMetric): string {
  switch (v) {
    case usage.UsageMetric.INVOCATIONS:
      return "Invocation count";
    case usage.UsageMetric.ACTION_CACHE_HITS:
      return "Action cache hits";
    case usage.UsageMetric.CAS_CACHE_HITS:
      return "CAS cache hits";
    case usage.UsageMetric.TOTAL_DOWNLOAD_SIZE_BYTES:
      return "Cache download";
    case usage.UsageMetric.LINUX_EXECUTION_DURATION_USEC:
      return "Linux execution minutes";
    case usage.UsageMetric.TOTAL_UPLOAD_SIZE_BYTES:
      return "Cache upload";
    case usage.UsageMetric.TOTAL_CACHED_ACTION_EXEC_USEC:
      return "Cached build minutes";
    default:
      return "";
  }
}

function formatAlertingThreshold(v: usage.UsageMetric, threshold: number): string {
  switch (v) {
    case usage.UsageMetric.INVOCATIONS:
      return count(threshold);
    case usage.UsageMetric.ACTION_CACHE_HITS:
      return count(threshold);
    case usage.UsageMetric.CAS_CACHE_HITS:
      return count(threshold);
    case usage.UsageMetric.TOTAL_DOWNLOAD_SIZE_BYTES:
      return bytes(threshold);
    case usage.UsageMetric.LINUX_EXECUTION_DURATION_USEC:
      return `${count(threshold)} minutes`;
    case usage.UsageMetric.TOTAL_UPLOAD_SIZE_BYTES:
      return bytes(threshold);
    case usage.UsageMetric.TOTAL_CACHED_ACTION_EXEC_USEC:
      return `${count(threshold)} minutes`;
    default:
      return "";
  }
}

function defaultThreshold(v: usage.UsageMetric): number {
  switch (v) {
    case usage.UsageMetric.INVOCATIONS:
      return 100_000;
    case usage.UsageMetric.ACTION_CACHE_HITS:
      return 100e6;
    case usage.UsageMetric.CAS_CACHE_HITS:
      return 100e6;
    case usage.UsageMetric.TOTAL_DOWNLOAD_SIZE_BYTES:
      return 100e9;
    case usage.UsageMetric.LINUX_EXECUTION_DURATION_USEC:
      return 1e6; // minutes
    case usage.UsageMetric.TOTAL_UPLOAD_SIZE_BYTES:
      return 100e9;
    case usage.UsageMetric.TOTAL_CACHED_ACTION_EXEC_USEC:
      return 1e6; // minutes
    default:
      return 0;
  }
}

function formatUnits(v: usage.UsageMetric): string {
  switch (v) {
    case usage.UsageMetric.INVOCATIONS:
      return "";
    case usage.UsageMetric.ACTION_CACHE_HITS:
      return "";
    case usage.UsageMetric.CAS_CACHE_HITS:
      return "";
    case usage.UsageMetric.TOTAL_DOWNLOAD_SIZE_BYTES:
      return "bytes";
    case usage.UsageMetric.LINUX_EXECUTION_DURATION_USEC:
      return "minutes";
    case usage.UsageMetric.TOTAL_UPLOAD_SIZE_BYTES:
      return "bytes";
    case usage.UsageMetric.TOTAL_CACHED_ACTION_EXEC_USEC:
      return "minutes";
    default:
      return "";
  }
}

function getDefaultTimePeriodString(): string {
  return moment.utc().format("YYYY-MM");
}

function formatMinutes(usec: number): string {
  return `${formatWithCommas(usec / 60e6)} minutes`;
}
