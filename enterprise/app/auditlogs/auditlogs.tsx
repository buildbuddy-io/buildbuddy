import React from "react";
import rpcService from "../../../app/service/rpc_service";
import {auditlog} from "../../../proto/auditlog_ts_proto";
import * as proto from "../../../app/util/proto";
import * as format from "../../../app/format/format";
import {formatDateRange} from "../../../app/format/format";
import Button, {OutlinedButton} from "../../../app/components/button/button";
import {Calendar} from "lucide-react";
import Popup from "../../../app/components/popup/popup";
import {DateRangePicker, OnChangeProps, RangeWithKey} from "react-date-range";
import error_service from "../../../app/errors/error_service";
import Spinner from "../../../app/components/spinner/spinner";
import Action = auditlog.Action;

interface State {
  loading: boolean
  entries: auditlog.Entry[]
  nextPageToken: string
  isDatePickerOpen: boolean
  dateRange: RangeWithKey
}

export default class AuditLogsComponent extends React.Component<{}, State> {
  state: State = {
    loading: false,
    entries: [],
    nextPageToken: "",
    isDatePickerOpen: false,
    dateRange: {
      startDate: new Date(),
      endDate: new Date(),
      key: "selection",
    },
  };

  methods: {[key:string]: string} = {
    "apiKeys.create": "Create",
    "apiKeys.update": "Update",
    "apiKeys.list": "List",
    "apiKeys.delete": "Delete",
    "groups.update": "Update",
    "groups.updateMembership": "Update Membership",
    "secrets.create": "Create",
    "secrets.update": "Update",
    "secrets.delete": "Delete",
    "invocations.update": "Update",
    "invocations.delete": "Delete",
  }

  componentDidMount() {
    const today = new Date()
    const todayDate = new Date(today.getFullYear(), today.getMonth(), today.getDate())
    const todayEndDate = new Date(todayDate.getFullYear(), today.getMonth(), today.getDate(), 23, 59, 59, 999)
    const dateRange: RangeWithKey = {startDate: todayDate, endDate: todayEndDate, key: "selection"}
    this.setState({dateRange: dateRange})
    this.fetchAuditLogs(dateRange);
  }

  async fetchAuditLogs(dateRange: RangeWithKey) {
    let req = auditlog.GetAuditLogsRequest.create({
      pageToken: this.state.nextPageToken,
      timestampAfter: proto.dateToTimestamp(dateRange.startDate!),
      timestampBefore: proto.dateToTimestamp(dateRange.endDate!),
    });
    try {
      const response = await rpcService.service.getAuditLogs(req);

      if (this.state.nextPageToken) {
        this.setState((prevState) => ({
          entries: prevState.entries.concat(response.entries)
        }))
      } else {
        this.setState({entries: response.entries})
      }

      this.setState({
        nextPageToken: response.nextPageToken,
      });
    } catch (e) {
      error_service.handleError(e)
    }
  }

  onClickLoadMore() {
    this.setState({loading: true})
    this.fetchAuditLogs(this.state.dateRange).then(() => {
      this.setState({loading: false})
    })
  }

  renderUser(authInfo: auditlog.AuthenticationInfo) {
    let user = ""
    if (authInfo?.user?.userEmail) {
      user = authInfo.user.userEmail
    } else if (authInfo?.user?.userId) {
      user = authInfo.user.userId
    } else if (authInfo?.apiKey?.label) {
      user = authInfo.apiKey.label
    } else if (authInfo?.apiKey?.id) {
      user = authInfo.apiKey.id
    }
    return (<>
      <div>{user}</div>
      <div>{authInfo?.clientIp}</div>
    </>)
  }

  renderResource(resourceID: auditlog.ResourceID) {
    let res = ""
    if (resourceID.type == auditlog.ResourceType.GROUP) {
      res = "Organization"
    }
    switch (resourceID.type) {
      case auditlog.ResourceType.GROUP_API_KEY:
        res = "Org API Key"
        break
      case auditlog.ResourceType.USER_API_KEY:
        res = "User API Key"
        break
      case auditlog.ResourceType.GROUP:
        res = "Organization"
        break
      case auditlog.ResourceType.SECRET:
        res = "Secret"
        break
      case auditlog.ResourceType.INVOCATION:
        res = "Invocation"
        break
    }
    let label = ""
    if (resourceID.name) {
      label = resourceID.name
    } else if (resourceID.id) {
      label = resourceID.id
    }
    return (<>
      <div>{res}</div>
      <div>{label}</div>
    </>)
  }

  renderAction(action: auditlog.Action) {
    switch (action) {
      case Action.ACTION_CREATE:
        return "Create"
      case Action.ACTION_DELETE:
        return "Delete"
      case Action.ACTION_UPDATE:
        return "Update"
      case Action.ACTION_LIST:
        return "List"
      case Action.ACTION_UPDATE_MEMBERSHIP:
        return "Update Membership"
    }
    return ""
  }

  renderRequest(request: auditlog.Entry.ResourceRequest | null | undefined) {
    if (!request) {
      return ""
    }
    let obj = request.toJSON()
    let vals = Object.values(obj)
    if (vals.length == 0) {
      return ""
    }
    let s = JSON.stringify(vals[0], null, 4)
    let lines = s.split("\n")
    lines = lines.map((l) => l.slice(4))
    return lines.slice(1, lines.length - 1).join("\n")
  }

  private onOpenDatePicker() {
    this.setState({ isDatePickerOpen: true });
  }
  private onCloseDatePicker() {
    this.setState({ isDatePickerOpen: false });
  }

  private onDateChange(range: OnChangeProps) {
    let dateRange = (range as {selection: RangeWithKey}).selection;
    this.setState({dateRange: dateRange, nextPageToken: ""})
    this.fetchAuditLogs(dateRange)
  }

  render() {
    return (
      <div className="audit-logs-page">
        <div className="shelf">
          <div className="container">
            <div className="breadcrumbs">
              <span>Audit Logs</span>
            </div>
            <div className="title">Audit logs</div>
          </div>
        </div>
        <div className="container">
          <div className="audit-logs">
            <div className="popup-wrapper">
              <OutlinedButton className="date-picker-button icon-text-button" onClick={this.onOpenDatePicker.bind(this)}>
                <Calendar className="icon" />
                <span>{formatDateRange(this.state.dateRange.startDate!, this.state.dateRange.endDate!)}</span>
              </OutlinedButton>
              <Popup
                  anchor="left"
                  isOpen={this.state.isDatePickerOpen}
                  onRequestClose={this.onCloseDatePicker.bind(this)}
                  className="date-picker-popup">
                <DateRangePicker
                    ranges={[this.state.dateRange]}
                    onChange={this.onDateChange.bind(this)}
                    // Disable textbox inputs, like "days from today", or "days until today".
                    inputRanges={[]}
                    editableDateInputs
                    color="#263238"
                    rangeColors={["#263238"]}
                    startDatePlaceholder="Start date"
                    endDatePlaceholder="End date"
                />
              </Popup>
            </div>
            {this.state.entries.length == 0 && (
            <div>There are no logs in the specified time range.</div>
            )}
            {this.state.entries.length > 0 && (
            <div className="audit-logs-header">
              <div className="timestamp">Time</div>
              <div className="user">User</div>
              <div className="resource">Resource</div>
              <div className="method">Method</div>
              <div className="request">Request</div>
            </div>
            )}
            {this.state.entries.map((entry) => {
              return (
                <div className="audit-log-entry">
                  <div className="timestamp">{format.formatDate(proto.timestampToDate(entry.eventTime || {}))}</div>
                  <div className="user">
                    {this.renderUser(entry.authenticationInfo!)}
                  </div>
                <div className="resource">{this.renderResource(entry.resource!)}</div>
                  <div className="method">{this.renderAction(entry.action)}</div>
                  <div className="request"><pre>{this.renderRequest(entry.request)}</pre></div>
                </div>
              );
            })}
            {this.state.nextPageToken && (
                <Button
                    className="load-more-button"
                    onClick={this.onClickLoadMore.bind(this)}
                    disabled={this.state.loading}>
                  <span>Load more</span>
                  {this.state.loading && <Spinner className="white" />}
                </Button>
            )}
          </div>
        </div>
      </div>
    );
  }
}
