import React from "react";
import moment from "moment";
import rpcService from "../../../app/service/rpc_service";
import { auditlog } from "../../../proto/auditlog_ts_proto";
import * as proto from "../../../app/util/proto";
import * as format from "../../../app/format/format";
import { formatDateRange } from "../../../app/format/format";
import Button, { OutlinedButton } from "../../../app/components/button/button";
import { Calendar } from "lucide-react";
import Popup from "../../../app/components/popup/popup";
import { DateRangePicker, OnChangeProps, RangeWithKey } from "react-date-range";
import error_service from "../../../app/errors/error_service";
import Spinner from "../../../app/components/spinner/spinner";
import { User } from "../../../app/auth/user";
import Action = auditlog.Action;

interface AuditLogsComponentProps {
  user: User;
}
interface State {
  loading: boolean;
  entries: auditlog.Entry[];
  nextPageToken: string;
  isDatePickerOpen: boolean;
  dateRange: RangeWithKey;
}

export default class AuditLogsComponent extends React.Component<AuditLogsComponentProps, State> {
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

  componentDidMount() {
    document.title = "Audit logs | BuildBuddy";
    const today = new Date();
    today.setHours(0, 0, 0, 0);
    const dateRange: RangeWithKey = { startDate: today, endDate: today, key: "selection" };
    this.setState({ dateRange: dateRange });
    this.fetchAuditLogs(dateRange);
  }

  async fetchAuditLogs(dateRange: RangeWithKey) {
    // Default start time to the midnight today, local time.
    const start = dateRange.startDate ?? moment().startOf("day").toDate();
    // Default end time to the end of today, local time (regardless of start date).
    const end = moment(dateRange.endDate ?? new Date())
      .add(1, "day")
      .startOf("day")
      .toDate();
    let req = auditlog.GetAuditLogsRequest.create({
      pageToken: this.state.nextPageToken,
      timestampAfter: proto.dateToTimestamp(start),
      timestampBefore: proto.dateToTimestamp(end),
    });
    try {
      const response = await rpcService.service.getAuditLogs(req);

      if (this.state.nextPageToken) {
        this.setState((prevState) => ({
          entries: prevState.entries.concat(response.entries),
        }));
      } else {
        this.setState({ entries: response.entries });
      }

      this.setState({
        nextPageToken: response.nextPageToken,
      });
    } catch (e) {
      error_service.handleError(e);
    }
  }

  onClickLoadMore() {
    this.setState({ loading: true });
    this.fetchAuditLogs(this.state.dateRange).then(() => {
      this.setState({ loading: false });
    });
  }

  renderUser(authInfo: auditlog.AuthenticationInfo) {
    let user = "";
    if (authInfo?.user?.userEmail) {
      user = authInfo.user.userEmail;
    } else if (authInfo?.user?.userId) {
      user = authInfo.user.userId;
    } else if (authInfo?.apiKey?.label) {
      user = `API Key "${authInfo.apiKey.label}"`;
    } else if (authInfo?.apiKey?.id) {
      user = authInfo.apiKey.id;
    }
    return (
      <>
        <div>{user}</div>
        <div>IP: {authInfo?.clientIp}</div>
      </>
    );
  }

  renderResource(resourceID: auditlog.ResourceID) {
    let res = "";
    if (resourceID.type == auditlog.ResourceType.GROUP) {
      res = "Organization";
    }
    switch (resourceID.type) {
      case auditlog.ResourceType.GROUP_API_KEY:
        res = "Org API Key";
        break;
      case auditlog.ResourceType.USER_API_KEY:
        res = "User API Key";
        break;
      case auditlog.ResourceType.GROUP:
        res = "Organization";
        break;
      case auditlog.ResourceType.SECRET:
        res = "Secret";
        break;
      case auditlog.ResourceType.INVOCATION:
        res = "Invocation";
        break;
      case auditlog.ResourceType.IP_RULE:
        res = "IP Rule";
        break;
    }
    return (
      <>
        <div>{res}</div>
        {resourceID.id && <div>{resourceID.id}</div>}
        {resourceID.name && <div>"{resourceID.name}"</div>}
      </>
    );
  }

  renderAction(action: auditlog.Action) {
    switch (action) {
      case Action.CREATE:
        return "Create";
      case Action.ACCESS:
        return "Access";
      case Action.GET:
        return "Get";
      case Action.DELETE:
        return "Delete";
      case Action.UPDATE:
        return "Update";
      case Action.LIST:
        return "List";
      case Action.UPDATE_MEMBERSHIP:
        return "Update Membership";
      case Action.LINK_GITHUB_REPO:
        return "Link GitHub Repo";
      case Action.UNLINK_GITHUB_REPO:
        return "Unlink GitHub Repo";
      case Action.EXECUTE_CLEAN_WORKFLOW:
        return "Execute Clean Workflow";
      case Action.CREATE_IMPERSONATION_API_KEY:
        return "Create Impersonation API Key";
      case Action.UPDATE_IP_RULES_CONFIG:
        return "Update IP Rules Config";
    }
    return "";
  }

  renderRequest(request: auditlog.Entry.Request | null | undefined) {
    if (!request || !request.apiRequest) {
      return "";
    }

    // Populate any available ID descriptor information by appending it
    // directly to the field value. We can display this in a prettier
    // way in the future, but this will do for now.
    const idDescriptors = new Map<string, string>();
    for (const desc of request.idDescriptors) {
      idDescriptors.set(desc.id, desc.value);
    }
    if (request.apiRequest.updateGroupUsers) {
      for (const update of request.apiRequest.updateGroupUsers.update) {
        if (update.userId?.id && idDescriptors.has(update.userId.id)) {
          update.userId.id += " (" + idDescriptors.get(update.userId.id) + ")";
        }
      }
    }

    let obj = request.apiRequest.toJSON();
    let vals = Object.values(obj);
    if (vals.length == 0) {
      return "";
    }
    // Only one field of ResourceRequest will be set, and we want to display
    // the contents of that field, so we grab vals[0].
    let s = JSON.stringify(vals[0], null, 4);
    // To make the request look slightly nicer, we strip the outer { and }
    // braces and remove the 4 space indentation from the fields.
    let lines = s.split("\n");
    lines = lines.map((l) => l.slice(4));
    return lines.slice(1, lines.length - 1).join("\n");
  }

  private onOpenDatePicker() {
    this.setState({ isDatePickerOpen: true });
  }
  private onCloseDatePicker() {
    this.setState({ isDatePickerOpen: false });
  }

  private onDateChange(range: OnChangeProps) {
    let dateRange = (range as { selection: RangeWithKey }).selection;
    this.setState({ dateRange: dateRange, nextPageToken: "" });
    this.fetchAuditLogs(dateRange);
  }

  render() {
    return (
      <div className="audit-logs-page">
        <div className="shelf">
          <div className="container">
            <div className="breadcrumbs">
              <span>{this.props.user.selectedGroupName()}</span>
            </div>
            <div className="title">Audit logs</div>
          </div>
        </div>
        <div className="container">
          <div className="audit-logs">
            <div className="popup-wrapper">
              <OutlinedButton
                className="date-picker-button icon-text-button"
                onClick={this.onOpenDatePicker.bind(this)}>
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
              <div className="empty-state">There are no audit logs in the specified time range.</div>
            )}
            {this.state.entries.length > 0 && (
              <div className="audit-logs-table">
                <div className="audit-logs-header">
                  <div className="timestamp">Time</div>
                  <div className="user">User</div>
                  <div className="resource">Resource</div>
                  <div className="method">Method</div>
                  <div className="request">Request</div>
                </div>
                {this.state.entries.map((entry) => {
                  return (
                    <div className="audit-log-entry">
                      <div className="timestamp">{format.formatDate(proto.timestampToDate(entry.eventTime || {}))}</div>
                      <div className="user">{this.renderUser(entry.authenticationInfo!)}</div>
                      <div className="resource">{this.renderResource(entry.resource!)}</div>
                      <div className="method">{this.renderAction(entry.action)}</div>
                      <div className="request">
                        <pre>{this.renderRequest(entry.request)}</pre>
                      </div>
                    </div>
                  );
                })}
              </div>
            )}
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
