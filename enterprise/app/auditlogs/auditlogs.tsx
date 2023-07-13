import React from "react";
import rpcService from "../../../app/service/rpc_service";
import { auditlog } from "../../../proto/auditlog_ts_proto";
import * as proto from "../../../app/util/proto";
import * as format from "../../../app/format/format";
import {audit} from "rxjs/operators";

interface State {
  entries: auditlog.Entry[];
}

export default class AuditLogsComponent extends React.Component<{}, State> {
  state: State = {
    entries: [],
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
    this.fetchAuditLogs();
  }

  async fetchAuditLogs() {
    const response = await rpcService.service.getAuditLogs(auditlog.GetAuditLogsRequest.create());
    this.setState({
      entries: response.entries,
    });
  }

  renderActor(authInfo: auditlog.AuthenticationInfo) {
    let actor = ""
    if (authInfo?.user?.userEmail) {
      actor = authInfo.user.userEmail
    } else if (authInfo?.user?.userId) {
      actor = authInfo.user.userId
    } else if (authInfo?.apiKey?.label) {
      actor = authInfo.apiKey.label
    } else if (authInfo?.apiKey?.id) {
      actor = authInfo.apiKey.id
    }
    return (<>
      <div>{actor}</div>
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

  renderRequest(request: auditlog.Entry.ResourceRequest | null | undefined) {
    if (!request) {
      return ""
    }
    let obj = request.toJSON()
    let vals = Object.values(obj)
    if (vals.length == 0) {
      return ""
    }
    return JSON.stringify(vals[0], null, 4)
  }

  render() {
    return (
      <div className="executors-page">
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
            {this.state.entries.map((entry) => {
              return (
                <div className="audit-log-entry">
                  <div className="timestamp">{format.formatDate(proto.timestampToDate(entry.eventTime || {}))}</div>
                  <div className="actor">
                    {this.renderActor(entry.authenticationInfo!)}
                  </div>
                <div className="resource">{this.renderResource(entry.resource!)}</div>
                  <div className="method">{this.methods[entry.method]}</div>
                  <div className="payload"><pre>{this.renderRequest(entry.request)}</pre></div>
                </div>
              );
            })}
          </div>
        </div>
      </div>
    );
  }
}
