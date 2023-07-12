import React from "react";
import rpcService from "../../../app/service/rpc_service";
import { auditlog } from "../../../proto/auditlog_ts_proto";
import * as proto from "../../../app/util/proto";
import * as format from "../../../app/format/format";

interface State {
  entries: auditlog.Entry[];
}

export default class AuditLogsComponent extends React.Component<{}, State> {
  state: State = {
    entries: [],
  };

  componentDidMount() {
    this.fetchAuditLogs();
  }

  async fetchAuditLogs() {
    const response = await rpcService.service.getAuditLogs(auditlog.GetAuditLogsRequest.create());
    this.setState({
      entries: response.entries,
    });
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
                  <div className="actor">{entry.authenticationInfo?.user?.userEmail}</div>
                  <div className="method">{entry.method}</div>
                  <div className="payload">
                    {entry.diffState && entry.diffState}
                    {!entry.diffState && entry.newState && JSON.stringify(entry.newState.toJSON(), null, 4)}
                    {!entry.diffState &&
                      !entry.newState &&
                      entry.oldState &&
                      JSON.stringify(entry.oldState.toJSON(), null, 4)}
                  </div>
                </div>
              );
            })}
          </div>
        </div>
      </div>
    );
  }
}
