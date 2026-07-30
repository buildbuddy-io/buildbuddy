import { ShieldCheck } from "lucide-react";
import React from "react";
import { agentsecurity } from "../../proto/agent_security_ts_proto";
import { Link } from "../components/link/link";
import rpcService from "../service/rpc_service";
import * as proto from "../util/proto";

interface Props {
  invocationId: string;
  invocationStartTime: Date;
}

interface State {
  events: agentsecurity.AgentSecurityEvent[];
  hasMore: boolean;
}

export default class InvocationSecurityCardComponent extends React.Component<Props, State> {
  state: State = {
    events: [],
    hasMore: false,
  };

  private mounted = false;

  componentDidMount() {
    this.mounted = true;
    this.fetchEvents();
  }

  componentDidUpdate(prevProps: Props) {
    if (prevProps.invocationId !== this.props.invocationId) {
      this.setState({ events: [], hasMore: false }, () => this.fetchEvents());
    }
  }

  componentWillUnmount() {
    this.mounted = false;
  }

  private async fetchEvents() {
    const start = new Date(this.props.invocationStartTime);
    start.setHours(0, 0, 0, 0);
    const end = new Date();
    end.setDate(end.getDate() + 1);
    try {
      const response = await rpcService.service.getAgentSecurityEvents(
        agentsecurity.GetAgentSecurityEventsRequest.create({
          invocationId: this.props.invocationId,
          timestampAfter: proto.dateToTimestamp(start),
          timestampBefore: proto.dateToTimestamp(end),
          pageSize: 100,
        })
      );
      if (this.mounted) {
        this.setState({ events: response.events, hasMore: Boolean(response.nextPageToken) });
      }
    } catch (e) {
      // This card is supplemental. Audit-only data may not be available to
      // every invocation viewer.
      console.debug("Agent security summary unavailable", e);
    }
  }

  render() {
    if (!this.state.events.length) {
      return null;
    }
    const secretCount = new Set(this.state.events.map((event) => event.secretName)).size;
    const occurrenceCount = this.state.events.reduce((sum, event) => sum + Number(event.occurrenceCount), 0);
    const start = new Date(this.props.invocationStartTime);
    start.setHours(0, 0, 0, 0);
    const query = new URLSearchParams({
      view: "agent-security",
      invocation_id: this.props.invocationId,
      start: String(start.getTime()),
    });

    return (
      <div className="card agent-security-card">
        <ShieldCheck />
        <div className="content">
          <div className="title">Secret value detected and redacted</div>
          <div className="details">
            {this.state.hasMore ? "100+" : occurrenceCount.toLocaleString()}{" "}
            {occurrenceCount === 1 ? "detection" : "detections"} across {secretCount.toLocaleString()} named{" "}
            {secretCount === 1 ? "secret" : "secrets"}. Secret values and request content are not retained.
          </div>
          <Link href={`/audit-logs/?${query.toString()}`}>Review agent security events</Link>
        </div>
      </div>
    );
  }
}
