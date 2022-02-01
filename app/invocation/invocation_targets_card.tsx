import React from "react";
import InvocationModel from "./invocation_model";
import router from "../router/router";
import { build_event_stream } from "../../proto/build_event_stream_ts_proto";
import { Copy } from "lucide-react";
import { copyToClipboard } from "../util/clipboard";
import alert_service from "../alert/alert_service";

interface Props {
  model: InvocationModel;
  icon: JSX.Element;
  buildEvents: build_event_stream.BuildEvent[];
  pastVerb: string;
  presentVerb: string;
  pageSize: number;
  filter: string;
  className: string;
}

interface State {
  numPages: number;
}

export default class TargetsCardComponent extends React.Component<Props, State> {
  state: State = {
    numPages: 1,
  };

  handleMoreClicked() {
    this.setState({ ...this.state, numPages: this.state.numPages + 1 });
  }

  handleTargetClicked(label: string, event: MouseEvent) {
    event.preventDefault();
    router.navigateToQueryParam("target", label);
  }

  handleCopyClicked(label: string) {
    copyToClipboard(label);
    alert_service.success("Target labels copied to clipboard!");
  }

  render() {
    let events = this.props.buildEvents.filter(
      (target) =>
        !this.props.filter || target.id.targetCompleted.label.toLowerCase().includes(this.props.filter.toLowerCase())
    );

    return (
      <div className={`card ${this.props.className}`}>
        <div className="icon">{this.props.icon}</div>
        <div className="content">
          <div className="title">
            {events.length}
            {this.props.filter ? " matching" : ""} {this.props.pastVerb}{" "}
            <Copy
              className="copy-icon"
              onClick={this.handleCopyClicked.bind(
                this,
                events.map((target) => target.id.targetCompleted.label).join(" ")
              )}
            />
          </div>
          <div className="details">
            {events
              .slice(0, (this.props.pageSize && this.state.numPages * this.props.pageSize) || undefined)
              .map((target) => (
                <a
                  className="list-grid"
                  href={`?target=${encodeURIComponent(target.id.targetCompleted.label)}`}
                  onClick={this.handleTargetClicked.bind(this, target.id.targetCompleted.label)}>
                  <div
                    title={`${
                      this.props.model.configuredMap.get(target.id.targetCompleted.label)?.buildEvent.configured
                        .targetKind
                    } ${this.props.model.getTestSize(
                      this.props.model.configuredMap.get(target.id.targetCompleted.label)?.buildEvent.configured
                        .testSize
                    )}`}
                    className={"clickable target"}>
                    <span className="target-status-icon">{this.props.icon}</span> {target.id.targetCompleted.label}
                  </div>
                  <div>{this.props.model.getRuntime(target.id.targetCompleted.label)}</div>
                </a>
              ))}
          </div>
          {this.props.pageSize && events.length > this.props.pageSize * this.state.numPages && !!this.state.numPages && (
            <div className="more" onClick={this.handleMoreClicked.bind(this)}>
              See more {this.props.presentVerb}
            </div>
          )}
        </div>
      </div>
    );
  }
}
