import React from "react";
import Breadcrumbs from "../components/breadcrumbs/breadcrumbs";

interface Props {
  invocationId: string;
  title?: React.ReactNode;
  subtitle?: React.ReactNode;
}

export default class InvocationInProgressComponent extends React.Component<Props> {
  render() {
    return (
      <div className="state-page">
        <div className="shelf">
          <div className="container">
            <Breadcrumbs>Invocation {this.props.invocationId}</Breadcrumbs>
            <div className="titles">
              <div className="title">{this.props.title || "Invocation in progress..."}</div>
            </div>
            <div className="details">{this.props.subtitle || "This page will refresh every few seconds."}</div>
          </div>
        </div>
      </div>
    );
  }
}
