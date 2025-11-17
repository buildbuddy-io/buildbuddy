import React from "react";
import BucketComponent from "./bucket";
import NamespaceComponent from "./namespace";
import NamespacesComponent from "./namespaces";

export interface QuotaProps {
  path: string;
  search: URLSearchParams;
}

export default class QuotaComponent extends React.Component<QuotaProps> {
  private renderChildPage(): React.ReactNode {
    if (this.props.path === "/settings/server/quota/namespace") {
      return <NamespaceComponent path={this.props.path} search={this.props.search} />;
    }
    if (this.props.path.startsWith("/settings/server/quota/bucket")) {
      return <BucketComponent search={this.props.search} />;
    }
    return <NamespacesComponent />;
  }

  render(): React.ReactNode {
    return <div className="quota">{this.renderChildPage()}</div>;
  }
}
