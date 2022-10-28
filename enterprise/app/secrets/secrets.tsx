import React from "react";
import UpdateSecretComponent from "./update_secret";
import SecretsListComponent from "./secrets_list";

export interface SecretsComponentProps {
  path: string;
  search: URLSearchParams;
}

export default class SecretsComponent extends React.Component<SecretsComponentProps> {
  private renderPage() {
    if (this.props.path === "/settings/org/secrets/new") {
      return <UpdateSecretComponent />;
    }
    if (this.props.path === "/settings/org/secrets/edit") {
      return <UpdateSecretComponent name={this.props.search.get("name") || ""} />;
    }
    return <SecretsListComponent />;
  }

  render() {
    return <div className="secrets">{this.renderPage()}</div>;
  }
}
