import React from "react";
import { User } from "../../../app/auth/user";

interface Props {
  user: User;
  search: URLSearchParams;
}

export default class TargetDataComponent extends React.Component<Props> {
  private getPageTitle() {
    return "Target Data";
  }

  private updateDocumentTitle() {
    document.title = `${this.getPageTitle()} | BuildBuddy`;
  }

  componentDidMount(): void {
    this.updateDocumentTitle();
  }

  componentDidUpdate(): void {
    this.updateDocumentTitle();
  }

  render(): React.ReactNode {
    return (
      <div className="target-data">
        <div className="container">
          <div className="target-data-header">
            <div className="target-data-title">{this.getPageTitle()}</div>
          </div>
        </div>
      </div>
    );
  }
}
