import React from "react";
import { User } from "../../../app/auth/auth_service";
import TestBuddyComponent from "./test_buddy";

interface Props {
  user: User;
  tab: string;
  search: URLSearchParams;
  dark: boolean;
}

export default class TapComponent extends React.Component<Props> {
  componentDidMount() {
    document.title = "TestBuddy | BuildBuddy";
  }

  render() {
    return (
      <TestBuddyComponent
        repo={this.props.search.get("repo") || ""}
        targetLabel={this.props.search.get("target") || ""}
      />
    );
  }
}
