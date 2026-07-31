import React from "react";
import { User } from "../../../app/auth/auth_service";
import TestHealthComponent from "./test_health";

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
    return <TestHealthComponent repo="https://github.com/buildbuddy-io/buildbuddy" />;
  }
}
