import React from "react";

export default class FooterComponent extends React.Component {
  render() {
    return (
      <div className="footer">
        <a href="https://buildbuddy.io/terms" target="_blank">
          Terms
        </a>{" "}
        |{" "}
        <a href="https://buildbuddy.io/privacy" target="_blank">
          Privacy
        </a>{" "}
        |{" "}
        <a href="https://buildbuddy.io" target="_blank">
          BuildBuddy
        </a>{" "}
        | &copy; 2020 Iteration, Inc.
      </div>
    );
  }
}
