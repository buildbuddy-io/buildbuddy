import React from "react";

import capabilities from "../capabilities/capabilities";

const RELEASE_VERSION_REGEX = /^v\d+\.\d+\.\d+$/;
// Non-release builds report a ci- or local- label ending in the public
// buildbuddy commit.
const BUILD_VERSION_REGEX = /^(?:ci|local)-\d{8}-([0-9a-f]{7,40})$/;

function versionLink(version: string): string | undefined {
  if (RELEASE_VERSION_REGEX.test(version)) {
    return `https://github.com/buildbuddy-io/buildbuddy/releases/tag/${version}`;
  }
  const build = version.match(BUILD_VERSION_REGEX);
  if (build) {
    return `https://github.com/buildbuddy-io/buildbuddy/commit/${build[1]}`;
  }
  return undefined;
}

export default class FooterComponent extends React.Component {
  render() {
    const version = capabilities.version;
    const link = versionLink(version);
    return (
      <div className="footer">
        <span>&copy; {new Date().getFullYear()} Iteration, Inc.</span>
        <a href="https://buildbuddy.io/terms" target="_blank">
          Terms
        </a>
        <a href="https://buildbuddy.io/privacy" target="_blank">
          Privacy
        </a>
        <a href="https://buildbuddy.io" target="_blank">
          BuildBuddy
        </a>
        {version != "unknown" &&
          (link ? (
            <a href={link} target="_blank">
              {version}
            </a>
          ) : (
            <span>{version}</span>
          ))}
        <a href="mailto:hello@buildbuddy.io" target="_blank">
          Contact us
        </a>
        {capabilities.config.communityLinksEnabled && (
          <a href="https://community.buildbuddy.io" target="_blank">
            Slack
          </a>
        )}
        <a href="https://twitter.com/buildbuddy" target="_blank">
          Twitter
        </a>
        <a href="https://github.com/buildbuddy-io/buildbuddy/" target="_blank">
          GitHub
        </a>
      </div>
    );
  }
}
