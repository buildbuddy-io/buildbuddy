import React from "react";

import capabilities from "../capabilities/capabilities";

const RELEASE_VERSION_REGEX = /^v\d+\.\d+\.\d+$/;
// Non-release builds report a label like ci-20261026T163202Z-<sha7>.
const BUILD_VERSION_REGEX = /^(?:ci|local)-\d{8}T\d{6}Z-[0-9a-f]{7,40}$/;
// The commit can be a placeholder like "unknown" or "dev", which has no page.
const COMMIT_REGEX = /^[0-9a-f]{7,40}$/;

function versionLink(version: string, commit: string): string | undefined {
  if (RELEASE_VERSION_REGEX.test(version)) {
    return `https://github.com/buildbuddy-io/buildbuddy/releases/tag/${version}`;
  }
  if (BUILD_VERSION_REGEX.test(version) && COMMIT_REGEX.test(commit)) {
    return `https://github.com/buildbuddy-io/buildbuddy/commit/${commit}`;
  }
  return undefined;
}

export default class FooterComponent extends React.Component {
  render() {
    const version = capabilities.version;
    const link = versionLink(version, capabilities.config.commit);
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
