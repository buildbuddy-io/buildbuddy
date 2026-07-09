import { AlertCircle, Info } from "lucide-react";
import React from "react";
import { upgrade } from "../../../proto/upgrade_ts_proto";

export interface UpgradePromptProps {
  // The upgrade prompt from the server, if any. Nothing is rendered when
  // this is unset.
  prompt?: upgrade.IPrompt | null;
  // Plural, lowercase name of the outdated components, e.g. "cache proxies".
  componentName: string;
  // Optional link to upgrade instructions.
  docsHref?: string;
}

// Per-urgency banner styling, all from the shared Banner component's
// stylesheet. HIGH is the yellow banner-warning style like MEDIUM, but with
// a red icon; CRITICAL is the red banner-error style with an
// exclamation-mark icon rather than banner-error's usual XCircle.
function appearance(urgency: upgrade.Prompt.Urgency | null | undefined): { className: string; icon: JSX.Element } {
  switch (urgency) {
    case upgrade.Prompt.Urgency.CRITICAL:
      return { className: "banner-error", icon: <AlertCircle className="red" /> };
    case upgrade.Prompt.Urgency.HIGH:
      return { className: "banner-warning", icon: <AlertCircle className="red" /> };
    case upgrade.Prompt.Urgency.MEDIUM:
      return { className: "banner-warning", icon: <AlertCircle /> };
    default:
      return { className: "banner-info", icon: <Info className="blue" /> };
  }
}

/**
 * A banner prompting the user to upgrade out-of-date components, styled
 * according to the urgency the server assigned.
 */
export default class UpgradePrompt extends React.Component<UpgradePromptProps> {
  render() {
    const prompt = this.props.prompt;
    if (!prompt?.newestAvailableVersion) {
      return null;
    }
    const { className, icon } = appearance(prompt.urgency);
    return (
      <div className={`banner upgrade-prompt-banner ${className}`}>
        {icon}
        <div className="banner-content">
          Some of your {this.props.componentName} are running an outdated version. The newest available version is{" "}
          <b>{prompt.newestAvailableVersion}</b>.
          {this.props.docsHref && (
            <>
              {" "}
              <a href={this.props.docsHref} target="_blank">
                See the docs
              </a>{" "}
              for upgrade instructions.
            </>
          )}
        </div>
      </div>
    );
  }
}
