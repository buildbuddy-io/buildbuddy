import React from "react";
import { HelpCircle } from "lucide-react";
import { Tooltip } from "../../../app/components/tooltip/tooltip";

export default class GitHubTooltip extends React.Component<{}, {}> {
  render() {
    return (
      <Tooltip
        renderContent={() => (
          <div className="github-hovercard">
            <div>
              <p>To use GitHub related features:</p>
              <p>
                <b>1. Link your personal GitHub account to BuildBuddy.</b>
                <br />
                This generates a user-specific token, so that GitHub can attribute certain requests back to your GitHub
                user. This lets you manage GitHub features in our UI.
              </p>
              <p>
                <b>2. Install the BuildBuddy GitHub app to your GitHub organization.</b>
                <br />
                This grants the app necessary permissions to your desired repositories. This GitHub app powers our
                GitHub related features. Any requests from these features will be authenticated under the app, and not a
                specific GitHub user.
              </p>
              <p>
                <b>3. Import the app installation to BuildBuddy.</b>
                <br />
                Even after you've installed the app on the GitHub side, you need to explicitly import the installation
                to BuildBuddy so we're aware that the installation exists.
              </p>
            </div>
          </div>
        )}>
        <HelpCircle className="icon" />
      </Tooltip>
    );
  }
}
