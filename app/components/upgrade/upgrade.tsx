import React from "react";
import { upgrade } from "../../../proto/upgrade_ts_proto";
import Banner from "../banner/banner";

export interface UpgradePromptProps {
  // The upgrade prompt from the server, if any. Nothing is rendered when
  // this is unset.
  prompt?: upgrade.IPrompt | null;
}

function bannerType(urgency: upgrade.Prompt.Urgency | null | undefined): "info" | "warning" | "error" {
  switch (urgency) {
    case upgrade.Prompt.Urgency.CRITICAL:
      return "error";
    case upgrade.Prompt.Urgency.HIGH:
    case upgrade.Prompt.Urgency.MEDIUM:
      return "warning";
    default:
      return "info";
  }
}

// mostUrgent picks the prompt to display from several server responses
// (e.g. one per region), preferring higher urgency.
export function mostUrgent(prompts: (upgrade.IPrompt | null | undefined)[]): upgrade.IPrompt | null {
  let best: upgrade.IPrompt | null = null;
  for (const prompt of prompts) {
    if (!prompt?.message) continue;
    if (!best || (prompt.urgency ?? 0) > (best.urgency ?? 0)) {
      best = prompt;
    }
  }
  return best;
}

/**
 * A banner prompting the user to upgrade out-of-date components, styled
 * according to the urgency the server assigned.
 */
export default class UpgradePrompt extends React.Component<UpgradePromptProps> {
  render() {
    const prompt = this.props.prompt;
    if (!prompt?.message) {
      return null;
    }
    return (
      <Banner type={bannerType(prompt.urgency)} className="upgrade-prompt-banner">
        {prompt.message}
      </Banner>
    );
  }
}
