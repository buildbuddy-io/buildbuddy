import React from "react";
import { CI_RUNNER_ROLE, HOSTED_BAZEL_ROLE } from "./invocation_model";

export type InvocationTabsProps = TabsContext;

/**
 * TabsContext contains common props that determine how tabs are rendered
 * and which tab is the default tab.
 */
export type TabsContext = {
  hash: string;
  denseMode: boolean;
  role: string;
  rbeEnabled?: boolean;
  hasSuggestions?: boolean;
};

export type TabId =
  | "all"
  | "targets"
  | "commands"
  | "log"
  | "details"
  | "artifacts"
  | "timing"
  | "cache"
  | "suggestions"
  | "raw"
  | "execution"
  | "fetches"
  | "action";

export function getTabId(hash: string): TabId {
  return (hash.substring(1) as TabId) || "all";
}

export function getActiveTab({ hash, role, denseMode }: TabsContext): TabId {
  if (hash) return getTabId(hash);

  if (!denseMode) return "all";

  return role === CI_RUNNER_ROLE ? "commands" : "targets";
}

export default class InvocationTabsComponent extends React.Component<InvocationTabsProps> {
  private renderTab(id: TabId, { label, href }: { label: string; href?: string }) {
    return (
      <a href={href || `#${id}`} className={`tab ${getActiveTab(this.props) === id ? "selected" : ""}`}>
        {label}
      </a>
    );
  }

  render() {
    const isBazelInvocation = this.props.role !== CI_RUNNER_ROLE && this.props.role !== HOSTED_BAZEL_ROLE;

    return (
      <div className="tabs">
        {!this.props.denseMode && this.renderTab("all", { href: "#", label: "All" })}
        {isBazelInvocation && this.renderTab("targets", { label: "Targets" })}
        {!isBazelInvocation && this.renderTab("commands", { label: "Commands" })}
        {this.renderTab("log", { label: "Logs" })}
        {this.renderTab("details", { label: "Details" })}
        {isBazelInvocation && this.renderTab("artifacts", { label: "Artifacts" })}
        {isBazelInvocation && this.renderTab("timing", { label: "Timing" })}
        {isBazelInvocation && this.renderTab("cache", { label: "Cache" })}
        {isBazelInvocation && this.props.rbeEnabled && this.renderTab("execution", { label: "Executions" })}
        {this.props.hasSuggestions && this.renderTab("suggestions", { label: "Suggestions" })}
        {this.renderTab("raw", { label: "Raw" })}
      </div>
    );
  }
}
