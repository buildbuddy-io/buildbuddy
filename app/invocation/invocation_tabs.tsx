import React from "react";
import { CI_RUNNER_ROLE, HOSTED_BAZEL_ROLE } from "./invocation_model";

export type InvocationTabsProps = TabsContext;

/**
 * TabsContext contains common props that determine how tabs are rendered
 * and which tab is the default tab.
 */
export type TabsContext = {
  tab: string;
  denseMode: boolean;
  role: string;
  executionsEnabled?: boolean;
  hasSuggestions?: boolean;
};

export type TabId =
  | "all"
  | "steps"
  | "targets"
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

function getTabId(tab: string): TabId {
  return (tab.substring(1) as TabId) || "all";
}

export function getActiveTab({ tab, role, denseMode }: Partial<TabsContext>): TabId {
  if (tab) return getTabId(tab);

  if (role === CI_RUNNER_ROLE) return "steps";

  return denseMode ? "log" : "all";
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
    return (
      <div className="tabs">
        {!this.props.denseMode && this.renderTab("all", { href: "#", label: "All" })}
        {this.renderTab("targets", { label: "Targets" })}
        {this.renderTab("log", { label: "Logs" })}
        {this.renderTab("details", { label: "Details" })}
        {this.renderTab("artifacts", { label: "Artifacts" })}
        {this.renderTab("timing", { label: "Timing" })}
        {this.renderTab("cache", { label: "Cache" })}
        {this.props.executionsEnabled && this.renderTab("execution", { label: "Executions" })}
        {this.props.hasSuggestions && this.renderTab("suggestions", { label: "Suggestions" })}
        {this.renderTab("raw", { label: "Raw" })}
      </div>
    );
  }
}

export interface WorkflowInvocationTabsProps {
  tab: string;
}
