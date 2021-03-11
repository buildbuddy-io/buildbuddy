import React from "react";
import { ToolName } from "./invocation_model";

export type InvocationTabsProps = TabsContext;

/**
 * TabsContext contains common props that determine how tabs are rendered
 * and which tab is the default tab.
 */
export type TabsContext = {
  hash: string;
  denseMode: boolean;
  tool: ToolName;
};

export type InvocationTabHash =
  | "#" // "ALL" tab
  | "#targets"
  | "#log"
  | "#details"
  | "#artifacts"
  | "#timing"
  | "#cache"
  | "#raw"
  | "#execution"; // "EXECUTION" tab (currently not rendered).

/** Returns the `TabHash` of the tab that is active when the URL hash is empty. */
export function getDefaultTab({ tool, denseMode }: Omit<TabsContext, "hash">): InvocationTabHash {
  if (!denseMode) return "#";

  return tool === "ci_runner" ? "#log" : "#targets";
}

/** Returns whether the given tab is selected. */
export function isTabSelected(tabHash: InvocationTabHash, { tool, hash, denseMode }: TabsContext) {
  return tabHash === (hash || getDefaultTab({ tool, denseMode }));
}

export default class InvocationTabsComponent extends React.Component<InvocationTabsProps> {
  private renderTab(tab: InvocationTabHash, { label }: { label: string }) {
    const { hash, denseMode, tool } = this.props;
    const selected = isTabSelected(tab, { hash, denseMode, tool });
    return (
      <a href={tab} className={`tab ${selected ? "selected" : ""}`}>
        {label}
      </a>
    );
  }

  render() {
    const { denseMode, tool } = this.props;
    return (
      <div className="tabs">
        {!denseMode && this.renderTab("#", { label: "All" })}
        {tool === "bazel" && this.renderTab("#targets", { label: "Targets" })}
        {this.renderTab("#log", { label: "Logs" })}
        {this.renderTab("#details", { label: "Details" })}
        {tool === "bazel" && this.renderTab("#artifacts", { label: "Artifacts" })}
        {tool === "bazel" && this.renderTab("#timing", { label: "Timing" })}
        {tool === "bazel" && this.renderTab("#cache", { label: "Cache" })}
        {this.renderTab("#raw", { label: "Raw" })}
      </div>
    );
  }
}
