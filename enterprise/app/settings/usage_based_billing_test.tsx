import React from "react";

declare function require(moduleName: string): any;

(globalThis as any).window = {
  buildbuddyConfig: {
    usageBasedBillingEnabled: true,
  },
  history: {
    replaceState: () => {},
  },
  location: {
    href: "https://app.buildbuddy.test/settings/org/details",
    hash: "",
    pathname: "/settings/org/details",
    search: "",
  },
};

const UsageBasedBillingComponent = require("./usage_based_billing").default;
const { grp } = require("../../../proto/group_ts_proto");

function userWithStatus(status: number) {
  return {
    canCall: (rpc: string) => rpc === "createUsageBasedBillingSetupSession",
    selectedGroup: new grp.Group({ status }),
  };
}

function renderForStatus(status: number): React.ReactNode {
  const component = new UsageBasedBillingComponent({
    search: new URLSearchParams(),
    user: userWithStatus(status),
  });
  return component.render();
}

function textContent(node: React.ReactNode): string {
  if (node == null || typeof node === "boolean") {
    return "";
  }
  if (typeof node === "string" || typeof node === "number") {
    return String(node);
  }
  if (Array.isArray(node)) {
    return node.map(textContent).join("");
  }
  if (React.isValidElement(node)) {
    return textContent(node.props.children);
  }
  return "";
}

function hasDebugID(node: React.ReactNode, debugID: string): boolean {
  if (node == null || typeof node === "boolean" || typeof node === "string" || typeof node === "number") {
    return false;
  }
  if (Array.isArray(node)) {
    return node.some((child) => hasDebugID(child, debugID));
  }
  if (React.isValidElement(node)) {
    return node.props["debug-id"] === debugID || hasDebugID(node.props.children, debugID);
  }
  return false;
}

describe("UsageBasedBillingComponent", () => {
  it("shows enterprise quote but not upgrade for usage-based groups", () => {
    const tree = renderForStatus(grp.Group.GroupStatus.USAGE_BASED_GROUP_STATUS);

    expect(textContent(tree)).toContain("Request an Enterprise Quote");
    expect(textContent(tree)).not.toContain("Upgrade to Usage Based Billing");
    expect(hasDebugID(tree, "upgrade-to-usage-based-billing-button")).toBe(false);
  });
});
