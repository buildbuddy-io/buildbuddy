import React from "react";

/**
 * Joins a list of react nodes with the given separator. Similar to
 * String.prototype.join, but for JSX.
 *
 * Example:
 * ```tsx
 * joinReactNodes([<MyComponent />, <MyComponent />], ", ")
 * ```
 */
export function joinReactNodes(nodes: React.ReactNode[], joiner: React.ReactNode): React.ReactNode[] {
  const joined: React.ReactNode[] = [];
  for (let i = 0; i < nodes.length; i++) {
    joined.push(nodes[i]);
    // If the next element exists, append the joiner node.
    if (i + 1 < nodes.length) {
      joined.push(joiner);
    }
  }
  return joined;
}
