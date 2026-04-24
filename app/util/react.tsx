import React from "react";

/**
 * Returns a flat list of direct children, treating fragment children as
 * though they were passed directly.
 *
 * For example:
 *
 * ```tsx
 * directReactChildren(
 *   <>
 *     <span>Foo</span>
 *     <>
 *       <span>Bar</span>
 *     </>
 *   </>
 * )
 * ```
 *
 * returns:
 *
 * ```tsx
 * [ <span>Foo</span>, <span>Bar</span> ]
 * ```
 *
 * Arrays and falsy values are normalized the same way as
 * `React.Children.toArray`.
 */
export function directReactChildren(children: React.ReactNode): React.ReactNode[] {
  const direct: React.ReactNode[] = [];
  pushAllDirectChildren(children, direct);
  return direct;
}

function pushAllDirectChildren(children: React.ReactNode, direct: React.ReactNode[]): void {
  for (const child of React.Children.toArray(children)) {
    if (React.isValidElement<{ children?: React.ReactNode }>(child) && child.type === React.Fragment) {
      pushAllDirectChildren(child.props.children, direct);
      continue;
    }
    direct.push(child);
  }
}

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
